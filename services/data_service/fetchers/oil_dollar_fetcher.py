"""
石油数据采集模块
通过新浪财经 API 采集布伦特原油和 WTI 原油实时价格

数据源:
- 新浪财经 hq.sinajs.cn
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

import requests

from core.logger import get_logger
from services.db_pool import get_db_pool

logger = get_logger(__name__)

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

SINA_HEADERS = {
    'Referer': 'https://finance.sina.com.cn',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# 新浪原油代码配置
OIL_CONFIG = {
    'brent': {
        'sina_code': 'UKOIL',
        'display_name': '布伦特原油',
        'url': 'https://hq.sinajs.cn/rn=latest6&list=UKOIL',
    },
    'wti': {
        'sina_code': 'USOIL',
        'display_name': 'WTI原油',
        'url': 'https://hq.sinajs.cn/rn=latest6&list=USOIL',
    },
}


@dataclass
class OilDollarDataModel:
    """石油价格数据模型"""
    name: str  # brent/wti
    display_name: str  # 布伦特原油/WTI原油
    price: float
    change_value: float
    change_pct: float
    source: str = 'sina'
    update_time: str = ''


class OilDollarFetcher:
    """石油价格采集器 - 新浪财经数据源"""

    def __init__(self):
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.data_dir = PROJECT_ROOT / 'data'
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _parse_sina_response(self, text: str, name: str, config: dict) -> Optional[OilDollarDataModel]:
        """解析新浪财经返回的逗号分隔字符串

        新浪 hq.sinajs.cn 返回格式:
        var hq_str_UKOIL="name,0,昨收,价格,最高,最低,涨跌额,涨跌幅,...";
        字段顺序可能变化，核心取价格和涨跌信息
        """
        try:
            # 提取引号内的内容
            quote_start = text.find('"')
            quote_end = text.rfind('"')
            if quote_start == -1 or quote_end == -1 or quote_start == quote_end:
                logger.warning(f"[Sina] {config['display_name']} 响应格式异常: {text[:100]}")
                return None

            content = text[quote_start + 1:quote_end]
            if not content:
                logger.warning(f"[Sina] {config['display_name']} 响应为空")
                return None

            fields = content.split(',')

            # 新浪原油字段布局:
            # 0: 名称, 1: 时间戳/日期, 2: 昨收, 3: 现价,
            # 4: 最高, 5: 最低, 6: 买入, 7: 卖出,
            # 8: 涨跌额, 9: 涨跌幅, ...
            # 不同品种字段位置可能略有差异，按常见布局取值
            price = float(fields[3]) if len(fields) > 3 and fields[3] else 0.0
            prev_close = float(fields[2]) if len(fields) > 2 and fields[2] else 0.0
            change_value = float(fields[8]) if len(fields) > 8 and fields[8] else price - prev_close
            change_pct = float(fields[9]) if len(fields) > 9 and fields[9] else (
                (price - prev_close) / prev_close * 100 if prev_close else 0.0
            )

            # 如果涨跌额为 0 但有昨收和现价，自行计算
            if change_value == 0.0 and prev_close > 0 and price > 0:
                change_value = round(price - prev_close, 4)
                change_pct = round(change_value / prev_close * 100, 2)

            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            return OilDollarDataModel(
                name=name,
                display_name=config['display_name'],
                price=round(price, 2),
                change_value=round(change_value, 2),
                change_pct=round(change_pct, 2),
                source='sina',
                update_time=update_time,
            )
        except (ValueError, IndexError) as e:
            logger.warning(f"[Sina] {config['display_name']} 解析失败: {e}")
            return None

    def fetch_oil_prices(self) -> Dict[str, Any]:
        """采集布伦特和 WTI 原油价格"""
        logger.info("=" * 50)
        logger.info("开始石油价格采集 (新浪财经)")
        logger.info("=" * 50)

        oil_data = {}
        success_count = 0

        for name, config in OIL_CONFIG.items():
            try:
                resp = requests.get(config['url'], headers=SINA_HEADERS, timeout=15)
                resp.encoding = 'gbk'

                if resp.status_code == 200 and resp.text.strip():
                    model = self._parse_sina_response(resp.text, name, config)
                    if model:
                        oil_data[name] = {
                            'status': 'success',
                            'display_name': model.display_name,
                            'price': model.price,
                            'change_value': model.change_value,
                            'change_pct': model.change_pct,
                            'source': model.source,
                        }
                        success_count += 1
                        logger.info(f"[Sina] {model.display_name}: {model.price} ({model.change_pct:+.2f}%)")
                        continue

                logger.warning(f"[Sina] {config['display_name']} 无有效数据")

            except requests.RequestException as e:
                logger.warning(f"[Sina] {config['display_name']} 请求失败: {e}")

            oil_data[name] = {
                'status': 'failed',
                'display_name': config['display_name'],
                'price': 0.0,
                'change_value': 0.0,
                'change_pct': 0.0,
            }

        now = datetime.now()
        result = {
            'date': self.today,
            'update_time': now.strftime('%Y-%m-%d %H:%M:%S'),
            'oil': oil_data,
            'status': 'success' if success_count == len(OIL_CONFIG) else (
                'partial' if success_count > 0 else 'failed'
            ),
        }

        logger.info(f"石油价格采集完成: {success_count}/{len(OIL_CONFIG)} 个成功")
        return result

    def save_to_db(self, data: Dict) -> None:
        """保存石油数据到 MySQL"""
        from services.data_service.models.market_data_models import OilDollarData

        pool = get_db_pool()
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '3306')
        db_user = os.getenv('DB_USER', 'root')
        db_password = os.getenv('DB_PASSWORD', '')
        db_name = os.getenv('DB_NAME', 'quantdb')

        conn_str = (
            f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}'
            f'/{db_name}?charset=utf8mb4'
        )
        pool.get_pool('market_data', conn_str)
        session = pool.get_session('market_data')

        try:
            trade_date = datetime.strptime(data['date'], '%Y-%m-%d').date()
            oil_section = data.get('oil', {})

            for name, info in oil_section.items():
                if info.get('status') != 'success':
                    continue
                row = OilDollarData(
                    trade_date=trade_date,
                    data_type=name,
                    price=info['price'],
                    change_value=info['change_value'],
                    change_pct=info['change_pct'],
                    source=info.get('source', 'sina'),
                )
                session.add(row)

            session.commit()
            logger.info(f"[DB] 石油数据已保存: {data['date']}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] 石油数据保存失败: {e}")
        finally:
            session.close()

    def save_to_json(self, data: Dict) -> None:
        """保存石油数据到 JSON 文件"""
        json_path = self.data_dir / 'oil_dollar_data.json'

        # 精简写入：只保留核心字段，与现有 json 格式对齐
        output = {
            'date': data['date'],
            'update_time': data['update_time'],
            'oil': {},
        }

        for name, info in data.get('oil', {}).items():
            output['oil'][name] = {
                'price': info['price'],
                'change': info['change_value'],
                'change_pct': info['change_pct'],
            }

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        logger.info(f"[JSON] 石油数据已保存: {json_path}")

    def fetch_and_save(self) -> Dict[str, Any]:
        """采集并保存石油数据 (DB + JSON)"""
        data = self.fetch_oil_prices()

        try:
            self.save_to_db(data)
        except Exception as e:
            logger.error(f"[DB] 石油数据写入异常: {e}")

        try:
            self.save_to_json(data)
        except Exception as e:
            logger.error(f"[JSON] 石油数据写入异常: {e}")

        return data


def fetch_oil_dollar() -> Dict[str, Any]:
    """便捷接口：采集并保存石油数据"""
    fetcher = OilDollarFetcher()
    return fetcher.fetch_and_save()


if __name__ == '__main__':
    result = fetch_oil_dollar()
    print(json.dumps(result, indent=2, ensure_ascii=False))
