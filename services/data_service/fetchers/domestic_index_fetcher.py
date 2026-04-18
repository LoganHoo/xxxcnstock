"""
国内大盘指数采集微服务模块
支持上证指数、深证成指、创业板指等国内主要指数

数据源:
- akshare (主要数据源)
- 手动更新脚本 (备用)
"""
import asyncio
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, date, time as dt_time
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
from core.logger import get_logger
import polars as pl

logger = get_logger(__name__)


@dataclass
class DomesticIndexData:
    """国内指数数据模型"""
    code: str  # 指数代码
    symbol: str  # 完整代码
    name: str  # 指数名称
    close: float  # 收盘价
    open: float  # 开盘价
    high: float  # 最高价
    low: float  # 最低价
    prev_close: float  # 昨收
    change: float  # 涨跌额
    change_pct: float  # 涨跌幅(%)
    volume: float  # 成交量
    amount: float  # 成交额
    source: str  # 数据来源
    trade_date: str  # 交易日期


class DomesticIndexFetcher:
    """国内指数采集器 - 微服务版"""

    # 指数配置列表
    INDICES: List[Tuple[str, str, str]] = [
        ('sh000001', '000001', '上证指数'),
        ('sz399001', '399001', '深证成指'),
        ('sz399006', '399006', '创业板指'),
        ('sh000300', '000300', '沪深300'),
        ('sh000016', '000016', '上证50'),
        ('sh000905', '000905', '中证500'),
    ]

    def __init__(self, data_dir: Optional[Path] = None):
        self.project_root = Path(__file__).parent.parent.parent.parent
        self.data_dir = data_dir or (self.project_root / "data" / "index")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.today = datetime.now().strftime('%Y-%m-%d')

    def check_data_freshness(self) -> Dict[str, Any]:
        """检查指数数据新鲜度

        逻辑：
        - 收盘前（15:00前）：昨天数据也算最新
        - 收盘后（15:00后）：需要今天数据
        """
        now = datetime.now()
        today = now.date()
        market_close = dt_time(15, 0)

        # 收盘后需要今天数据，收盘前昨天数据也算最新
        is_after_close = now.time() >= market_close
        max_acceptable_lag = 0 if is_after_close else 1

        status = {}

        for symbol, code, name in self.INDICES:
            parquet_file = self.data_dir / f"{code}.parquet"
            if parquet_file.exists():
                try:
                    df = pl.read_parquet(parquet_file)
                    latest_date = df['trade_date'].max()
                    latest_date = datetime.strptime(str(latest_date), '%Y-%m-%d').date() if isinstance(latest_date, str) else latest_date
                    days_diff = (today - latest_date).days

                    status[code] = {
                        'name': name,
                        'latest_date': latest_date,
                        'days_diff': days_diff,
                        'is_fresh': days_diff <= max_acceptable_lag,
                        'rows': len(df),
                        'is_after_close': is_after_close
                    }
                except Exception as e:
                    status[code] = {
                        'name': name,
                        'error': str(e),
                        'is_fresh': False
                    }
            else:
                status[code] = {
                    'name': name,
                    'error': '文件不存在',
                    'is_fresh': False
                }

        return status

    async def fetch_via_akshare(self) -> Dict[str, DomesticIndexData]:
        """使用akshare采集指数数据"""
        logger.info("使用akshare采集大盘指数...")
        result = {}

        try:
            # 清除代理设置，避免网络连接问题
            env = os.environ.copy()
            for key in list(env.keys()):
                if 'proxy' in key.lower():
                    del env[key]

            # 调用akshare采集脚本
            script_path = self.project_root / "scripts" / "fetch_index_data.py"
            if not script_path.exists():
                logger.error(f"采集脚本不存在: {script_path}")
                return result

            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=str(self.project_root)
            )

            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)

            if proc.returncode == 0:
                logger.info("akshare采集成功")
                # 重新读取数据文件
                result = self._load_index_data_from_files()
            else:
                logger.warning(f"akshare采集失败: {stderr.decode()}")

        except asyncio.TimeoutError:
            logger.error("akshare采集超时")
        except Exception as e:
            logger.error(f"akshare采集异常: {e}")

        return result

    async def fetch_via_manual(self) -> Dict[str, DomesticIndexData]:
        """使用手动更新脚本"""
        logger.info("使用手动更新脚本...")
        result = {}

        try:
            script_path = self.project_root / "scripts" / "update_index_manual.py"
            if not script_path.exists():
                logger.error(f"手动更新脚本不存在: {script_path}")
                return result

            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.project_root)
            )

            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

            if proc.returncode == 0:
                logger.info("手动更新成功")
                # 重新读取数据文件
                result = self._load_index_data_from_files()
            else:
                logger.warning(f"手动更新失败: {stderr.decode()}")

        except asyncio.TimeoutError:
            logger.error("手动更新超时")
        except Exception as e:
            logger.error(f"手动更新异常: {e}")

        return result

    def _load_index_data_from_files(self) -> Dict[str, DomesticIndexData]:
        """从Parquet文件加载指数数据"""
        result = {}

        for symbol, code, name in self.INDICES:
            parquet_file = self.data_dir / f"{code}.parquet"
            if parquet_file.exists():
                try:
                    df = pl.read_parquet(parquet_file)
                    if len(df) > 0:
                        # 获取最新数据
                        latest = df.tail(1).to_dicts()[0]

                        result[code] = DomesticIndexData(
                            code=code,
                            symbol=symbol,
                            name=name,
                            close=float(latest.get('close', 0)),
                            open=float(latest.get('open', 0)),
                            high=float(latest.get('high', 0)),
                            low=float(latest.get('low', 0)),
                            prev_close=float(latest.get('preclose', latest.get('open', 0))),
                            change=float(latest.get('change', 0)),
                            change_pct=float(latest.get('pct_chg', 0)),
                            volume=float(latest.get('volume', 0)),
                            amount=float(latest.get('amount', 0)),
                            source='akshare',
                            trade_date=str(latest.get('trade_date', self.today))
                        )
                except Exception as e:
                    logger.warning(f"读取{code}数据失败: {e}")

        return result

    async def fetch_all_domestic_indices(self, use_manual_fallback: bool = True) -> Dict[str, Any]:
        """采集所有国内指数"""
        logger.info("=" * 50)
        logger.info("开始国内指数采集任务")
        logger.info("=" * 50)

        # 1. 检查当前数据新鲜度
        logger.info("检查当前数据状态...")
        before_status = self.check_data_freshness()
        fresh_count = sum(1 for s in before_status.values() if s.get('is_fresh', False))
        logger.info(f"当前数据新鲜度: {fresh_count}/{len(self.INDICES)} 个指数已更新")

        # 如果所有数据都是最新的，直接返回
        if fresh_count == len(self.INDICES):
            logger.info("所有指数数据已是最新，无需采集")
            data = self._load_index_data_from_files()
            return {
                'date': self.today,
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': {k: {
                    'code': v.code,
                    'name': v.name,
                    'close': v.close,
                    'open': v.open,
                    'high': v.high,
                    'low': v.low,
                    'prev_close': v.prev_close,
                    'change': v.change,
                    'change_pct': v.change_pct,
                    'volume': v.volume,
                    'amount': v.amount,
                    'trade_date': v.trade_date
                } for k, v in data.items()},
                'status': 'success',
                'message': '数据已是最新'
            }

        # 2. 尝试akshare采集
        logger.info("尝试akshare采集...")
        data = await self.fetch_via_akshare()

        # 3. 如果失败且允许手动回退，尝试手动更新
        if len(data) < len(self.INDICES) and use_manual_fallback:
            logger.info("akshare采集不完整，尝试手动更新...")
            manual_data = await self.fetch_via_manual()
            # 合并数据
            data.update({k: v for k, v in manual_data.items() if k not in data})

        # 4. 检查结果
        if len(data) >= len(self.INDICES) * 0.8:  # 80%成功率视为成功
            status = 'success'
        elif len(data) > 0:
            status = 'partial'
        else:
            status = 'failed'

        # 5. 再次检查数据新鲜度
        after_status = self.check_data_freshness()
        after_fresh_count = sum(1 for s in after_status.values() if s.get('is_fresh', False))

        result = {
            'date': self.today,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': {k: {
                'code': v.code,
                'name': v.name,
                'close': v.close,
                'open': v.open,
                'high': v.high,
                'low': v.low,
                'prev_close': v.prev_close,
                'change': v.change,
                'change_pct': v.change_pct,
                'volume': v.volume,
                'amount': v.amount,
                'trade_date': v.trade_date
            } for k, v in data.items()},
            'status': status,
            'fresh_count': f"{after_fresh_count}/{len(self.INDICES)}"
        }

        logger.info(f"国内指数采集完成: {len(data)}/{len(self.INDICES)} 个成功")

        return result


# 同步包装函数
async def fetch_domestic_indices_via_service(data_dir: Optional[Path] = None) -> Dict[str, Any]:
    """通过微服务获取国内指数数据"""
    fetcher = DomesticIndexFetcher(data_dir)
    return await fetcher.fetch_all_domestic_indices()


def fetch_domestic_indices(data_dir: Optional[Path] = None) -> Dict[str, Any]:
    """同步接口：获取国内指数数据"""
    return asyncio.run(fetch_domestic_indices_via_service(data_dir))


if __name__ == "__main__":
    # 测试
    import json
    result = fetch_domestic_indices()
    print(json.dumps(result, indent=2, ensure_ascii=False))
