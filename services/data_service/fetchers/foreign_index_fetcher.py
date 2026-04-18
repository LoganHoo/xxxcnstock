"""
外盘指数采集微服务模块
支持美股指数、亚洲股指的多数据源采集

数据源:
- Yahoo Finance (国际数据源，需要代理)
- Sina Finance (新浪，国内访问)
- Eastmoney (东方财富，国内访问)
"""
import asyncio
import os
import socket
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ForeignIndexData:
    """外盘指数数据模型"""
    name: str  # 指数名称: nasdaq, sp500, dow, hang_seng, hscei, hstech
    display_name: str  # 显示名称
    price: float  # 当前价格
    change: float  # 涨跌额
    change_pct: float  # 涨跌幅(%)
    previous_close: float  # 昨收
    source: str  # 数据来源
    update_time: str  # 更新时间
    region: str  # 地区: us, asia


class ForeignIndexFetcher:
    """外盘指数采集器 - 微服务版"""

    # 美股指数配置
    US_INDICES = {
        'nasdaq': {'symbol': '^IXIC', 'sina_code': 'gb_ixic', 'eastmoney_code': 'IXIC', 'display_name': 'NASDAQ Composite'},
        'sp500': {'symbol': '^GSPC', 'sina_code': 'gb_sp500', 'eastmoney_code': 'GSPC', 'display_name': 'S&P 500'},
        'dow': {'symbol': '^DJI', 'sina_code': 'gb_dji', 'eastmoney_code': 'DJI', 'display_name': 'Dow Jones'}
    }

    # 亚洲股指配置
    ASIA_INDICES = {
        'hang_seng': {'sina_code': 'hkHSI', 'display_name': '恒生指数'},
        'hscei': {'sina_code': 'hkHSCEI', 'display_name': 'H股指数'},
        'hstech': {'sina_code': 'hkHSTECH', 'display_name': '恒生科技'}
    }

    def __init__(self):
        self.proxies = self._detect_proxies()
        self.today = datetime.now().strftime('%Y-%m-%d')

    def _detect_proxies(self) -> Dict[str, str]:
        """检测并设置代理"""
        proxies = {
            'http': os.getenv('http_proxy', os.getenv('HTTP_PROXY', '')),
            'https': os.getenv('https_proxy', os.getenv('HTTPS_PROXY', '')),
        }
        proxies = {k: v for k, v in proxies.items() if v}

        # Docker环境检测
        if not proxies and self._is_docker():
            host_ip = self._get_host_ip()
            if host_ip:
                proxy_url = f"http://{host_ip}:7890"
                proxies = {'http': proxy_url, 'https': proxy_url}
                logger.info(f"Docker环境使用宿主机代理: {host_ip}:7890")

        return proxies

    def _is_docker(self) -> bool:
        """检测是否在Docker容器中运行"""
        return os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER', '') == 'true'

    def _get_host_ip(self) -> Optional[str]:
        """获取宿主机IP"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            host_ip = s.getsockname()[0]
            s.close()
            return host_ip
        except Exception:
            return None

    def _get_requests_session(self):
        """获取配置了代理的requests会话"""
        import requests
        session = requests.Session()
        if self.proxies:
            session.proxies.update(self.proxies)
        return session

    async def fetch_yahoo_us_index(self) -> Dict[str, Optional[Dict]]:
        """Yahoo Finance 美股指数"""
        result = {}

        try:
            import requests
            session = self._get_requests_session()

            for name, config in self.US_INDICES.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{config['symbol']}"
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    resp = session.get(url, headers=headers, timeout=15)

                    if resp.status_code == 200:
                        data = resp.json()
                        meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
                        price = meta.get('regularMarketPrice', 0)
                        prev_close = meta.get('previousClose', 0) or meta.get('chartPreviousClose', 0)
                        change = price - prev_close
                        change_pct = (change / prev_close * 100) if prev_close else 0

                        result[name] = {
                            'price': round(price, 2),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2),
                            'previous_close': round(prev_close, 2),
                            'source': 'yahoo'
                        }
                        logger.info(f"[Yahoo] {config['display_name']}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Yahoo] {name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Yahoo] 采集异常: {e}")

        return result

    async def fetch_sina_us_index(self) -> Dict[str, Optional[Dict]]:
        """新浪财经 美股指数"""
        result = {}

        try:
            import requests
            session = self._get_requests_session()

            for name, config in self.US_INDICES.items():
                try:
                    url = f"https://hq.sinajs.cn/list={config['sina_code']}"
                    headers = {
                        'Referer': 'https://finance.sina.com.cn',
                        'User-Agent': 'Mozilla/5.0'
                    }
                    resp = session.get(url, headers=headers, timeout=10)

                    if resp.status_code == 200:
                        text = resp.text
                        if '"' in text and text.split('"')[1].strip():
                            parts = text.split('"')[1].split(',')
                            if len(parts) > 4:
                                price = float(parts[1]) if parts[1].replace('.', '').replace('-', '').isdigit() else 0
                                prev_close = float(parts[5]) if parts[5].replace('.', '').replace('-', '').isdigit() else price
                                change = price - prev_close
                                change_pct = (change / prev_close * 100) if prev_close else 0

                                result[name] = {
                                    'price': round(price, 2),
                                    'change': round(change, 2),
                                    'change_pct': round(change_pct, 2),
                                    'previous_close': round(prev_close, 2),
                                    'source': 'sina'
                                }
                                logger.info(f"[Sina] {name}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Sina] {name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Sina] 采集异常: {e}")

        return result

    async def fetch_eastmoney_us_index(self) -> Dict[str, Optional[Dict]]:
        """东方财富 美股指数"""
        result = {}

        try:
            import requests
            session = self._get_requests_session()

            for name, config in self.US_INDICES.items():
                try:
                    url = "https://push2.eastmoney.com/api/qt/stock/get"
                    params = {
                        'secid': f'1.{config["eastmoney_code"]}',
                        'fields': 'f43,f169,f170',
                        'ut': 'fa5fd1943c7b386f172d6893dbfba10b'
                    }
                    headers = {
                        'Referer': 'https://quote.eastmoney.com',
                        'User-Agent': 'Mozilla/5.0'
                    }
                    resp = session.get(url, params=params, headers=headers, timeout=10)

                    if resp.status_code == 200:
                        data = resp.json().get('data', {})
                        price = data.get('f43', 0) / 100 if data.get('f43') else 0
                        prev_close = data.get('f170', 0) / 100 if data.get('f170') else 0
                        change = price - prev_close
                        change_pct = (change / prev_close * 100) if prev_close else 0

                        result[name] = {
                            'price': round(price, 2),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2),
                            'previous_close': round(prev_close, 2),
                            'source': 'eastmoney'
                        }
                        logger.info(f"[Eastmoney] {config['display_name']}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Eastmoney] {name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Eastmoney] 采集异常: {e}")

        return result

    def _merge_us_results(self, yahoo: Dict, sina: Dict, eastmoney: Dict) -> Dict[str, ForeignIndexData]:
        """合并多数据源美股结果，取最优值"""
        final = {}
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for name, config in self.US_INDICES.items():
            sources = []
            for source_data, source_name in [(yahoo, 'yahoo'), (sina, 'sina'), (eastmoney, 'eastmoney')]:
                if source_data.get(name) and source_data[name].get('price'):
                    sources.append(source_data[name])

            if sources:
                # 选择价格最高的作为有效数据
                best = max(sources, key=lambda x: x['price'])
                final[name] = ForeignIndexData(
                    name=name,
                    display_name=config['display_name'],
                    price=best['price'],
                    change=best['change'],
                    change_pct=best['change_pct'],
                    previous_close=best['previous_close'],
                    source=best['source'],
                    update_time=update_time,
                    region='us'
                )
                logger.info(f"[Merge] {name}: {best['price']} (来源: {best['source']})")
            else:
                logger.warning(f"[Merge] {name}: 所有数据源均失败")

        return final

    async def fetch_us_index_data(self) -> Dict[str, ForeignIndexData]:
        """采集美股指数（多数据源）"""
        logger.info("开始采集美股指数（多数据源）...")

        # 并发采集多个数据源
        yahoo_data, sina_data, eastmoney_data = await asyncio.gather(
            self.fetch_yahoo_us_index(),
            self.fetch_sina_us_index(),
            self.fetch_eastmoney_us_index(),
            return_exceptions=True
        )

        # 处理异常
        if isinstance(yahoo_data, Exception):
            logger.error(f"Yahoo采集异常: {yahoo_data}")
            yahoo_data = {}
        if isinstance(sina_data, Exception):
            logger.error(f"Sina采集异常: {sina_data}")
            sina_data = {}
        if isinstance(eastmoney_data, Exception):
            logger.error(f"Eastmoney采集异常: {eastmoney_data}")
            eastmoney_data = {}

        return self._merge_us_results(yahoo_data, sina_data, eastmoney_data)

    async def fetch_asia_index_data(self) -> Dict[str, ForeignIndexData]:
        """采集亚洲股指"""
        logger.info("开始采集亚洲股指...")

        result = {}
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            import requests
            session = self._get_requests_session()

            for name, config in self.ASIA_INDICES.items():
                try:
                    url = f"https://hq.sinajs.cn/list={config['sina_code']}"
                    headers = {
                        'Referer': 'https://finance.sina.com.cn',
                        'User-Agent': 'Mozilla/5.0'
                    }
                    resp = session.get(url, headers=headers, timeout=10)

                    if resp.status_code == 200:
                        text = resp.text
                        if '"' in text and '""' not in text:
                            parts = text.split('"')[1].split(',')
                            if len(parts) > 4:
                                price = float(parts[2]) if parts[2].replace('.', '').replace('-', '').isdigit() else 0
                                prev_close = float(parts[3]) if parts[3].replace('.', '').replace('-', '').isdigit() else price
                                change = price - prev_close
                                change_pct = (change / prev_close * 100) if prev_close else 0

                                result[name] = ForeignIndexData(
                                    name=name,
                                    display_name=config['display_name'],
                                    price=round(price, 2),
                                    change=round(change, 2),
                                    change_pct=round(change_pct, 2),
                                    previous_close=round(prev_close, 2),
                                    source='sina',
                                    update_time=update_time,
                                    region='asia'
                                )
                                logger.info(f"[Sina] {config['display_name']}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Sina] {config['display_name']} 失败: {e}")

        except Exception as e:
            logger.error(f"[Sina] 亚洲股指采集异常: {e}")

        return result

    async def fetch_all_foreign_indices(self) -> Dict[str, Any]:
        """采集所有外盘指数"""
        logger.info("=" * 50)
        logger.info("开始外盘指数采集任务")
        logger.info("=" * 50)

        # 并发采集美股和亚洲股指
        us_data, asia_data = await asyncio.gather(
            self.fetch_us_index_data(),
            self.fetch_asia_index_data(),
            return_exceptions=True
        )

        result = {
            'date': self.today,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'us_index': {},
            'asia_index': {},
            'status': 'success'
        }

        # 处理美股数据
        if isinstance(us_data, Exception):
            logger.error(f"美股指数采集失败: {us_data}")
            result['us_index']['status'] = 'failed'
            result['us_index']['error'] = str(us_data)
        else:
            result['us_index'] = {
                'status': 'success' if us_data else 'failed',
                'data': {k: {
                    'price': v.price,
                    'change': v.change,
                    'change_pct': v.change_pct,
                    'previous_close': v.previous_close,
                    'source': v.source
                } for k, v in us_data.items()}
            }

        # 处理亚洲股指数据
        if isinstance(asia_data, Exception):
            logger.error(f"亚洲股指采集失败: {asia_data}")
            result['asia_index']['status'] = 'failed'
            result['asia_index']['error'] = str(asia_data)
        else:
            result['asia_index'] = {
                'status': 'success' if asia_data else 'failed',
                'data': {k: {
                    'price': v.price,
                    'change': v.change,
                    'change_pct': v.change_pct,
                    'previous_close': v.previous_close,
                    'source': v.source
                } for k, v in asia_data.items()}
            }

        # 整体状态判断
        if (result['us_index'].get('status') == 'failed' and
            result['asia_index'].get('status') == 'failed'):
            result['status'] = 'failed'

        logger.info(f"外盘指数采集完成: 美股{len(us_data) if not isinstance(us_data, Exception) else 0}个, "
                   f"亚洲{len(asia_data) if not isinstance(asia_data, Exception) else 0}个")

        return result


# 同步包装函数
async def fetch_foreign_indices_via_service() -> Dict[str, Any]:
    """通过微服务获取外盘指数数据"""
    fetcher = ForeignIndexFetcher()
    return await fetcher.fetch_all_foreign_indices()


def fetch_foreign_indices() -> Dict[str, Any]:
    """同步接口：获取外盘指数数据"""
    return asyncio.run(fetch_foreign_indices_via_service())


if __name__ == "__main__":
    # 测试
    result = fetch_foreign_indices()
    print(json.dumps(result, indent=2, ensure_ascii=False))
