"""
大宗商品数据采集微服务模块
支持黄金、原油、美元指数等大宗商品数据

数据源:
- Yahoo Finance (主要数据源，需要代理)
- 备用数据源可扩展
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
class CommodityData:
    """大宗商品数据模型"""
    name: str  # 商品代码: gold, oil, usd_index
    display_name: str  # 显示名称
    symbol: str  # 交易代码
    price: float  # 当前价格
    change_value: float  # 涨跌额
    change_pct: float  # 涨跌幅(%)
    previous_close: float  # 昨收
    open_price: Optional[float]  # 开盘价
    high: Optional[float]  # 最高价
    low: Optional[float]  # 最低价
    source: str  # 数据来源
    update_time: str  # 更新时间


class CommodityFetcher:
    """大宗商品采集器 - 微服务版"""

    # 大宗商品配置
    COMMODITIES = {
        'gold': {
            'symbol': 'GLD',
            'yahoo_symbol': 'GLD',
            'display_name': '伦敦金(ETF)',
            'category': 'precious_metal'
        },
        'oil': {
            'symbol': 'CLUSD',
            'yahoo_symbol': 'CL=F',  # WTI原油期货
            'display_name': 'WTI原油',
            'category': 'energy'
        },
        'usd_index': {
            'symbol': 'UUP',
            'yahoo_symbol': 'UUP',  # 美元指数ETF
            'display_name': '美元指数(ETF)',
            'category': 'currency'
        }
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

    async def fetch_yahoo_commodity(self, name: str, config: Dict) -> Optional[CommodityData]:
        """从Yahoo Finance获取单个大宗商品数据"""
        try:
            import requests
            session = self._get_requests_session()

            yahoo_symbol = config['yahoo_symbol']
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=5d"
            headers = {'User-Agent': 'Mozilla/5.0'}

            resp = session.get(url, headers=headers, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                chart_result = data.get('chart', {}).get('result', [])

                if not chart_result:
                    logger.warning(f"[Yahoo] {config['display_name']} 无有效数据")
                    return None

                meta = chart_result[0].get('meta', {})
                price = meta.get('regularMarketPrice', 0)
                prev_close = meta.get('previousClose', 0) or meta.get('chartPreviousClose', 0)

                if not price or not prev_close:
                    logger.warning(f"[Yahoo] {config['display_name']} 无有效价格数据")
                    return None

                change = price - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0

                # 获取OHLC数据
                indicators = chart_result[0].get('indicators', {}).get('quote', [{}])[0]
                timestamps = chart_result[0].get('timestamp', [])

                # 获取最新交易日的数据
                high = None
                low = None
                open_price = None

                if indicators:
                    highs = indicators.get('high', [])
                    lows = indicators.get('low', [])
                    opens = indicators.get('open', [])

                    # 找到最后一个有效值
                    for i in range(len(timestamps) - 1, -1, -1):
                        if highs and i < len(highs) and highs[i]:
                            high = highs[i]
                            break

                    for i in range(len(timestamps) - 1, -1, -1):
                        if lows and i < len(lows) and lows[i]:
                            low = lows[i]
                            break

                    for i in range(len(timestamps) - 1, -1, -1):
                        if opens and i < len(opens) and opens[i]:
                            open_price = opens[i]
                            break

                update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                commodity = CommodityData(
                    name=name,
                    display_name=config['display_name'],
                    symbol=config['symbol'],
                    price=round(price, 2),
                    change_value=round(change, 2),
                    change_pct=round(change_pct, 2),
                    previous_close=round(prev_close, 2),
                    open_price=round(open_price, 2) if open_price else None,
                    high=round(high, 2) if high else None,
                    low=round(low, 2) if low else None,
                    source='yahoo',
                    update_time=update_time
                )

                logger.info(f"[Yahoo] {config['display_name']}: {price} ({change_pct:+.2f}%)")
                return commodity

        except Exception as e:
            logger.warning(f"[Yahoo] {config['display_name']} 失败: {e}")

        return None

    async def fetch_all_commodities(self) -> Dict[str, Any]:
        """采集所有大宗商品数据"""
        logger.info("=" * 50)
        logger.info("开始大宗商品数据采集")
        logger.info("=" * 50)

        result = {
            'date': self.today,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': {},
            'status': 'pending'
        }

        # 并发采集所有大宗商品
        tasks = []
        for name, config in self.COMMODITIES.items():
            tasks.append(self.fetch_yahoo_commodity(name, config))

        commodities = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        for (name, config), commodity in zip(self.COMMODITIES.items(), commodities):
            if isinstance(commodity, Exception):
                logger.error(f"{config['display_name']} 采集异常: {commodity}")
                result['data'][name] = {
                    'status': 'failed',
                    'error': str(commodity)
                }
            elif commodity:
                result['data'][name] = {
                    'status': 'success',
                    'symbol': commodity.symbol,
                    'display_name': commodity.display_name,
                    'price': commodity.price,
                    'change_value': commodity.change_value,
                    'change_pct': commodity.change_pct,
                    'previous_close': commodity.previous_close,
                    'open': commodity.open_price,
                    'high': commodity.high,
                    'low': commodity.low,
                    'source': commodity.source
                }
                success_count += 1
            else:
                result['data'][name] = {
                    'status': 'failed',
                    'error': '无有效数据'
                }

        # 设置整体状态
        if success_count == len(self.COMMODITIES):
            result['status'] = 'success'
        elif success_count > 0:
            result['status'] = 'partial'
        else:
            result['status'] = 'failed'

        logger.info(f"大宗商品采集完成: {success_count}/{len(self.COMMODITIES)} 个成功")

        return result


# 同步包装函数
async def fetch_commodities_via_service() -> Dict[str, Any]:
    """通过微服务获取大宗商品数据"""
    fetcher = CommodityFetcher()
    return await fetcher.fetch_all_commodities()


def fetch_commodities() -> Dict[str, Any]:
    """同步接口：获取大宗商品数据"""
    return asyncio.run(fetch_commodities_via_service())


if __name__ == "__main__":
    # 测试
    import json
    result = fetch_commodities()
    print(json.dumps(result, indent=2, ensure_ascii=False))
