#!/usr/bin/env python3
"""
外盘指数数据采集脚本
【06:00执行】采集美股、亚洲股指数据

支持多数据源:
1. Yahoo Finance (需要代理)
2. Sina Finance (新浪)
3. Eastmoney (东方财富)
"""
import sys
import json
import logging
import os
import socket
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ForeignIndexFetcher:
    """外盘指数采集器"""

    def __init__(self):
        self.output_path = project_root / "data" / "foreign_index.json"
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.proxies = self._detect_proxies()

    def _detect_proxies(self) -> dict:
        """检测并设置代理"""
        proxies = {
            'http': os.getenv('http_proxy', os.getenv('HTTP_PROXY', '')),
            'https': os.getenv('https_proxy', os.getenv('HTTPS_PROXY', '')),
            'all': os.getenv('all_proxy', os.getenv('ALL_PROXY', ''))
        }
        proxies = {k: v for k, v in proxies.items() if v}

        if not proxies and self._is_docker():
            host_ip = self._get_host_ip()
            if host_ip:
                proxy_url = f"http://{host_ip}:7890"
                proxies = {
                    'http': proxy_url,
                    'https': proxy_url,
                    'all': f"socks5://{host_ip}:7890"
                }
                logger.info(f"检测到Docker环境，使用宿主机代理: {host_ip}:7890")

        return proxies

    def _is_docker(self) -> bool:
        """检测是否在Docker容器中运行"""
        return os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER', '')

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
            logger.info(f"使用代理: {self.proxies}")
        return session

    def fetch_yahoo_us_index(self) -> dict:
        """Yahoo Finance 美股指数"""
        result = {'nasdaq': None, 'sp500': None, 'dow': None}

        try:
            import requests
            session = self._get_requests_session()

            indices = {
                'nasdaq': ('^IXIC', 'NASDAQ Composite'),
                'sp500': ('^GSPC', 'S&P 500'),
                'dow': ('^DJI', 'Dow Jones')
            }

            for name, (symbol, display_name) in indices.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
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
                        logger.info(f"[Yahoo] {display_name}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Yahoo] {name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Yahoo] 采集异常: {e}")

        return result

    def fetch_sina_us_index(self) -> dict:
        """新浪财经 美股指数"""
        result = {'nasdaq': None, 'sp500': None, 'dow': None}

        try:
            import requests
            session = self._get_requests_session()

            indices = {
                'nasdaq': 'gb_ixic',
                'sp500': 'gb_sp500',
                'dow': 'gb_dji'
            }

            for name, sina_code in indices.items():
                try:
                    url = f"https://hq.sinajs.cn/list={sina_code}"
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

    def fetch_eastmoney_index(self) -> dict:
        """东方财富 美股指数"""
        result = {}

        try:
            import requests
            session = self._get_requests_session()

            indices = {
                'nasdaq': ('IXIC', '纳斯达克'),
                'sp500': ('GSPC', '标普500'),
                'dow': ('DJI', '道琼斯')
            }

            for name, (symbol, display_name) in indices.items():
                try:
                    url = f"https://push2.eastmoney.com/api/qt/stock/get"
                    params = {
                        'secid': f'1.{symbol}',
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
                        logger.info(f"[Eastmoney] {display_name}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Eastmoney] {name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Eastmoney] 采集异常: {e}")

        return result

    def merge_us_index_results(self, yahoo: dict, sina: dict, eastmoney: dict) -> dict:
        """合并多数据源结果，取最优值"""
        final = {}
        indices = ['nasdaq', 'sp500', 'dow']

        for name in indices:
            sources = []
            for source_data, source_name in [(yahoo, 'yahoo'), (sina, 'sina'), (eastmoney, 'eastmoney')]:
                if source_data.get(name):
                    sources.append(source_data[name])

            if sources:
                final[name] = max(sources, key=lambda x: x['price'] if x['price'] else 0)
                final[name]['sources'] = [s['source'] for s in sources]
                logger.info(f"[Merge] {name}: {final[name]['price']} (来源: {final[name]['sources']})")
            else:
                logger.warning(f"[Merge] {name}: 所有数据源均失败")

        return final

    def fetch_us_index_data(self) -> dict:
        """采集美股指数（多数据源）"""
        logger.info("-" * 50)
        logger.info("开始采集美股指数（多数据源）...")

        yahoo_data = self.fetch_yahoo_us_index()
        sina_data = self.fetch_sina_us_index()
        eastmoney_data = self.fetch_eastmoney_index()

        merged = self.merge_us_index_results(yahoo_data, sina_data, eastmoney_data)

        result = {
            'name': '美股指数',
            'data': merged,
            'status': 'success' if merged else 'failed'
        }

        if not merged:
            result['error'] = '所有数据源均失败'

        logger.info(f"美股指数采集完成: {list(merged.keys())}")
        return result

    def fetch_asia_index_data(self) -> dict:
        """采集亚洲股指（多数据源）"""
        logger.info("-" * 50)
        logger.info("开始采集亚洲股指...")

        result = {
            'name': '亚洲股指',
            'data': {},
            'status': 'pending'
        }

        try:
            import requests
            session = self._get_requests_session()

            indices = {
                'hang_seng': ('hkHSI', '恒生指数'),
                'hscei': ('hkHSCEI', 'H股指数'),
                'hstech': ('hkHSTECH', '恒生科技')
            }

            for name, (sina_code, display_name) in indices.items():
                try:
                    url = f"https://hq.sinajs.cn/list={sina_code}"
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

                                result['data'][name] = {
                                    'price': round(price, 2),
                                    'change': round(change, 2),
                                    'change_pct': round(change_pct, 2),
                                    'source': 'sina'
                                }
                                logger.info(f"[Sina] {display_name}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Sina] {display_name} 失败: {e}")

        except Exception as e:
            logger.error(f"[Sina] 亚洲股指采集异常: {e}")

        if result['data']:
            result['status'] = 'success'
        else:
            result['status'] = 'failed'
            result['error'] = '所有API均失败'
            logger.warning("亚洲股指: 所有API均失败")

        return result

    def fetch_commodity_data(self) -> dict:
        """采集大宗商品数据（黄金、原油、美元指数）"""
        logger.info("-" * 50)
        logger.info("开始采集大宗商品数据...")

        result = {
            'name': '大宗商品',
            'data': {},
            'status': 'pending'
        }

        try:
            import requests
            session = self._get_requests_session()

            commodities = {
                'gold': {'symbol': 'GLD', 'name': '伦敦金(ETF)'},
                'oil': {'symbol': 'CLUSD', 'name': 'WTI原油'},
                'usd_index': {'symbol': 'UUP', 'name': '美元指数(ETF)'}
            }

            commodity_urls = {
                'gold': "https://query1.finance.yahoo.com/v8/finance/chart/GLD?interval=1d&range=5d",
                'oil': "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?interval=1d&range=5d",
                'usd_index': "https://query1.finance.yahoo.com/v8/finance/chart/UUP?interval=1d&range=5d"
            }

            for name, info in commodities.items():
                try:
                    url = commodity_urls.get(name)
                    if not url:
                        continue
                        
                    headers = {'User-Agent': 'Mozilla/5.0'}
                    resp = session.get(url, headers=headers, timeout=15)

                    if resp.status_code == 200:
                        data = resp.json()
                        chart_result = data.get('chart', {}).get('result', [])
                        if chart_result:
                            meta = chart_result[0].get('meta', {})
                            price = meta.get('regularMarketPrice', 0)
                            prev_close = meta.get('previousClose', 0) or meta.get('chartPreviousClose', 0)
                            
                            if not price or not prev_close:
                                logger.warning(f"[Yahoo] {info['name']} 无有效价格数据")
                                continue
                                
                            change = price - prev_close
                            change_pct = (change / prev_close * 100) if prev_close else 0
                            
                            indicators = chart_result[0].get('indicators', {}).get('quote', [{}])[0]
                            high = indicators.get('high', [None])[-1] if indicators else None
                            low = indicators.get('low', [None])[-1] if indicators else None
                            open_price = indicators.get('open', [None])[-1] if indicators else None

                            result['data'][name] = {
                                'symbol': info['symbol'],
                                'name': info['name'],
                                'price': round(price, 2),
                                'change_value': round(change, 2),
                                'change_pct': round(change_pct, 2),
                                'previous_close': round(prev_close, 2),
                                'high': round(high, 2) if high else None,
                                'low': round(low, 2) if low else None,
                                'open': round(open_price, 2) if open_price else None,
                                'source': 'yahoo'
                            }
                            logger.info(f"[Yahoo] {info['name']}: {price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"[Yahoo] {info['name']} 失败: {e}")

        except Exception as e:
            logger.error(f"大宗商品采集异常: {e}")

        if result['data']:
            result['status'] = 'success'
        else:
            result['status'] = 'failed'
            result['error'] = '所有API均失败'

        return result

    def save_to_mysql(self, foreign_data: dict) -> bool:
        """保存外盘和大宗商品数据到MySQL"""
        logger.info("开始保存数据到MySQL...")

        try:
            import pymysql
            from pymysql.cursors import DictCursor

            conn = pymysql.connect(
                host='49.233.10.199',
                port=3306,
                user='nextai',
                password='100200',
                database='xcn_db',
                charset='utf8mb4',
                cursorclass=DictCursor
            )

            trade_date = foreign_data.get('date', datetime.now().strftime('%Y-%m-%d'))

            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM foreign_commodity_data WHERE trade_date = %s", (trade_date,))
                logger.info(f"清理旧数据: 删除{cursor.rowcount}条{trade_date}的旧记录")
                
                us_data = foreign_data.get('us_index', {}).get('data', {})
                for symbol, data in us_data.items():
                    name_map = {'nasdaq': '纳斯达克', 'sp500': '标普500', 'dow': '道琼斯'}
                    cursor.execute("""
                        INSERT INTO foreign_commodity_data 
                        (trade_date, data_type, symbol, name, price, change_value, change_pct, previous_close, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        price = VALUES(price), change_value = VALUES(change_value),
                        change_pct = VALUES(change_pct), previous_close = VALUES(previous_close),
                        source = VALUES(source)
                    """, (
                        trade_date, 'us_index', symbol, name_map.get(symbol, symbol),
                        data.get('price'), data.get('change'), data.get('change_pct'),
                        data.get('previous_close'), data.get('source')
                    ))

                asia_data = foreign_data.get('asia_index', {}).get('data', {})
                for symbol, data in asia_data.items():
                    name_map = {'hang_seng': '恒生指数', 'hscei': 'H股指数', 'hstech': '恒生科技'}
                    cursor.execute("""
                        INSERT INTO foreign_commodity_data 
                        (trade_date, data_type, symbol, name, price, change_value, change_pct, previous_close, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        price = VALUES(price), change_value = VALUES(change_value),
                        change_pct = VALUES(change_pct), previous_close = VALUES(previous_close),
                        source = VALUES(source)
                    """, (
                        trade_date, 'asia_index', symbol, name_map.get(symbol, symbol),
                        data.get('price'), data.get('change'), data.get('change_pct'),
                        data.get('previous_close'), data.get('source')
                    ))

                commodity_data = foreign_data.get('commodity', {}).get('data', {})
                for symbol, data in commodity_data.items():
                    cursor.execute("""
                        INSERT INTO foreign_commodity_data 
                        (trade_date, data_type, symbol, name, price, change_value, change_pct, 
                         previous_close, high, low, open, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        price = VALUES(price), change_value = VALUES(change_value),
                        change_pct = VALUES(change_pct), previous_close = VALUES(previous_close),
                        high = VALUES(high), low = VALUES(low), open = VALUES(open),
                        source = VALUES(source)
                    """, (
                        trade_date, 'commodity', data.get('symbol'), data.get('name'),
                        data.get('price'), data.get('change_value'), data.get('change_pct'),
                        data.get('previous_close'), data.get('high'), data.get('low'),
                        data.get('open'), data.get('source')
                    ))

            conn.commit()
            conn.close()
            logger.info("数据已保存到MySQL")
            return True

        except Exception as e:
            logger.error(f"保存到MySQL失败: {e}")
            return False

    def run(self) -> dict:
        """执行外盘指数采集"""
        logger.info("=" * 60)
        logger.info("开始采集外盘指数数据（多数据源+代理）")
        logger.info("=" * 60)

        us_index = self.fetch_us_index_data()
        asia_index = self.fetch_asia_index_data()
        commodity = self.fetch_commodity_data()

        foreign_data = {
            'date': self.today,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'us_index': us_index,
            'asia_index': asia_index,
            'commodity': commodity,
            'proxies': {k: '***' for k in self.proxies.keys()}
        }

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(foreign_data, f, ensure_ascii=False, indent=2)

        logger.info(f"外盘指数数据已保存: {self.output_path}")
        logger.info(f"  美股: {us_index['status']}")
        logger.info(f"  亚洲: {asia_index['status']}")
        logger.info(f"  大宗商品: {commodity['status']}")

        self.save_to_mysql(foreign_data)

        return foreign_data


if __name__ == '__main__':
    fetcher = ForeignIndexFetcher()
    result = fetcher.run()

    print("\n" + "=" * 60)
    print("外盘指数采集结果")
    print("=" * 60)
    print(f"美股: {result['us_index']['status']}")
    for name, data in result['us_index'].get('data', {}).items():
        sources = data.get('sources', [data.get('source')])
        print(f"  {name}: {data['price']} ({data['change_pct']:+.2f}%) 来源: {sources}")
    print(f"亚洲: {result['asia_index']['status']}")
    for name, data in result['asia_index'].get('data', {}).items():
        print(f"  {name}: {data['price']} ({data['change_pct']:+.2f}%)")
    print(f"大宗商品: {result['commodity']['status']}")
    for name, data in result['commodity'].get('data', {}).items():
        print(f"  {name}: {data['name']} {data['price']} ({data['change_pct']:+.2f}%)")