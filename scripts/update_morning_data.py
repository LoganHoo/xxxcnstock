#!/usr/bin/env python3
"""
晨间数据更新脚本
【08:30执行】更新隔夜外盘数据、A股盘前数据
"""
import sys
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MorningDataUpdater:
    """晨间数据更新器"""

    def __init__(self):
        self.data_dir = project_root / "data"
        self.log_dir = project_root / "logs"
        self.morning_data_path = self.data_dir / "morning_data.json"
        self.today = datetime.now().strftime('%Y-%m-%d')

    def fetch_us_index_data(self) -> dict:
        """采集美股指数数据"""
        result = {
            'name': '美股指数',
            'data': {},
            'status': 'pending'
        }

        try:
            import requests

            indices = {
                'nasdaq': ('IXIC', '^IXIC'),
                'sp500': ('GSPC', '^GSPC'),
                'dow': ('DJI', '^DJI')
            }

            for name, (symbol, yahoo_sym) in indices.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}"
                    response = requests.get(url, timeout=10)

                    if response.status_code == 200:
                        data = response.json()
                        meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
                        current_price = meta.get('regularMarketPrice', 0)
                        previous_close = meta.get('previousClose', 0)
                        change = current_price - previous_close
                        change_pct = (change / previous_close * 100) if previous_close else 0

                        result['data'][name] = {
                            'price': round(current_price, 2),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2),
                            'previous_close': round(previous_close, 2)
                        }
                        logger.info(f"成功采集{name}: {current_price} ({change_pct:+.2f}%)")
                except Exception as e:
                    logger.warning(f"采集{name}数据失败(Yahoo): {e}")
                    try:
                        if name == 'nasdaq':
                            url2 = "https://hq.sinajs.cn/list=gb_ixic"
                        elif name == 'sp500':
                            url2 = "https://hq.sinajs.cn/list=gb_sp500"
                        else:
                            url2 = "https://hq.sinajs.cn/list=gb_dji"

                        headers = {'Referer': 'https://finance.sina.com.cn'}
                        response2 = requests.get(url2, headers=headers, timeout=10)
                        if response2.status_code == 200:
                            text = response2.text
                            parts = text.split('"')[1].split(',')
                            if len(parts) > 1:
                                price = float(parts[0])
                                prev_close = float(parts[1]) if len(parts) > 4 else price
                                change = price - prev_close
                                change_pct = (change / prev_close * 100) if prev_close else 0
                                result['data'][name] = {
                                    'price': round(price, 2),
                                    'change': round(change, 2),
                                    'change_pct': round(change_pct, 2),
                                    'previous_close': round(prev_close, 2)
                                }
                                logger.info(f"成功采集{name}(Sina): {price}")
                    except Exception as e2:
                        logger.warning(f"采集{name}数据失败(Sina): {e2}")

            if result['data']:
                result['status'] = 'success'
            else:
                result['status'] = 'failed'
                result['error'] = '所有API均失败'

            logger.info(f"美股指数数据采集完成: {list(result['data'].keys())}")

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"美股指数采集异常: {e}")

        return result

    def fetch_asia_index_data(self) -> dict:
        """采集亚洲股指数据"""
        result = {
            'name': '亚洲股指',
            'data': {},
            'status': 'pending'
        }

        try:
            import requests

            indices = {
                'nikkei': ('jpn225', 'NII225'),
                'hang_seng': ('hkHSI', 'HSI'),
                'kospi': ('kr200', 'KS200')
            }

            for name, (sina_code, yahoo_sym) in indices.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}"
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
                        current_price = meta.get('regularMarketPrice', 0)
                        previous_close = meta.get('previousClose', 0)
                        change = current_price - previous_close
                        change_pct = (change / previous_close * 100) if previous_close else 0

                        result['data'][name] = {
                            'price': round(current_price, 2),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2)
                        }
                        logger.info(f"成功采集{name}(Yahoo): {current_price}")
                except Exception as e:
                    logger.warning(f"采集{name}失败(Yahoo): {e}")

                    try:
                        sina_url = f"https://hq.sinajs.cn/list={sina_code}"
                        headers = {'Referer': 'https://finance.sina.com.cn'}
                        resp = requests.get(sina_url, headers=headers, timeout=10)
                        if resp.status_code == 200:
                            text = resp.text
                            if '"' in text:
                                parts = text.split('"')[1].split(',')
                                if len(parts) > 1:
                                    price = float(parts[1]) if len(parts) > 1 else 0
                                    prev_close = float(parts[3]) if len(parts) > 3 else price
                                    change = price - prev_close
                                    change_pct = (change / prev_close * 100) if prev_close else 0
                                    result['data'][name] = {
                                        'price': round(price, 2),
                                        'change': round(change, 2),
                                        'change_pct': round(change_pct, 2)
                                    }
                                    logger.info(f"成功采集{name}(Sina): {price}")
                    except Exception as e2:
                        logger.warning(f"采集{name}失败(Sina): {e2}")

            if result['data']:
                result['status'] = 'success'
                logger.info(f"亚洲股指采集成功: {list(result['data'].keys())}")
            else:
                result['status'] = 'failed'
                result['error'] = '所有API均失败'
                logger.warning("亚洲股指: 所有API均失败")

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"亚洲股指采集异常: {e}")

        return result

    def fetch_us_futures(self) -> dict:
        """采集美股期货数据"""
        result = {
            'name': '美股期货',
            'data': {},
            'status': 'pending'
        }

        try:
            import requests

            futures = {
                'es': 'ES=F',
                'nq': 'NQ=F',
                'ym': 'YM=F'
            }

            for name, symbol in futures.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
                        current_price = meta.get('regularMarketPrice', 0)
                        previous_close = meta.get('previousClose', 0)

                        result['data'][name] = {
                            'price': round(current_price, 2),
                            'previous_close': round(previous_close, 2) if previous_close else 0
                        }
                        logger.info(f"成功采集{name}期货: {current_price}")
                except Exception as e:
                    logger.warning(f"采集{name}期货失败: {e}")

            if result['data']:
                result['status'] = 'success'
                logger.info(f"美股期货采集成功: {list(result['data'].keys())}")
            else:
                result['status'] = 'failed'
                result['error'] = '所有API均失败'
                logger.warning("美股期货: 所有API均失败")

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"美股期货采集异常: {e}")

        return result

    def check_ah_discount(self) -> dict:
        """检查AH溢价情况"""
        result = {
            'name': 'AH溢价',
            'status': 'pending',
            'data': {}
        }

        try:
            import requests

            url = "https://query1.finance.yahoo.com/v8/finance/chart/CNHAM"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
                current_price = meta.get('regularMarketPrice', 0)

                result['data'] = {
                    'ah_premium': round(current_price, 2) if current_price else 0
                }
                result['status'] = 'success'
                logger.info(f"AH溢价: {current_price}")

        except Exception as e:
            result['status'] = 'failed'
            logger.warning(f"AH溢价数据采集失败: {e}")

        return result

    def update_yesterday_picks_tracking(self) -> dict:
        """更新昨日推荐的跟踪数据"""
        result = {
            'name': '推荐跟踪',
            'status': 'pending'
        }

        try:
            tracking_script = project_root / "scripts" / "update_tracking.py"
            if tracking_script.exists():
                try:
                    process = subprocess.run(
                        [sys.executable, str(tracking_script)],
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    if process.returncode == 0:
                        result['status'] = 'success'
                        logger.info("昨日推荐跟踪更新成功")
                    else:
                        error_output = process.stderr or process.stdout
                        if 'mysql' in error_output.lower() or 'connection' in error_output.lower():
                            result['status'] = 'skipped'
                            result['note'] = 'MySQL不可用，跳过跟踪更新'
                            logger.warning(f"MySQL连接失败，跳过跟踪: {error_output[:200]}")
                        else:
                            result['status'] = 'failed'
                            result['error'] = error_output[:500]
                            logger.warning(f"昨日推荐跟踪更新失败: {error_output[:200]}")
                except subprocess.TimeoutExpired:
                    result['status'] = 'skipped'
                    result['note'] = '跟踪脚本执行超时'
                    logger.warning("昨日推荐跟踪更新超时")
            else:
                result['status'] = 'skipped'
                result['note'] = '跟踪脚本不存在'

        except Exception as e:
            result['status'] = 'skipped'
            result['note'] = f'跟踪更新异常: {str(e)[:100]}'
            logger.error(f"更新跟踪数据异常: {e}")

        return result

    def generate_morning_summary(self, morning_data: dict) -> str:
        """生成晨间数据摘要"""
        lines = []
        lines.append("=" * 60)
        lines.append("【晨间数据摘要】")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 60)

        has_any_data = False

        us_index = morning_data.get('us_index', {})
        if us_index.get('status') == 'success' and us_index.get('data'):
            has_any_data = True
            lines.append("\n📈 美股指数（隔夜）：")
            for name, data in us_index.get('data', {}).items():
                change_pct = data.get('change_pct', 0)
                change_sign = '+' if change_pct > 0 else ''
                color = '🔴' if change_pct > 0 else '🟢' if change_pct < 0 else '⚪'
                lines.append(f"  {color} {name.upper()}: {data.get('price', 'N/A')} ({change_sign}{change_pct}%)")
        elif us_index.get('status') == 'failed':
            lines.append("\n⚠️ 美股指数获取失败")
            error = us_index.get('error', '未知错误')
            lines.append(f"  原因: {error}")

        asia_index = morning_data.get('asia_index', {})
        if asia_index.get('status') == 'success' and asia_index.get('data'):
            has_any_data = True
            lines.append("\n📈 亚洲股指：")
            for name, data in asia_index.get('data', {}).items():
                change_pct = data.get('change_pct', 0)
                change_sign = '+' if change_pct > 0 else ''
                lines.append(f"  {name}: {data.get('price', 'N/A')} ({change_sign}{change_pct}%)")
        elif asia_index.get('status') == 'failed':
            lines.append("\n⚠️ 亚洲股指获取失败")
            error = asia_index.get('error', '未知错误')
            lines.append(f"  原因: {error}")

        futures = morning_data.get('us_futures', {})
        if futures.get('status') == 'success' and futures.get('data'):
            has_any_data = True
            lines.append("\n📊 美股期货：")
            for name, data in futures.get('data', {}).items():
                lines.append(f"  {name.upper()}: {data.get('price', 'N/A')}")
        elif futures.get('status') == 'failed':
            lines.append("\n⚠️ 美股期货获取失败")

        ah_premium = morning_data.get('ah_premium', {})
        if ah_premium.get('status') == 'success' and ah_premium.get('data'):
            has_any_data = True
            premium = ah_premium.get('data', {}).get('ah_premium', 0)
            lines.append(f"\n💱 AH溢价: {premium:.2f}%")
            if premium > 130:
                lines.append("  ⚠️ AH溢价较高，警惕大盘回调风险")
            elif premium < 120:
                lines.append("  ✓ AH溢价合理，暂无明显风险")

        tracking = morning_data.get('tracking', {})
        if tracking.get('status') == 'success':
            lines.append("\n✓ 昨日推荐跟踪数据已更新")
        elif tracking.get('status') == 'skipped':
            lines.append("\n⚠️ 昨日推荐跟踪已跳过")
        elif tracking.get('status') == 'failed':
            lines.append("\n⚠️ 昨日推荐跟踪更新失败")

        if not has_any_data and tracking.get('status') != 'success':
            lines.append("\n📊 外部市场数据暂不可用")
            lines.append("  (可能原因: 网络问题或API不可访问)")
            lines.append("  → A股交易将正常进行，不受影响")

        return "\n".join(lines)

    def update_all(self) -> dict:
        """更新所有晨间数据"""
        logger.info("开始更新晨间数据...")

        morning_data = {
            'timestamp': datetime.now().isoformat(),
            'date': self.today,
            'us_index': self.fetch_us_index_data(),
            'asia_index': self.fetch_asia_index_data(),
            'us_futures': self.fetch_us_futures(),
            'ah_premium': self.check_ah_discount(),
            'tracking': self.update_yesterday_picks_tracking()
        }

        with open(self.morning_data_path, 'w', encoding='utf-8') as f:
            json.dump(morning_data, f, ensure_ascii=False, indent=2)

        logger.info(f"晨间数据已保存: {self.morning_data_path}")

        return morning_data


def main():
    logger.info("=" * 60)
    logger.info("开始晨间数据更新")
    logger.info("=" * 60)

    updater = MorningDataUpdater()
    morning_data = updater.update_all()

    summary = updater.generate_morning_summary(morning_data)
    print(summary)

    logger.info("=" * 60)
    logger.info("晨间数据更新完成")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
