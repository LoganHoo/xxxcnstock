"""数据采集任务 - 16:00执行（收盘后）"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import polars as pl
from pathlib import Path
from datetime import datetime, time
import time as time_module
import re
import json
import requests
import argparse

from core.trading_calendar import check_market_status


class DataCollector:
    """数据采集器"""

    def __init__(self, force_date=None, retry_mode=False):
        self.project_root = Path(__file__).parent.parent.parent
        self.kline_dir = self.project_root / "data" / "kline"
        self.force_date = force_date
        self.retry_mode = retry_mode
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def run(self):
        """执行采集"""
        self.logger.info("开始采集K线数据...")

        if self.force_date:
            self.logger.info(f"强制重新采集日期: {self.force_date}")
        elif not self._check_market_closed():
            return

        if not self.retry_mode:
            self._create_lock()

        if self.retry_mode:
            self.logger.info("断点续传模式：只采集缺失最新日期数据的股票")
            if self._check_retry_conflict():
                self.logger.error("❌ 检测到与初始采集冲突，退出")
                return

        codes = self._get_stock_codes()
        
        if self.retry_mode:
            codes = self._filter_missing_codes(codes)
            self.logger.info(f"断点续传：需要补充的股票数量: {len(codes)}")
        
        self.logger.info(f"共 {len(codes)} 只股票")

        success = 0
        failed = 0
        skipped = 0
        updated = 0

        for i, code in enumerate(codes):
            if (i + 1) % 100 == 0:
                self.logger.info(f"  进度: {i+1}/{len(codes)}")

            result = self._fetch_single(code)
            if result == 'success':
                success += 1
            elif result == 'updated':
                updated += 1
            elif result == 'skipped':
                skipped += 1
            else:
                failed += 1

            time_module.sleep(0.1)

        self.logger.info(f"采集完成: 新增 {success}, 更新 {updated}, 已是最新 {skipped}, 失败 {failed}")

    def _check_market_closed(self) -> bool:
        """检查市场是否已收盘

        Returns:
            bool: 市场已收盘返回True，否则返回False
        """
        market_status = check_market_status()

        if not market_status['is_trading_day']:
            self.logger.info(f"非交易日（周末或节假日），跳过数据采集")
            return False

        if market_status['is_after_market_close']:
            return True

        current_time = datetime.now().time()
        if current_time >= time(15, 30):
            return True

        self.logger.info(f"交易日盘中（{datetime.now().strftime('%H:%M')}），需等待收盘后采集")
        return False

    def _create_lock(self):
        """创建锁文件"""
        lock_file = self.project_root / "data" / ".data_fetch.lock"
        with open(lock_file, 'w') as f:
            json.dump({
                'pid': os.getpid(),
                'start_time': datetime.now().isoformat()
            }, f)
        self.logger.info(f"锁文件已创建: {lock_file}")

    def _check_retry_conflict(self) -> bool:
        """检查是否与初始采集冲突

        Returns:
            bool: 存在冲突返回True，否则返回False
        """
        lock_file = self.project_root / "data" / ".data_fetch.lock"

        if not lock_file.exists():
            return False

        try:
            with open(lock_file, 'r') as f:
                data = json.load(f)
                pid = data.get('pid')
                start_time = data.get('start_time')

            import psutil
            if pid and psutil.pid_exists(pid):
                self.logger.warning(f"⚠️ 检测到初始采集仍在运行 (PID: {pid})，重试任务退出")
                return True

            self.logger.info("发现残留锁文件，清理后继续")
            lock_file.unlink()
        except:
            if lock_file.exists():
                lock_file.unlink()

        return False

    def _get_stock_codes(self) -> list:
        codes = []
        for f in sorted(self.kline_dir.glob("*.parquet")):
            codes.append(f.stem)
        return codes

    def _filter_missing_codes(self, all_codes: list) -> list:
        """过滤缺失最新日期数据的股票"""
        missing_codes = []
        
        latest_date = self._get_latest_trade_date()
        if not latest_date:
            self.logger.warning("无法获取最新交易日期，返回全部股票")
            return all_codes
        
        self.logger.info(f"检查缺失最新日期 {latest_date} 数据的股票...")
        
        for code in all_codes:
            parquet_file = self.kline_dir / f"{code}.parquet"
            if not parquet_file.exists():
                missing_codes.append(code)
                continue
            
            try:
                df = pl.read_parquet(parquet_file)
                dates = df["trade_date"].to_list()
                if latest_date not in dates:
                    missing_codes.append(code)
            except:
                missing_codes.append(code)
        
        return missing_codes
    
    def _get_latest_trade_date(self) -> str:
        """获取最新交易日期"""
        from datetime import datetime, timedelta
        
        today = datetime.now()
        
        for i in range(7):
            check_date = today - timedelta(days=i)
            weekday = check_date.weekday()
            
            if weekday < 5:
                return check_date.strftime("%Y-%m-%d")
        
        return None

    def _fetch_single(self, code: str) -> str:
        if code.startswith('6'):
            symbol = f'sh{code}'
        else:
            symbol = f'sz{code}'

        url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
        params = {
            '_var': f'kline_dayqfq_{symbol}',
            'param': f'{symbol},day,,,10,qfq',
            'r': str(int(time_module.time() * 1000))
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/'
        }

        try:
            r = requests.get(url, params=params, headers=headers, timeout=15)
            text = r.text
            match = re.match(r'kline_dayqfq_\w+=(.*)', text)
            if match:
                data = json.loads(match.group(1))
                if data.get('code') == 0:
                    klines = data['data'][symbol].get('qfqday', [])
                    if klines:
                        return self._update_parquet(code, klines)
        except Exception as e:
            self.logger.debug(f"获取 {code} K线失败: {e}")
        return 'failed'

    def _update_parquet(self, code: str, klines: list) -> str:
        parquet_file = self.kline_dir / f"{code}.parquet"

        records = []
        for k in klines:
            records.append({
                'code': code,
                'trade_date': k[0],
                'open': float(k[1]),
                'close': float(k[2]),
                'high': float(k[3]),
                'low': float(k[4]),
                'volume': int(float(k[5])),
            })

        new_df = pl.DataFrame(records)

        if parquet_file.exists():
            try:
                existing = pl.read_parquet(parquet_file)
                existing_dates = set(existing["trade_date"].to_list())

                if self.force_date:
                    if self.force_date in existing_dates:
                        existing_filtered = existing.filter(pl.col("trade_date") != self.force_date)
                        merged = pl.concat([existing_filtered, new_df], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        return 'updated'
                    else:
                        merged = pl.concat([existing, new_df], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        return 'success'
                else:
                    new_dates = set(new_df["trade_date"].to_list())
                    already_has = new_dates.intersection(existing_dates)

                    if len(already_has) == len(new_dates):
                        return 'skipped'

                    new_to_add = new_df.filter(~pl.col("trade_date").is_in(existing_dates))
                    if len(new_to_add) > 0:
                        merged = pl.concat([existing, new_to_add], how="diagonal")
                        merged = merged.sort("trade_date")
                        merged.write_parquet(parquet_file)
                        return 'updated'
                    return 'skipped'
            except Exception as e:
                self.logger.warning(f"更新 {code} Parquet失败: {e}")
                if parquet_file.exists():
                    try:
                        os.remove(parquet_file)
                        self.logger.info(f"已删除损坏文件: {parquet_file}")
                    except:
                        pass
                return 'failed'
        else:
            try:
                new_df.write_parquet(parquet_file)
                return 'success'
            except Exception as e:
                self.logger.warning(f"创建 {code} Parquet失败: {e}")
                if parquet_file.exists():
                    try:
                        os.remove(parquet_file)
                        self.logger.info(f"已删除损坏文件: {parquet_file}")
                    except:
                        pass
                return 'failed'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='数据采集')
    parser.add_argument('--date', type=str, default=None, help='强制重新采集指定日期 (YYYY-MM-DD)')
    parser.add_argument('--retry', action='store_true', help='断点续传模式：只采集缺失的股票')
    args = parser.parse_args()

    collector = DataCollector(force_date=args.date)
    collector.retry_mode = args.retry
    collector.run()