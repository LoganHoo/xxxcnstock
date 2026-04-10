"""数据采集任务 - 16:00执行（收盘后）"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import polars as pl
from pathlib import Path
from datetime import datetime
from .base_task import BaseTask


class DataCollectTask(BaseTask):
    """数据采集任务"""

    name = "data_collect"
    description = "采集实时行情和K线数据"

    def run(self) -> bool:
        try:
            self._fetch_klines()
            self.logger.info("数据采集完成")
            return True
        except Exception as e:
            self.logger.error(f"数据采集失败: {e}")
            return False

    def _fetch_klines(self):
        """增量更新K线数据"""
        import time
        import re
        import json
        import requests

        project_root = Path(__file__).parent.parent.parent
        kline_dir = project_root / "data" / "kline"

        codes = self._get_stock_codes(kline_dir)
        self.logger.info(f"开始采集K线数据，共 {len(codes)} 只股票...")

        success = 0
        failed = 0
        skipped = 0

        for i, code in enumerate(codes):
            if (i + 1) % 100 == 0:
                self.logger.info(f"  进度: {i+1}/{len(codes)}")

            if code.startswith('6'):
                symbol = f'sh{code}'
            else:
                symbol = f'sz{code}'

            url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
            params = {
                '_var': f'kline_dayqfq_{symbol}',
                'param': f'{symbol},day,,,3,qfq',
                'r': str(int(time.time() * 1000))
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
                            latest = klines[-1]
                            new_row = {
                                'code': code,
                                'trade_date': latest[0],
                                'open': float(latest[1]),
                                'close': float(latest[2]),
                                'high': float(latest[3]),
                                'low': float(latest[4]),
                                'volume': int(float(latest[5])),
                            }

                            was_updated = self._update_parquet_file(code, new_row, kline_dir)
                            if was_updated:
                                success += 1
                            else:
                                skipped += 1
                    else:
                        failed += 1
                else:
                    failed += 1
            except Exception as e:
                self.logger.debug(f"获取 {code} K线失败: {e}")
                failed += 1

            time.sleep(0.1)

        self.logger.info(f"K线采集完成: 新增/更新 {success}, 已是最新 {skipped}, 失败 {failed}")

    def _get_stock_codes(self, kline_dir: Path) -> list:
        """获取股票代码列表"""
        codes = []
        for f in sorted(kline_dir.glob("*.parquet")):
            code = f.stem
            codes.append(code)
        return codes

    def _update_parquet_file(self, code: str, new_row: dict, kline_dir: Path) -> bool:
        """更新单个股票的Parquet文件"""
        parquet_file = kline_dir / f"{code}.parquet"

        if parquet_file.exists():
            try:
                existing = pl.read_parquet(parquet_file)
                existing_dates = set(existing["trade_date"].to_list())

                if new_row["trade_date"] in existing_dates:
                    return False

                new_df = pl.DataFrame([new_row])
                merged = pl.concat([existing, new_df], how="diagonal")
                merged = merged.sort("trade_date")
                merged.write_parquet(parquet_file)
                return True
            except Exception as e:
                self.logger.warning(f"更新 {code} Parquet失败: {e}")
                return False
        else:
            try:
                new_df = pl.DataFrame([new_row])
                new_df.write_parquet(parquet_file)
                return True
            except Exception as e:
                self.logger.warning(f"创建 {code} Parquet失败: {e}")
                return False
