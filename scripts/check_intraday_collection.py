#!/usr/bin/env python3
"""
检查盘中采集违规情况
"""
import sys
sys.path.insert(0, '.')

import polars as pl
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')
TARGET_DATE = "2026-04-17"


def main():
    print("=" * 70)
    print(f"检查 {TARGET_DATE} 数据采集时间")
    print("=" * 70)
    
    all_files = list(KLINE_DIR.glob("*.parquet"))
    total = len(all_files)
    intraday_count = 0
    after_close_count = 0
    no_time_count = 0
    
    intraday_samples = []
    
    for f in all_files:
        try:
            df = pl.read_parquet(f)
            row = df.filter(pl.col('trade_date').cast(str) == TARGET_DATE)
            if not row.is_empty():
                fetch_time = row[0]['fetch_time']
                if fetch_time:
                    # 提取时间部分
                    time_part = fetch_time.split()[1] if ' ' in str(fetch_time) else ''
                    hour = int(time_part.split(':')[0]) if time_part else 0
                    
                    # 判断是否在盘中 (9:30 - 15:00)
                    if 9 <= hour < 15:
                        intraday_count += 1
                        if len(intraday_samples) < 5:
                            code = f.stem
                            intraday_samples.append((code, str(fetch_time)))
                    elif hour >= 15:
                        after_close_count += 1
                else:
                    no_time_count += 1
        except:
            pass
    
    print(f"\n总股票数: {total}")
    print(f"盘中采集 (9:30-15:00): {intraday_count}")
    print(f"收盘后采集 (15:00+): {after_close_count}")
    print(f"无采集时间: {no_time_count}")
    
    print("\n" + "=" * 70)
    if intraday_count > 0:
        print("⚠️  警告: 存在盘中采集的数据！")
        print("\n盘中采集样本:")
        for code, fetch_time in intraday_samples:
            print(f"  {code}: {fetch_time}")
    else:
        print("✅ 所有数据都是收盘后采集")
    print("=" * 70)


if __name__ == "__main__":
    main()
