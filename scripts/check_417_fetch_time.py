#!/usr/bin/env python3
"""
检查 2026-04-17 数据的采集时间
"""
import sys
sys.path.insert(0, '.')

import polars as pl
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')

# 检查 4月17日 数据的采集时间
codes = ['000001', '600519', '002119', '002219', '000002']

print('=' * 70)
print('检查 2026-04-17 数据的实际采集时间')
print('=' * 70)
print(f'今天: 2026-04-18 (周六)')
print('=' * 70)

for code in codes:
    try:
        df = pl.read_parquet(KLINE_DIR / f'{code}.parquet')
        row = df.filter(pl.col('trade_date').cast(str) == '2026-04-17')
        if not row.is_empty():
            fetch_time_series = row[0]['fetch_time']
            fetch_time_str = fetch_time_series[0] if len(fetch_time_series) > 0 else None
            if fetch_time_str:
                print(f'{code}: {fetch_time_str}')
    except Exception as e:
        print(f'{code}: 错误 - {e}')

print('=' * 70)
print('分析:')
print('')
print('情况1: 如果采集时间是 2026-04-17 15:xx:xx 之后')
print('       → 周五收盘后采集，✅ 合规')
print('')
print('情况2: 如果采集时间是 2026-04-18 09:xx:xx')
print('       → 周六早上采集（非交易日），✅ 合规')
print('       因为周六不是交易日，采集的是昨天(4/17)的历史数据')
print('')
print('情况3: 如果采集时间是 2026-04-17 09:30-15:00 之间')
print('       → 周五盘中采集，❌ 违规！')
print('=' * 70)
