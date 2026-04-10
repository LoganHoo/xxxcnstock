#!/usr/bin/env python3
import polars as pl
from pathlib import Path

kline_dir = Path('data/kline')
files = list(kline_dir.glob('*.parquet'))

all_data = []
for f in files[:500]:
    df = pl.read_parquet(f)
    if len(df) >= 2:
        all_data.append(df)

data = pl.concat(all_data)

latest_date = '2026-03-31'
prev_date = '2026-03-30'

latest_data = data.filter(pl.col('trade_date') == latest_date)
prev_data = data.filter(pl.col('trade_date') == prev_date).select(['code', 'close']).rename({'close': 'prev_close'})

merged = latest_data.join(prev_data, on='code', how='left')

def get_limit_rate(code):
    code_str = str(code)
    if code_str.startswith('300') or code_str.startswith('688'):
        return 0.20
    elif code_str.startswith('8') or code_str.startswith('4') or code_str.startswith('43'):
        return 0.30
    else:
        return 0.10

merged = merged.with_columns([
    pl.col('code').map_elements(get_limit_rate, return_dtype=pl.Float64).alias('limit_rate')
])

merged = merged.with_columns([
    (pl.col('prev_close').is_not_null()).alias('has_prev_close')
])

merged = merged.with_columns([
    (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100) >= pl.col('limit_rate') * 100).cast(pl.Int64).alias('is_limit_up')
])
merged = merged.with_columns([
    (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100) <= -pl.col('limit_rate') * 100).cast(pl.Int64).alias('is_limit_down')
])

limit_up_count = int(merged['is_limit_up'].sum())
limit_down_count = int(merged['is_limit_down'].sum())
valid_stocks = int(merged['has_prev_close'].sum())

print(f'2026-03-31 涨跌停统计 (采样500文件):')
print(f'  有效股票: {valid_stocks}')
print(f'  涨停: {limit_up_count}')
print(f'  跌停: {limit_down_count}')

limit_ups = merged.filter(pl.col('is_limit_up') == 1)
print(f'\n涨停股票列表 ({len(limit_ups)}只):')
for row in limit_ups.sort('code').iter_rows(named=True):
    pct = (row['close'] - row['prev_close']) / row['prev_close'] * 100
    print(f"  {row['code']}: close={row['close']:.2f}, prev={row['prev_close']:.2f}, pct={pct:.2f}%")