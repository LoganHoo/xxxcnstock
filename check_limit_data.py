#!/usr/bin/env python3
import polars as pl
from pathlib import Path

kline_dir = Path('data/kline')
files = list(kline_dir.glob('*.parquet'))

count_with_data = 0
count_no_data = 0
limit_up_stocks = []

for f in files:
    df = pl.read_parquet(f)
    day_data = df.filter(pl.col('trade_date') == '2026-03-31')
    if len(day_data) > 0:
        count_with_data += 1
        prev = df.filter(pl.col('trade_date') < '2026-03-31').sort('trade_date', descending=True)
        if len(prev) > 0:
            prev_close = float(prev[0, 'close'])
            close = float(day_data[0, 'close'])
            if prev_close > 0:
                change = (close - prev_close) / prev_close * 100
                if change >= 9.5:
                    limit_up_stocks.append((f.stem, close, prev_close, change))
    else:
        count_no_data += 1

print(f'K线文件总数: {len(files)}')
print(f'有2026-03-31数据: {count_with_data}')
print(f'无2026-03-31数据: {count_no_data}')
print(f'\n涨停股票(涨幅>=9.5%): {len(limit_up_stocks)}只')
for s in limit_up_stocks[:20]:
    print(f"  {s[0]}: close={s[1]:.2f}, prev={s[2]:.2f}, change={s[3]:+.2f}%")