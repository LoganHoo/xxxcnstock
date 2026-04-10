#!/usr/bin/env python3
import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

kline_dir = Path('data/kline')
files = list(kline_dir.glob('*.parquet'))

print(f'Total files: {len(files)}')

def process_file(f):
    try:
        df = pl.read_parquet(f)
        if len(df) >= 2:
            dates = df['trade_date'].to_list()
            if '2026-03-31' in dates and '2026-03-30' in dates:
                day_data = df.filter(pl.col('trade_date') == '2026-03-31')
                prev_data = df.filter(pl.col('trade_date') == '2026-03-30')
                if len(day_data) > 0 and len(prev_data) > 0:
                    code = f.stem
                    close = day_data[0, 'close']
                    prev_close = prev_data[0, 'close']

                    code_str = str(code)
                    if code_str.startswith('300') or code_str.startswith('688'):
                        limit_rate = 0.20
                    elif code_str.startswith('8') or code_str.startswith('4') or code_str.startswith('43'):
                        limit_rate = 0.30
                    else:
                        limit_rate = 0.10

                    pct = (close - prev_close) / prev_close * 100
                    is_limit_up = 1 if pct >= limit_rate * 100 else 0
                    is_limit_down = 1 if pct <= -limit_rate * 100 else 0

                    return (code, close, prev_close, pct, is_limit_up, is_limit_down)
    except:
        pass
    return None

start = time.time()
results = []

with ThreadPoolExecutor(max_workers=50) as executor:
    futures = {executor.submit(process_file, f): f for f in files}
    for i, future in enumerate(as_completed(futures)):
        result = future.result()
        if result:
            results.append(result)
        if (i + 1) % 1000 == 0:
            print(f'Progress: {i+1}/{len(files)}')

elapsed = time.time() - start

limit_ups = [r for r in results if r[4] == 1]
limit_downs = [r for r in results if r[5] == 1]

print(f'\n处理完成: {len(results)}只有效股票, 耗时{elapsed:.1f}秒')
print(f'涨停: {len(limit_ups)}只')
print(f'跌停: {len(limit_downs)}只')

print(f'\n涨停股票列表:')
for r in sorted(limit_ups, key=lambda x: -x[3])[:30]:
    print(f"  {r[0]}: close={r[1]:.2f}, prev={r[2]:.2f}, pct={r[3]:+.2f}%")