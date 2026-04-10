import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))[:100]

valid_count = 0
limit_up_count = 0
limit_down_count = 0

for f in files:
    try:
        df = pl.read_parquet(f)
        if len(df) >= 2:
            valid_count += 1
            latest = df.sort('trade_date', descending=True).row(0)
            prev = df.sort('trade_date', descending=True).row(1)

            code = latest[0]
            limit_rate = 0.1
            if code.startswith('688') or code.startswith('300'):
                limit_rate = 0.2
            elif code.startswith('8') or code.startswith('4'):
                limit_rate = 0.3

            if latest[2] >= prev[2] * (1 + limit_rate):
                limit_up_count += 1
            elif latest[2] <= prev[2] * (1 - limit_rate):
                limit_down_count += 1
    except Exception as e:
        print(f'Error: {e}')

print(f'有效股票: {valid_count}')
print(f'涨停: {limit_up_count}, 跌停: {limit_down_count}')