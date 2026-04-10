import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))[:500]

all_data = []
for f in files:
    try:
        df = pl.read_parquet(f)
        if len(df) >= 2:
            all_data.append(df)
    except:
        pass

data = pl.concat(all_data)
print(f'Total rows: {len(data)}')
print(f'Trade dates sample:')
dates = data["trade_date"].unique()
print(f'Total unique dates: {len(dates)}')
print(f'Sorted desc:')
sorted_dates = dates.sort(descending=True)
print(sorted_dates.head(10))

# 找到 2026-04-01 的前一个交易日
latest_date = "2026-04-01"
prev_dates = data.filter(pl.col("trade_date") < latest_date).sort("trade_date", descending=True)["trade_date"].unique()
print(f'\n2026-04-01 之前的日期(前10个):')
print(prev_dates.head(10))