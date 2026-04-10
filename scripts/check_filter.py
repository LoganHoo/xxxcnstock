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

# 检查过滤问题
latest_date = data["trade_date"].max()
print(f'latest_date: {latest_date}')

filtered = data.filter(pl.col("trade_date") < latest_date)
print(f'Filtered rows: {len(filtered)}')

unique_dates = filtered["trade_date"].unique().sort(descending=True)
print(f'Unique dates in filtered data (top 10):')
print(unique_dates.head(10))

# 检查 2026-03-31 是否在过滤结果中
has_20260331 = filtered.filter(pl.col("trade_date") == "2026-03-31").height
print(f'\n2026-03-31 rows in filtered: {has_20260331}')

# 直接检查
has_20260331_direct = data.filter(pl.col("trade_date") == "2026-03-31").height
print(f'2026-03-31 rows in original: {has_20260331_direct}')