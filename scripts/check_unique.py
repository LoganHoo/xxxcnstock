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
latest_date = data["trade_date"].max()
print(f'latest_date: {latest_date}, type: {type(latest_date)}')

prev_date_candidates = data.filter(pl.col("trade_date") < latest_date).sort("trade_date", descending=True)["trade_date"].unique()
print(f'prev_date_candidates: {prev_date_candidates}')
print(f'len: {len(prev_date_candidates)}')
if len(prev_date_candidates) > 0:
    print(f'first: {prev_date_candidates[0]}, type: {type(prev_date_candidates[0])}')