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

# Method 1: like data_audit.py (original broken version)
method1 = data.filter(pl.col("trade_date") < latest_date).sort("trade_date", descending=True)["trade_date"].unique()
print(f'Method 1 (sort on DF then unique on Series):')
print(f'  First element: {method1[0]}')

# Method 2: like check_filter.py (working version)
method2 = data.filter(pl.col("trade_date") < latest_date)["trade_date"].unique().sort(descending=True)
print(f'\nMethod 2 (unique on Series then sort):')
print(f'  First element: {method2[0]}')