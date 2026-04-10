import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = sorted(kline_dir.glob('*.parquet'))[:500]

count_20260401 = 0

for f in files:
    try:
        df = pl.read_parquet(f)
        if 'trade_date' in df.columns:
            cnt = df.filter(pl.col('trade_date') == '2026-04-01').height
            if cnt > 0:
                count_20260401 += cnt
    except:
        pass

print(f'前500个文件中 2026-04-01 的记录数: {count_20260401}')