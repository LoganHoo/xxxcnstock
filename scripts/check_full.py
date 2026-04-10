import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = list(kline_dir.glob('*.parquet'))

count_20260401 = 0
count_other = 0

for f in files:
    try:
        df = pl.read_parquet(f)
        if 'trade_date' in df.columns:
            cnt = df.filter(pl.col('trade_date') == '2026-04-01').height
            if cnt > 0:
                count_20260401 += 1
            else:
                count_other += 1
    except:
        pass

print(f'总文件数: {len(files)}')
print(f'有 2026-04-01 数据的文件: {count_20260401}')
print(f'无 2026-04-01 数据的文件: {count_other}')