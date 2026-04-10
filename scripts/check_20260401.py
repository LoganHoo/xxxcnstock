import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
count_20260401 = 0
files_with_20260401 = []

for f in kline_dir.glob('*.parquet'):
    try:
        df = pl.read_parquet(f)
        if 'trade_date' in df.columns:
            has_date = df.filter(pl.col('trade_date') == '2026-04-01').height
            if has_date > 0:
                count_20260401 += 1
                files_with_20260401.append((f.name, has_date))
    except:
        pass

print(f'有 2026-04-01 数据的文件: {count_20260401}')
print(f'前10个:')
for name, cnt in files_with_20260401[:10]:
    print(f'  {name}: {cnt}条')