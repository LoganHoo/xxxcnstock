import polars as pl
from pathlib import Path

kline_dir = Path('/app/data/kline')
files = sorted(kline_dir.glob('*.parquet'))[:500]

total_stocks = 0
has_20260401 = 0

for f in files:
    try:
        df = pl.read_parquet(f)
        if 'trade_date' in df.columns:
            latest = df['trade_date'].max()
            count = df.filter(pl.col('trade_date') == latest).height
            total_stocks += count
            if latest == '2026-04-01':
                has_20260401 += 1
    except:
        pass

print(f'前500个文件统计:')
print(f'  最新日期为2026-04-01的文件: {has_20260401}')
print(f'  总股票数(按最新日期): {total_stocks}')