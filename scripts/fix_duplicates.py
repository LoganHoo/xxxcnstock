import polars as pl
from pathlib import Path
import sys

def get_limit_rate(code):
    code = str(code)
    if code.startswith('8') or code.startswith('4') or code.startswith('43'):
        return 0.30
    elif code.startswith('30') or code.startswith('688'):
        return 0.20
    else:
        return 0.10

kline_dir = Path('/app/data/kline')
kline_files = list(kline_dir.glob('*.parquet'))

print("=" * 60)
print("修复数据重复问题")
print("=" * 60)

fixed_count = 0
error_count = 0

for i, f in enumerate(kline_files):
    try:
        df = pl.read_parquet(f)
        original_len = len(df)

        df_unique = df.unique(subset=['trade_date'])

        if len(df_unique) < original_len:
            df_unique = df_unique.sort('trade_date')
            df_unique.write_parquet(f)
            fixed_count += 1
            if fixed_count <= 10:
                print(f"修复: {f.name} ({original_len} -> {len(df_unique)})")
    except Exception as e:
        error_count += 1

print(f"\n修复完成:")
print(f"  修复文件数: {fixed_count}")
print(f"  错误文件数: {error_count}")

print("\n" + "=" * 60)
print("验证 04-01 涨跌停数据")
print("=" * 60)

all_data = []
for f in kline_files:
    try:
        df = pl.read_parquet(f)
        df = df.with_columns([
            pl.col('volume').cast(pl.Float64),
            pl.col('open').cast(pl.Float64),
            pl.col('close').cast(pl.Float64),
            pl.col('high').cast(pl.Float64),
            pl.col('low').cast(pl.Float64),
        ])
        if len(df) >= 2:
            all_data.append(df)
    except:
        pass

combined = pl.concat(all_data)
print(f"总数据量: {len(combined)}")

target_date = '2026-04-01'
day_data = combined.filter(pl.col('trade_date') == target_date)
print(f"{target_date} 数据量: {len(day_data)}")

prev_date = '2026-03-31'
prev_day = combined.filter(pl.col('trade_date') == prev_date)[['code', 'close']].rename({'close': 'prev_close'})
day_data = day_data.join(prev_day, on='code', how='left')

day_data = day_data.with_columns([
    pl.col('code').map_elements(get_limit_rate, return_dtype=pl.Float64).alias('limit_rate')
])
day_data = day_data.with_columns([
    ((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100).alias('pct_change')
])
day_data = day_data.filter(pl.col('prev_close').is_not_null() & (pl.col('prev_close') > 0))

limit_ups = day_data.filter(pl.col('pct_change') >= pl.col('limit_rate') * 100)
limit_downs = day_data.filter(pl.col('pct_change') <= -pl.col('limit_rate') * 100)

print(f"\n✅ 有效数据: {len(day_data)}")
print(f"✅ 涨停家数: {len(limit_ups)}")
print(f"✅ 跌停家数: {len(limit_downs)}")

print(f"\n涨停明细 ({len(limit_ups)}只):")
for row in limit_ups.sort('pct_change', descending=True).iter_rows(named=True):
    print(f"  {row['code']}: {row['pct_change']:.2f}%")

print(f"\n跌停明细 ({len(limit_downs)}只):")
for row in limit_downs.sort('pct_change', descending=True).iter_rows(named=True):
    print(f"  {row['code']}: {row['pct_change']:.2f}%")