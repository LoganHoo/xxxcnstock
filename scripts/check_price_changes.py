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
prev_date = data.filter(pl.col("trade_date") < latest_date).sort("trade_date", descending=True)["trade_date"].unique().first()

print(f'最新日期: {latest_date}')
print(f'前一日期: {prev_date}')

latest_data = data.filter(pl.col("trade_date") == latest_date)
prev_data = data.filter(pl.col("trade_date") == prev_date).select(["code", "close"]).rename({"close": "prev_close"})

merged = latest_data.join(prev_data, on="code", how="left")
merged = merged.filter(pl.col("prev_close").is_not_null())

merged = merged.with_columns([
    (((pl.col('close') - pl.col('prev_close')) / pl.col('prev_close') * 100)).alias('pct_change')
])

# 统计涨跌幅分布
print(f'\n有效对比股票数: {len(merged)}')
print(f'\n涨跌幅分布:')
print(f'  >= 9%: {merged.filter(pl.col("pct_change") >= 9).height}')
print(f'  >= 5%: {merged.filter(pl.col("pct_change") >= 5).height}')
print(f'  >= 0%: {merged.filter(pl.col("pct_change") >= 0).height}')
print(f'  < 0%: {merged.filter(pl.col("pct_change") < 0).height}')
print(f'  <= -5%: {merged.filter(pl.col("pct_change") <= -5).height}')
print(f'  <= -9%: {merged.filter(pl.col("pct_change") <= -9).height}')

# 找出涨停的
limit_10 = merged.filter(pl.col("pct_change") >= 9.5)
print(f'\n接近10%涨停: {limit_10.height}')
if limit_10.height > 0:
    print(limit_10.select(["code", "close", "prev_close", "pct_change"]).head())