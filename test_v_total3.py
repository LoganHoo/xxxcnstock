#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from pathlib import Path

# Load ALL data
kline_dir = Path("data/kline")
parquet_files = list(kline_dir.glob("*.parquet"))
dfs = []
for f in parquet_files:
    try:
        df = pl.read_parquet(f)
        dfs.append(df)
    except:
        pass
data = pl.concat(dfs)

print(f"Total data shape: {data.shape}")

# Calculate v_total factor
factor_engine = FactorEngine()
data = factor_engine.calculate_factor(data, "v_total")

# Get latest date data
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)

print(f"Latest date: {latest_date}")
print(f"Rows for latest date: {len(latest)}")
print(f"factor_v_total unique count: {latest['factor_v_total'].n_unique()}")
print(f"factor_v_total first value: {latest['factor_v_total'][0]}")
print(f"factor_v_total in 万亿: {latest['factor_v_total'][0] / 10000}")

# Check all data grouped by date
market_avg = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
]).sort("trade_date")

print(f"\nmarket_avg last 5 rows:")
print(market_avg.tail(5))