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

# Calculate factors
factor_engine = FactorEngine()
data = factor_engine.calculate_factor(data, "v_total")

# Check v_total by date
daily = data.group_by("trade_date").agg([
    pl.col("factor_v_total").mean().alias("mean_v_total"),
    pl.col("factor_v_total").first().alias("first_v_total"),
    pl.count().alias("stock_count")
]).sort("trade_date")

print("Last 5 days:")
print(daily.tail(5))