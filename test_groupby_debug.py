#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from pathlib import Path

# Fresh load
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

# Calculate ALL factors
factor_engine = FactorEngine()
factors = ["v_total", "cost_peak", "limit_up_score", "pioneer_status", "ma5_bias", "v_ratio10"]
data = factor_engine.calculate_all_factors(data, factors)

# Manually replicate calculate_market_sentiment
print("Step 1: Check v_total in latest row")
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)
print(f"  v_total: {latest['factor_v_total'][0]}")

print("\nStep 2: Do group_by")
daily_data = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
])
print(f"  daily_data type: {type(daily_data)}")
print(f"  daily_data shape: {daily_data.shape}")

print("\nStep 3: Check result")
daily_sorted = daily_data.sort("trade_date")
print(f"  Last 3 avg_v_total: {daily_sorted['avg_v_total'][-3:].to_list()}")

# Is there a LazyFrame issue?
print("\nStep 4: Force collect")
if hasattr(daily_data, 'collect'):
    daily_data = daily_data.collect()
    print(f"  After collect: {daily_data['avg_v_total'][-3:].to_list()}")