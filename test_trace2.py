#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
from pathlib import Path

# Load ALL data fresh
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

print(f"Original data shape: {data.shape}")

# Calculate factors
factor_engine = FactorEngine()
data = factor_engine.calculate_factor(data, "v_total")

# Check the data after factor calculation
latest_date = data["trade_date"].max()
print(f"Latest date: {latest_date}")
print(f"Data shape after factor: {data.shape}")

# Filter to latest date
latest = data.filter(pl.col("trade_date") == latest_date)
print(f"Latest date rows: {len(latest)}")
print(f"Latest v_total: {latest['factor_v_total'][0]}")

# Check if there are multiple rows per date
date_counts = data.group_by("trade_date").count()
print(f"\nLast 3 dates row counts:")
print(date_counts.sort("trade_date").tail(3))

# Now calculate indicators
indicator = FundBehaviorIndicatorEngine()
indicators = indicator.calculate_all_indicators(data)

# Check v_total_list
v_total_list = indicators["market_sentiment"].get("avg_v_total", [])
print(f"\nv_total_list length: {len(v_total_list)}")
print(f"v_total_list last 5: {v_total_list[-5:]}")