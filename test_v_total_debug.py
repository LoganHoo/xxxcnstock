#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from pathlib import Path
import yaml

# Load config
with open("config/strategies/fund_behavior_config.yaml", "r") as f:
    config = yaml.safe_load(f)

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
factors = ["v_total", "cost_peak", "limit_up_score", "pioneer_status", "ma5_bias", "v_ratio10"]
data = factor_engine.calculate_all_factors(data, factors)

# Check v_total directly from factor
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)
v_total_from_factor = latest['factor_v_total'][0]
print(f"v_total from factor (latest): {v_total_from_factor}")

# Simulate what fund_behavior_strategy does
market_avg = data.group_by("trade_date").agg([
    pl.mean("factor_v_ratio10").alias("avg_v_ratio10"),
    pl.mean("factor_v_total").alias("avg_v_total"),
    pl.mean("close").alias("avg_close")
]).sort("trade_date")

last_row_idx = len(market_avg) - 1
v_total_from_avg = market_avg["avg_v_total"][last_row_idx]
print(f"v_total from group_by mean: {v_total_from_avg}")

# Check config threshold
hedge_config = config.get('indicators', {}).get('hedge', {})
v_total_threshold = hedge_config.get('v_total_threshold', 1800)
print(f"v_total_threshold from config: {v_total_threshold}")

# Check what strategy engine would see
print(f"\nIn strategy engine:")
print(f"  v_total (avg_v_total[-1]): {v_total_from_avg}")
print(f"  v_total >= threshold ({v_total_threshold})? {v_total_from_avg >= v_total_threshold}")