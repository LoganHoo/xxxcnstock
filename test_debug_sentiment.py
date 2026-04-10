#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
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

# BEFORE calling calculate_market_sentiment, check data
print(f"Data shape BEFORE calculate_market_sentiment: {data.shape}")
latest_date = data["trade_date"].max()
latest_before = data.filter(pl.col("trade_date") == latest_date)
print(f"v_total BEFORE calculate_market_sentiment: {latest_before['factor_v_total'][0]}")

# Call calculate_market_sentiment directly
indicator = FundBehaviorIndicatorEngine()

# Patch calculate_market_sentiment to add debug output
original_func = indicator.calculate_market_sentiment
def patched_market_sentiment(data):
    print(f"Data shape INSIDE calculate_market_sentiment: {data.shape}")
    latest_inside = data.filter(pl.col("trade_date") == data["trade_date"].max())
    print(f"v_total INSIDE calculate_market_sentiment: {latest_inside['factor_v_total'][0]}")
    return original_func(data)

# Call
result = patched_market_sentiment(data)

# Check result
v_total_list = result.get("avg_v_total", [])
print(f"\nResult avg_v_total length: {len(v_total_list)}")
print(f"Result avg_v_total last 3: {v_total_list[-3:]}")