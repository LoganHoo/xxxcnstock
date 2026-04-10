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

# Call calculate_market_sentiment directly
indicator = FundBehaviorIndicatorEngine()
result = indicator.calculate_market_sentiment(data)

# Check result
v_total_list = result.get("avg_v_total", [])
print(f"avg_v_total length: {len(v_total_list)}")
print(f"avg_v_total last 3: {v_total_list[-3:]}")

# Also check calculate_all_indicators
data2 = factor_engine.calculate_all_factors(data, factors)
result2 = indicator.calculate_all_indicators(data2)
v_total_list2 = result2.get("market_sentiment", {}).get("avg_v_total", [])
print(f"\ncalculate_all_indicators avg_v_total last 3: {v_total_list2[-3:]}")