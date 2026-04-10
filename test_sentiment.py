#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
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

# Calculate ALL factors
factor_engine = FactorEngine()
factors = ["v_total", "cost_peak", "limit_up_score", "pioneer_status", "ma5_bias", "v_ratio10"]
data = factor_engine.calculate_all_factors(data, factors)

# Calculate market sentiment indicators only
indicator = FundBehaviorIndicatorEngine()
sentiment_indicators = indicator.calculate_market_sentiment(data)

# Check v_total from market sentiment
v_total_list = sentiment_indicators.get("avg_v_total", [])
print(f"v_total_list length: {len(v_total_list)}")
print(f"v_total_list last 5: {v_total_list[-5:]}")
print(f"v_total_list[-1]: {v_total_list[-1] if v_total_list else 'N/A'}")

# Compare with manual calculation
daily_total = data.group_by("trade_date").agg([
    pl.col("factor_v_total").first().alias("first_v_total")
]).sort("trade_date")
print(f"\nManual calculation (first value per day):")
print(daily_total.tail(5))