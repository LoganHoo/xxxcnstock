#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
from core.fund_behavior_strategy import FundBehaviorStrategyEngine
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

print(f"Total data: {data.shape}")

# Calculate factors
factor_engine = FactorEngine()
factors = ["v_total", "cost_peak", "limit_up_score", "pioneer_status", "ma5_bias", "v_ratio10"]
data = factor_engine.calculate_all_factors(data, factors)

# Check v_total
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)
print(f"Latest date: {latest_date}, v_total: {latest['factor_v_total'][0]}")

# Calculate indicators
indicator = FundBehaviorIndicatorEngine()
indicators = indicator.calculate_all_indicators(data)

# Execute strategy
strategy = FundBehaviorStrategyEngine()
result = strategy.execute_strategy(data, 1000000, "10:00")

print(f"\nResult:")
print(f"  v_total: {result.get('v_total')}")
print(f"  hedge_effect: {result.get('hedge_effect')}")
print(f"  is_strong_region: {result.get('is_strong_region')}")