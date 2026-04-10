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

# Calculate factors
factor_engine = FactorEngine()
factors = ["v_total", "cost_peak", "limit_up_score", "pioneer_status", "ma5_bias", "v_ratio10"]
data = factor_engine.calculate_all_factors(data, factors)

# Check v_total from factor
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)
print(f"[1] Latest date: {latest_date}")
print(f"[2] v_total from factor (latest row): {latest['factor_v_total'][0]}")

# Calculate indicators
indicator = FundBehaviorIndicatorEngine()
indicators = indicator.calculate_all_indicators(data)

# Check v_total from indicators
v_total_list = indicators["market_sentiment"].get("avg_v_total", [])
print(f"[3] v_total_list length: {len(v_total_list)}")
print(f"[4] v_total_list last value: {v_total_list[-1] if v_total_list else 'empty'}")

# Execute strategy
strategy = FundBehaviorStrategyEngine()
result = strategy.execute_strategy(data, 1000000, "10:00")

# Check v_total from result
v_total_from_result = result.get("v_total", 0.0)
print(f"[5] v_total from result: {v_total_from_result}")

# Compare
print(f"\nComparison:")
print(f"  Factor v_total: {latest['factor_v_total'][0]}")
print(f"  Indicator avg_v_total[-1]: {v_total_list[-1] if v_total_list else 'N/A'}")
print(f"  Result v_total: {v_total_from_result}")