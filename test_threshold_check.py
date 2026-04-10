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

# Do the exact same aggregation as calculate_market_sentiment
daily_data = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
    pl.mean("factor_limit_up_score").alias("avg_limit_up_score"),
    pl.mean("factor_pioneer_status").alias("avg_pioneer_status"),
    pl.mean("factor_cost_peak").alias("avg_cost_peak"),
    pl.mean("close").alias("avg_close"),
    pl.mean("open").alias("avg_open"),
    pl.count().alias("total_stocks")
])

# Check the raw aggregated data for last 3 dates
daily_sorted = daily_data.sort("trade_date")
last3 = daily_sorted.tail(3)
print("Raw aggregated data (last 3 dates):")
for i in range(3):
    row = last3.row(i)
    print(f"  Date: {row[0]}, avg_v_total: {row[1]}, avg_limit_up_score: {row[2]}, avg_pioneer_status: {row[3]}")

# Now apply determine_market_state manually
thresholds = {
    'strong_v_total': 1800,
    'oscillating_v_total_min': 1200,
    'oscillating_v_total_max': 2000,
    'sentiment_temperature_strong': 50,
    'sentiment_temperature_overheat': 80,
    'cost_peak_support': 0.995
}

avg_cost_peak_mean = daily_sorted["avg_cost_peak"].mean()

# Manually check what market_state would be for last row
last_row = last3.row(2)
v_total = last_row[1]
print(f"\nLast row analysis:")
print(f"  v_total = {v_total}")
print(f"  strong_v_total threshold = {thresholds['strong_v_total']}")
print(f"  v_total > threshold? {v_total > thresholds['strong_v_total']}")