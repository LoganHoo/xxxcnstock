#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
from core.fund_behavior_indicator import FundBehaviorIndicatorEngine
from pathlib import Path

# Load data
kline_dir = Path("data/kline")
parquet_files = list(kline_dir.glob("*.parquet"))[:100]
dfs = []
for f in parquet_files:
    try:
        df = pl.read_parquet(f)
        dfs.append(df)
    except:
        pass
data = pl.concat(dfs)

# Calculate v_total factor
factor_engine = FactorEngine()
data = factor_engine.calculate_factor(data, "v_total")

# Get latest date data
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)

print(f"Latest date: {latest_date}")
print(f"Rows for latest date: {len(latest)}")
print(f"factor_v_total unique count: {latest['factor_v_total'].n_unique()}")
print(f"factor_v_total first value: {latest['factor_v_total'][0]}")
print(f"factor_v_total mean: {latest['factor_v_total'].mean()}")

# Now calculate using group_by as the strategy does
market_avg = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
    pl.mean("close").alias("avg_close")
]).sort("trade_date")

print(f"\nmarket_avg shape: {market_avg.shape}")
print(f"market_avg last row:")
print(market_avg.tail(1))

# Get the last avg_v_total value
last_row = market_avg.tail(1)
last_v_total = last_row["avg_v_total"][0]
print(f"\nLast avg_v_total: {last_v_total}")
print(f"Last avg_v_total in 万亿: {last_v_total / 10000}")