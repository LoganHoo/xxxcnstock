#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")
import polars as pl
from core.factor_engine import FactorEngine
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

# Check group_by as indicator does
daily_data = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
    pl.mean("factor_limit_up_score").alias("avg_limit_up_score"),
    pl.mean("factor_pioneer_status").alias("avg_pioneer_status"),
    pl.mean("factor_cost_peak").alias("avg_cost_peak"),
    pl.mean("close").alias("avg_close"),
    pl.mean("open").alias("avg_open"),
    pl.count().alias("total_stocks")
]).sort("trade_date")

print("Last 3 rows of daily_data:")
print(daily_data.tail(3))