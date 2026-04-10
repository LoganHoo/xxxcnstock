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

# Check v_total distribution on latest date
latest_date = data["trade_date"].max()
latest = data.filter(pl.col("trade_date") == latest_date)

print(f"Latest date: {latest_date}")
print(f"Rows: {len(latest)}")
print(f"v_total stats:")
print(f"  min: {latest['factor_v_total'].min()}")
print(f"  max: {latest['factor_v_total'].max()}")
print(f"  mean: {latest['factor_v_total'].mean()}")
print(f"  first 5: {latest['factor_v_total'].head(5).to_list()}")
print(f"  n_unique: {latest['factor_v_total'].n_unique()}")

# Check group_by result
daily = data.group_by("trade_date").agg([
    pl.mean("factor_v_total").alias("avg_v_total"),
]).sort("trade_date")
print(f"\nLast 3 daily avg_v_total:")
print(daily.tail(3))