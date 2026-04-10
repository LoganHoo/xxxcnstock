#!/usr/bin/env python3
import polars as pl
from pathlib import Path

kline_dir = Path("data/kline")
df = pl.read_parquet(kline_dir / "002119.parquet")
latest = df.sort("trade_date").tail(3)
print("002119 最新数据:")
print(latest[["trade_date", "open", "high", "low", "close", "volume"]])
print(f"\n最新日期: {latest['trade_date'][-1]}")
print(f"最新收盘价: {latest['close'][-1]}")