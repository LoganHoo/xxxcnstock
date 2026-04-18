#!/usr/bin/env python3
"""
对比4月12日选股结果与4月13日收盘价
"""
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# 4月12日选股结果
band_trend_stocks = [
    "000690", "600654", "002131", "600905", "600010",
    "600166", "000927", "600821", "600307", "002310"
]

short_term_stocks = [
    "600726", "002309", "601288", "002217", "000767"
]

all_stocks = list(set(band_trend_stocks + short_term_stocks))

def get_stock_price(code: str, date_str: str):
    """获取指定日期股票价格"""
    file_path = Path(f"/Volumes/Xdata/workstation/xxxcnstock/data/kline/{code}.parquet")
    if not file_path.exists():
        return None
    
    try:
        df = pl.read_parquet(file_path)
        df = df.filter(pl.col("trade_date") == date_str)
        if df.is_empty():
            return None
        return {
            "open": df["open"][0],
            "close": df["close"][0],
            "high": df["high"][0],
            "low": df["low"][0],
            "volume": df["volume"][0]
        }
    except Exception as e:
        print(f"读取 {code} 失败: {e}")
        return None

print("=" * 80)
print("4月12日选股结果 vs 4月13日收盘价对比")
print("=" * 80)

# 日期
apr_12 = "2026-04-12"
apr_13 = "2026-04-13"

print(f"\n📊 波段趋势股（前10只）")
print("-" * 80)
print(f"{'代码':<10} {'名称':<12} {'4/12收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态'}")
print("-" * 80)

for code in band_trend_stocks:
    price_12 = get_stock_price(code, apr_12)
    price_13 = get_stock_price(code, apr_13)
    
    if price_12 and price_13:
        change = price_13["close"] - price_12["close"]
        change_pct = (change / price_12["close"]) * 100
        status = "📈 涨" if change > 0 else "📉 跌" if change < 0 else "➡️ 平"
        print(f"{code:<10} {'':<12} {price_12['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status}")
    else:
        print(f"{code:<10} {'':<12} {'数据缺失':<12} {'数据缺失':<12}")

print(f"\n🚀 短线打板股（前5只）")
print("-" * 80)
print(f"{'代码':<10} {'名称':<12} {'4/12收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态'}")
print("-" * 80)

for code in short_term_stocks:
    price_12 = get_stock_price(code, apr_12)
    price_13 = get_stock_price(code, apr_13)
    
    if price_12 and price_13:
        change = price_13["close"] - price_12["close"]
        change_pct = (change / price_12["close"]) * 100
        status = "📈 涨" if change > 0 else "📉 跌" if change < 0 else "➡️ 平"
        print(f"{code:<10} {'':<12} {price_12['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status}")
    else:
        print(f"{code:<10} {'':<12} {'数据缺失':<12} {'数据缺失':<12}")

print("\n" + "=" * 80)
