#!/usr/bin/env python3
"""
对比4月11日选股结果：4月11日收盘 vs 4月13日收盘
"""
import polars as pl
from pathlib import Path

# 4月11日选股结果
band_trend_stocks = [
    "600666", "601857", "002506", "002263", "002256",
    "002309", "300058", "002470", "601868", "601288"
]

short_term_stocks = [
    "002131", "002309", "600759", "002470", "600166"
]

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
        return None

print("=" * 90)
print("4月11日选股结果 vs 4月13日收盘价对比（跨周末2个交易日）")
print("=" * 90)

apr_11 = "2026-04-11"
apr_13 = "2026-04-13"

print(f"\n📊 波段趋势股（前10只）")
print("-" * 90)
print(f"{'代码':<10} {'4/11收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态':<6} {'评价'}")
print("-" * 90)

band_total_change = 0
band_count = 0

for code in band_trend_stocks:
    price_11 = get_stock_price(code, apr_11)
    price_13 = get_stock_price(code, apr_13)
    
    if price_11 and price_13:
        change = price_13["close"] - price_11["close"]
        change_pct = (change / price_11["close"]) * 100
        band_total_change += change_pct
        band_count += 1
        
        status = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        if change_pct > 5:
            eval_text = "强势"
        elif change_pct > 0:
            eval_text = "良好"
        elif change_pct > -3:
            eval_text = "一般"
        else:
            eval_text = "弱势"
        print(f"{code:<10} {price_11['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status:<6} {eval_text}")
    else:
        print(f"{code:<10} {'数据缺失':<12} {'数据缺失':<12}")

if band_count > 0:
    band_avg = band_total_change / band_count
    print("-" * 90)
    print(f"{'波段趋势平均涨跌幅':<70} {band_avg:>+10.2f}%")

print(f"\n🚀 短线打板股（前5只）")
print("-" * 90)
print(f"{'代码':<10} {'4/11收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态':<6} {'评价'}")
print("-" * 90)

short_total_change = 0
short_count = 0

for code in short_term_stocks:
    price_11 = get_stock_price(code, apr_11)
    price_13 = get_stock_price(code, apr_13)
    
    if price_11 and price_13:
        change = price_13["close"] - price_11["close"]
        change_pct = (change / price_11["close"]) * 100
        short_total_change += change_pct
        short_count += 1
        
        status = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        if change_pct > 5:
            eval_text = "强势"
        elif change_pct > 0:
            eval_text = "良好"
        elif change_pct > -3:
            eval_text = "一般"
        else:
            eval_text = "弱势"
        print(f"{code:<10} {price_11['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status:<6} {eval_text}")
    else:
        print(f"{code:<10} {'数据缺失':<12} {'数据缺失':<12}")

if short_count > 0:
    short_avg = short_total_change / short_count
    print("-" * 90)
    print(f"{'短线打板平均涨跌幅':<70} {short_avg:>+10.2f}%")

print("\n" + "=" * 90)
print("📈 总体表现")
print("=" * 90)
if band_count > 0 and short_count > 0:
    total_avg = (band_total_change + short_total_change) / (band_count + short_count)
    print(f"波段趋势股平均: {band_avg:+.2f}% ({band_count}只)")
    print(f"短线打板股平均: {short_avg:+.2f}% ({short_count}只)")
    print(f"总体平均涨跌幅: {total_avg:+.2f}%")
print("=" * 90)
