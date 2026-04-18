#!/usr/bin/env python3
"""
对比4月10日（周五）收盘 vs 4月13日（周一）收盘
"""
import polars as pl
from pathlib import Path

# 4月11日选股结果（实际用4月10日收盘对比4月13日收盘）
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

print("=" * 100)
print("📊 4月10日（周五）收盘 vs 4月13日（周一）收盘 对比")
print("=" * 100)

apr_10 = "2026-04-10"
apr_13 = "2026-04-13"

print(f"\n📈 波段趋势股（前10只）")
print("-" * 100)
print(f"{'代码':<10} {'4/10收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态':<6} {'评价'}")
print("-" * 100)

band_total_change = 0
band_count = 0
band_up = 0
band_down = 0

for code in band_trend_stocks:
    price_10 = get_stock_price(code, apr_10)
    price_13 = get_stock_price(code, apr_13)
    
    if price_10 and price_13:
        change = price_13["close"] - price_10["close"]
        change_pct = (change / price_10["close"]) * 100
        band_total_change += change_pct
        band_count += 1
        if change > 0:
            band_up += 1
        elif change < 0:
            band_down += 1
        
        status = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        if change_pct >= 9:
            eval_text = "🔥涨停"
        elif change_pct >= 5:
            eval_text = "强势"
        elif change_pct > 0:
            eval_text = "良好"
        elif change_pct > -3:
            eval_text = "一般"
        else:
            eval_text = "弱势"
        print(f"{code:<10} {price_10['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status:<6} {eval_text}")
    else:
        missing_date = "4/10" if not price_10 else "4/13"
        print(f"{code:<10} {'数据缺失':<12} {'数据缺失':<12} ({missing_date}无数据)")

if band_count > 0:
    band_avg = band_total_change / band_count
    print("-" * 100)
    print(f"{'波段趋势统计':<70} 涨:{band_up} 跌:{band_down} 平均:{band_avg:+.2f}%")

print(f"\n🚀 短线打板股（前5只）")
print("-" * 100)
print(f"{'代码':<10} {'4/10收盘':<12} {'4/13收盘':<12} {'涨跌额':<10} {'涨跌幅':<10} {'状态':<6} {'评价'}")
print("-" * 100)

short_total_change = 0
short_count = 0
short_up = 0
short_down = 0

for code in short_term_stocks:
    price_10 = get_stock_price(code, apr_10)
    price_13 = get_stock_price(code, apr_13)
    
    if price_10 and price_13:
        change = price_13["close"] - price_10["close"]
        change_pct = (change / price_10["close"]) * 100
        short_total_change += change_pct
        short_count += 1
        if change > 0:
            short_up += 1
        elif change < 0:
            short_down += 1
        
        status = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        if change_pct >= 9:
            eval_text = "🔥涨停"
        elif change_pct >= 5:
            eval_text = "强势"
        elif change_pct > 0:
            eval_text = "良好"
        elif change_pct > -3:
            eval_text = "一般"
        else:
            eval_text = "弱势"
        print(f"{code:<10} {price_10['close']:<12.2f} {price_13['close']:<12.2f} {change:<+10.2f} {change_pct:<+10.2f}% {status:<6} {eval_text}")
    else:
        missing_date = "4/10" if not price_10 else "4/13"
        print(f"{code:<10} {'数据缺失':<12} {'数据缺失':<12} ({missing_date}无数据)")

if short_count > 0:
    short_avg = short_total_change / short_count
    print("-" * 100)
    print(f"{'短线打板统计':<70} 涨:{short_up} 跌:{short_down} 平均:{short_avg:+.2f}%")

print("\n" + "=" * 100)
print("📊 总体表现汇总")
print("=" * 100)
if band_count > 0 and short_count > 0:
    total_avg = (band_total_change + short_total_change) / (band_count + short_count)
    total_up = band_up + short_up
    total_down = band_down + short_down
    total_count = band_count + short_count
    
    print(f"\n波段趋势股（{band_count}只）:")
    print(f"  上涨: {band_up}只 | 下跌: {band_down}只 | 平均涨跌幅: {band_avg:+.2f}%")
    
    print(f"\n短线打板股（{short_count}只）:")
    print(f"  上涨: {short_up}只 | 下跌: {short_down}只 | 平均涨跌幅: {short_avg:+.2f}%")
    
    print(f"\n总体表现（{total_count}只）:")
    print(f"  上涨: {total_up}只 | 下跌: {total_down}只 | 平均涨跌幅: {total_avg:+.2f}%")
    
    if total_avg > 0:
        print(f"\n✅ 选股策略整体盈利: +{total_avg:.2f}%")
    else:
        print(f"\n❌ 选股策略整体亏损: {total_avg:.2f}%")
        
print("=" * 100)
