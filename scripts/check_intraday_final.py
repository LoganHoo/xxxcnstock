#!/usr/bin/env python3
"""
检查盘中采集违规情况 - 最终版本
"""
import sys
sys.path.insert(0, '.')

import polars as pl
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')
TARGET_DATE = "2026-04-17"


def main():
    print("=" * 70)
    print(f"检查 {TARGET_DATE} 数据采集时间违规情况")
    print("=" * 70)
    print("\n⚠️  规则: 交易日 9:30-15:00 禁止采集数据")
    print("=" * 70)
    
    all_files = list(KLINE_DIR.glob("*.parquet"))
    total = 0
    intraday_count = 0
    before_market_count = 0
    after_close_count = 0
    weekend_count = 0
    
    intraday_samples = []
    
    for f in all_files:
        try:
            df = pl.read_parquet(f)
            row = df.filter(pl.col('trade_date').cast(str) == TARGET_DATE)
            if not row.is_empty():
                total += 1
                
                # 正确获取 fetch_time 值
                fetch_time_series = row[0]['fetch_time']
                fetch_time_str = fetch_time_series[0] if len(fetch_time_series) > 0 else None
                
                if fetch_time_str:
                    try:
                        dt = datetime.strptime(fetch_time_str, '%Y-%m-%d %H:%M:%S')
                        weekday = dt.weekday()
                        hour = dt.hour
                        minute = dt.minute
                        time_val = hour * 60 + minute
                        
                        # 判断是否在交易日盘中
                        if weekday < 5:  # 周一到周五 (0-4)
                            if 570 <= time_val < 900:  # 9:30-15:00
                                intraday_count += 1
                                if len(intraday_samples) < 10:
                                    code = f.stem
                                    intraday_samples.append((code, fetch_time_str))
                            elif time_val < 570:  # 9:30前
                                before_market_count += 1
                            else:  # 15:00后
                                after_close_count += 1
                        else:
                            weekend_count += 1
                    except:
                        pass
        except:
            pass
    
    print(f"\n统计结果:")
    print(f"  有 {TARGET_DATE} 数据的股票: {total}")
    print(f"  🚨 盘中采集 (9:30-15:00): {intraday_count}")
    print(f"  ⏰ 盘前采集 (9:30前): {before_market_count}")
    print(f"  ✅ 收盘后采集 (15:00+): {after_close_count}")
    print(f"  📅 周末采集: {weekend_count}")
    
    print("\n" + "=" * 70)
    if intraday_count > 0:
        print(f"🚨 严重违规: {intraday_count} 只股票在交易日盘中采集！")
        print("\n违规样本:")
        for code, fetch_time in intraday_samples:
            print(f"  {code}: 采集时间 {fetch_time}")
        print("\n⚠️  影响: 盘中采集的数据不完整，可能导致:")
        print("   - 收盘价不准确")
        print("   - 成交量不完整")
        print("   - 技术指标计算错误")
        print("   - 选股策略失效")
    else:
        print("✅ 未发现盘中采集违规")
    print("=" * 70)


if __name__ == "__main__":
    main()
