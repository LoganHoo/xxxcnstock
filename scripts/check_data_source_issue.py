#!/usr/bin/env python3
"""
检查数据源问题 - 为什么价格与同花顺不一致
"""
import polars as pl
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')


def main():
    print("=" * 80)
    print("数据源问题分析")
    print("=" * 80)
    print()
    
    # 检查几个股票的数据详情
    test_codes = ['688223', '000001', '600519']
    
    for code in test_codes:
        print(f"\n【{code}】数据详情:")
        print("-" * 80)
        
        try:
            df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
            
            # 显示最近5条
            recent = df.tail(5)
            print(f"最近5个交易日数据:")
            
            for row in recent.iter_rows(named=True):
                fetch_time = row.get('fetch_time', 'N/A')
                print(f"  {row['trade_date']}: 收¥{row['close']:.2f} "
                      f"量{row['volume']:,.0f} (采集时间: {fetch_time})")
            
            # 检查最新日期
            latest_date = df['trade_date'].max()
            today = datetime.now().strftime('%Y-%m-%d')
            
            print(f"\n  最新数据日期: {latest_date}")
            print(f"  今天日期: {today}")
            
            if str(latest_date) == today:
                print(f"  状态: ✅ 已更新到今天")
            else:
                print(f"  状态: ⚠️ 数据不是今天的")
                
        except Exception as e:
            print(f"  错误: {e}")
    
    print()
    print("=" * 80)
    print()
    print("🔍 问题分析:")
    print()
    print("1. 数据源使用的是 baostock（主）+ akshare（备）")
    print("   - baostock: 提供历史K线数据")
    print("   - akshare: 可获取实时行情")
    print()
    print("2. 价格差异可能原因:")
    print("   a) 本地数据是昨日收盘数据，同花顺显示的是今日实时价格")
    print("   b) 数据源不同导致价格差异（不同数据源可能有复权差异）")
    print("   c) 采集时间不同（盘中采集 vs 收盘后采集）")
    print()
    print("3. 建议解决方案:")
    print("   a) 确认数据采集时间（应在交易日15:00收盘后）")
    print("   b) 使用 akshare 作为实时行情源进行验证")
    print("   c) 检查数据复权方式（前复权/后复权）")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
