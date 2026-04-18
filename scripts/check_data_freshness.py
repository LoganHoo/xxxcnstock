#!/usr/bin/env python3
"""
数据新鲜度检查
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import random

KLINE_DIR = Path('data/kline')

def main():
    # 获取所有股票文件
    all_files = list(KLINE_DIR.glob('*.parquet'))
    
    print("=" * 70)
    print("数据采集最新数据检查")
    print("=" * 70)
    print()
    print(f"当前日期: {datetime.now().strftime('%Y-%m-%d')}")
    print(f"总股票数: {len(all_files)}")
    print()
    
    # 统计最新日期分布
    latest_dates = {}
    for f in all_files[:500]:  # 抽样检查前500只
        try:
            df = pl.read_parquet(f)
            if not df.is_empty():
                latest_date = df['trade_date'].max()
                date_str = str(latest_date)
                if date_str not in latest_dates:
                    latest_dates[date_str] = 0
                latest_dates[date_str] += 1
        except:
            pass
    
    print("最新数据日期分布 (抽样500只):")
    print("-" * 70)
    for date in sorted(latest_dates.keys(), reverse=True)[:10]:
        count = latest_dates[date]
        bar = "█" * int(count / 10)
        print(f"  {date}: {count:4d} 只股票 {bar}")
    
    print()
    
    # 随机抽取3只股票展示最新数据
    random.seed(42)
    sample = random.sample(all_files, 3)
    
    print("随机抽取3只股票最新数据:")
    print("-" * 70)
    
    for i, pf in enumerate(sample, 1):
        code = pf.stem
        df = pl.read_parquet(pf)
        
        # 获取最新5条
        recent = df.tail(5)
        latest = recent[-1].to_dict()
        
        print(f"\n【{i}】股票代码: {code}")
        print(f"    最新日期: {latest['trade_date']}")
        print(f"    收盘价: ¥{latest['close']:.2f}")
        change_pct = (latest['close'] - latest['open']) / latest['open'] * 100
        print(f"    涨跌幅: {change_pct:.2f}%")
        print(f"    成交量: {latest['volume']:,.0f}")
        print(f"    最近5日:")
        for row in recent.iter_rows(named=True):
            print(f"      {row['trade_date']}: 收¥{row['close']:.2f} 量{row['volume']:,.0f}")
    
    print()
    print("=" * 70)
    
    # 检查今天是否有数据
    today = datetime.now().strftime('%Y-%m-%d')
    today_count = latest_dates.get(today, 0)
    if today_count > 0:
        print(f"✅ 今天({today})有 {today_count} 只股票已更新")
    else:
        latest = max(latest_dates.keys()) if latest_dates else "无数据"
        print(f"⚠️ 今天({today})无数据，最新数据日期: {latest}")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
