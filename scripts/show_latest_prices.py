#!/usr/bin/env python3
"""
显示最新的10个股票价格
"""
import polars as pl
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')

def main():
    # 获取所有股票文件
    all_files = list(KLINE_DIR.glob('*.parquet'))
    
    # 收集最新数据
    latest_data = []
    for f in all_files:
        try:
            df = pl.read_parquet(f)
            if not df.is_empty():
                latest_row = df[-1].to_dicts()[0]
                latest_data.append({
                    'code': f.stem,
                    'date': str(latest_row['trade_date']),
                    'close': float(latest_row['close']),
                    'open': float(latest_row['open']),
                    'high': float(latest_row['high']),
                    'low': float(latest_row['low']),
                    'volume': int(latest_row['volume'])
                })
        except Exception as e:
            pass
    
    # 按日期排序，取最新的10只
    today = datetime.now().strftime('%Y-%m-%d')
    today_stocks = [s for s in latest_data if s['date'] == today]
    
    # 如果没有今天的，取最新日期的
    if not today_stocks and latest_data:
        latest_date = max([s['date'] for s in latest_data])
        today_stocks = [s for s in latest_data if s['date'] == latest_date]
    
    # 按成交量排序取前10只
    top10 = sorted(today_stocks, key=lambda x: x['volume'], reverse=True)[:10]
    
    print("=" * 80)
    print(f"最新10只股票价格 (日期: {top10[0]['date'] if top10 else 'N/A'})")
    print("=" * 80)
    print()
    print(f"{'序号':<4} {'代码':<8} {'收盘价':<10} {'开盘价':<10} {'最高价':<10} {'最低价':<10} {'成交量':<12}")
    print("-" * 80)
    
    for i, stock in enumerate(top10, 1):
        change_pct = (stock['close'] - stock['open']) / stock['open'] * 100
        change_str = f"{change_pct:+.2f}%"
        print(f"{i:<4} {stock['code']:<8} ¥{stock['close']:<9.2f} ¥{stock['open']:<9.2f} ¥{stock['high']:<9.2f} ¥{stock['low']:<9.2f} {stock['volume']:<12,}")
    
    print("=" * 80)

if __name__ == "__main__":
    main()
