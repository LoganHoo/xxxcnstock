#!/usr/bin/env python3
"""
对比 data_collect.py 采集的数据和 baostock 原始数据
"""
import sys
sys.path.insert(0, '.')

import polars as pl
import baostock as bs
from pathlib import Path
from datetime import datetime, timedelta

KLINE_DIR = Path('data/kline')


def get_local_data(code: str) -> dict:
    """获取本地采集的数据"""
    try:
        df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
        if df.is_empty():
            return None
        latest = df[-1].to_dicts()[0]
        return {
            'date': str(latest['trade_date']),
            'open': float(latest['open']),
            'high': float(latest['high']),
            'low': float(latest['low']),
            'close': float(latest['close']),
            'volume': int(latest['volume']),
        }
    except Exception as e:
        return None


def get_baostock_data(code: str) -> dict:
    """从 baostock 获取数据"""
    lg = bs.login()
    if lg.error_code != '0':
        return None
    
    try:
        code_bs = f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}"
        today = datetime.now().strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,open,high,low,close,volume",
            start_date=today,
            end_date=today,
            frequency="d"
        )
        
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            return {
                'date': row[0],
                'open': float(row[1]) if row[1] else 0,
                'high': float(row[2]) if row[2] else 0,
                'low': float(row[3]) if row[3] else 0,
                'close': float(row[4]) if row[4] else 0,
                'volume': int(row[5]) if row[5] else 0,
            }
    except Exception as e:
        print(f"Error: {e}")
    finally:
        bs.logout()
    
    return None


def compare_stock(code: str):
    """对比单只股票的数据"""
    print(f"\n【{code}】数据对比:")
    print("-" * 70)
    
    local = get_local_data(code)
    baostock = get_baostock_data(code)
    
    if not local:
        print("  ❌ 本地数据不存在")
        return
    
    if not baostock:
        print("  ❌ baostock 数据获取失败")
        return
    
    print(f"  日期: {local['date']} (本地) vs {baostock['date']} (baostock)")
    print()
    
    # 对比各字段
    fields = ['open', 'high', 'low', 'close', 'volume']
    all_match = True
    
    for field in fields:
        local_val = local[field]
        bs_val = baostock[field]
        
        if field == 'volume':
            match = local_val == bs_val
            diff = local_val - bs_val
            print(f"  {field:8}: {local_val:12,} (本地) vs {bs_val:12,} (baostock) | {'✅' if match else '❌'} 差异: {diff:,}")
        else:
            match = abs(local_val - bs_val) < 0.01  # 价格允许0.01误差
            diff = local_val - bs_val
            print(f"  {field:8}: ¥{local_val:10.4f} (本地) vs ¥{bs_val:10.4f} (baostock) | {'✅' if match else '❌'} 差异: {diff:+.4f}")
        
        if not match:
            all_match = False
    
    print()
    if all_match:
        print("  ✅ 数据一致")
    else:
        print("  ❌ 数据存在差异")
    
    return all_match


def main():
    print("=" * 70)
    print("数据对比: data_collect.py 采集 vs baostock 原始")
    print("=" * 70)
    
    # 测试股票
    test_codes = ['002119', '002219', '000001', '600519']
    
    results = {}
    for code in test_codes:
        result = compare_stock(code)
        results[code] = result
    
    print("\n" + "=" * 70)
    print("对比总结:")
    print("-" * 70)
    
    match_count = sum(1 for r in results.values() if r)
    total_count = len(results)
    
    for code, match in results.items():
        status = "✅ 一致" if match else "❌ 差异"
        print(f"  {code}: {status}")
    
    print()
    print(f"结果: {match_count}/{total_count} 只股票数据一致")
    print("=" * 70)


if __name__ == "__main__":
    main()
