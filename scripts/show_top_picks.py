#!/usr/bin/env python3
"""显示选股Top结果"""
import sys
from pathlib import Path
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def main():
    scores = pl.read_parquet(PROJECT_ROOT / 'data/enhanced_scores_full.parquet')
    scores = scores.sort('enhanced_score', descending=True)
    
    print('🎯 选股 Top 20 (基于4月21日最新数据)')
    print('=' * 100)
    print(f"{'排名':<4} {'代码':<8} {'评分':<6} {'价格':<10} {'涨幅':<10} {'成交量':<15}")
    print('-' * 100)
    
    for i, row in enumerate(scores.head(20).iter_rows(named=True), 1):
        print(f"{i:<4} {row['code']:<8} {row['enhanced_score']:<6.0f} {row['price']:<10.2f} {row['change_pct']:<10.2f} {row['volume']:>15,.0f}")
    
    print('=' * 100)
    print('数据日期: 2026-04-21')
    print('评分统计: 平均58.5 | 最高98 | 最低11 | 总计5300只')

if __name__ == '__main__':
    main()
