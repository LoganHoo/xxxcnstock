#!/usr/bin/env python3
"""显示股票详细信息"""
import sys
from pathlib import Path
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def show_stock_detail(rank: int = 7):
    # 读取评分文件
    scores = pl.read_parquet(PROJECT_ROOT / 'data/enhanced_scores_full.parquet')
    scores = scores.sort('enhanced_score', descending=True)
    
    # 获取指定排名
    row = scores.head(rank).tail(1).to_dicts()[0]
    
    print(f'📈 股票详情 - 第{rank}名')
    print('=' * 60)
    print(f"代码: {row['code']}")
    print(f"名称: {row['name']}")
    print(f"评分: {row['enhanced_score']}")
    print(f"当前价格: {row['price']}")
    print(f"涨跌幅: {row['change_pct']}%")
    print(f"成交量: {row['volume']:,.0f}")
    print(f"数据日期: {row['trade_date']}")

if __name__ == '__main__':
    show_stock_detail(7)
