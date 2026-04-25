#!/usr/bin/env python3
"""显示Top 20选股结果"""
import json
import pandas as pd
from pathlib import Path

# 读取最新的选股结果
result_file = 'data/workflow_results/real_selection_comprehensive_2026-04-20.json'
with open(result_file, 'r') as f:
    result = json.load(f)

top_stocks = result['top_stocks'][:20]

# 读取股票列表获取名称
stock_list = pd.read_parquet('data/stock_list.parquet')
code_to_name = dict(zip(stock_list['code'], stock_list['code_name']))

# 获取最新收盘价
kline_dir = Path('data/kline')

print('='*90)
print('Top 20 选股结果 (含名称和收盘价)')
print('='*90)
print(f"{'排名':<6}{'代码':<10}{'名称':<14}{'收盘价':<10}{'综合评分':<10}{'财务':<8}{'市场':<8}{'技术':<8}")
print('-'*90)

for i, stock in enumerate(top_stocks, 1):
    code = stock['code']
    name = code_to_name.get(code, '')[:12]  # 限制名称长度
    
    # 获取最新收盘价
    kline_file = kline_dir / f'{code}.parquet'
    close_price = 'N/A'
    if kline_file.exists():
        try:
            df = pd.read_parquet(kline_file)
            if not df.empty and 'close' in df.columns:
                close_price = f"{df['close'].iloc[-1]:.2f}"
        except:
            pass
    
    print(f"{i:<6}{code:<10}{name:<14}{close_price:<10}{stock['total_score']:<10.1f}{stock['financial_score']:<8.1f}{stock['market_score']:<8.1f}{stock['technical_score']:<8.1f}")

print('='*90)
