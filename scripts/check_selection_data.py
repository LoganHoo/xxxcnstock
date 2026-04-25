#!/usr/bin/env python3
"""
检查选股数据来源和完整性
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import polars as pl
from datetime import datetime

print('=' * 60)
print('📊 选股数据来源检查')
print('=' * 60)

# 1. 检查评分数据
print('\n1️⃣ 评分数据来源:')
scores = pl.read_parquet('data/enhanced_scores_full.parquet')
print(f'   文件: data/enhanced_scores_full.parquet')
print(f'   股票数: {len(scores)}')
print(f'   数据日期: {scores["trade_date"].unique().to_list()}')
print(f'   列名: {scores.columns}')

# 2. 检查股票列表
print('\n2️⃣ 股票列表来源:')
stock_list = pl.read_parquet('data/stock_list.parquet')
print(f'   文件: data/stock_list.parquet')
print(f'   股票数: {len(stock_list)}')
print(f'   列名: {stock_list.columns}')

# 3. 检查选股结果
print('\n3️⃣ 选股结果:')
selection = pl.read_parquet('data/selection_results/selection_2026-04-23.parquet')
print(f'   文件: data/selection_results/selection_2026-04-23.parquet')
print(f'   选中股票数: {len(selection)}')
print(f'   列名: {selection.columns}')
print(f'   数据日期: {selection["trade_date"].unique().to_list()}')

# 4. 检查名称缺失问题
print('\n4️⃣ 名称缺失检查:')
try:
    empty_names = selection.filter(pl.col('name') == '').shape[0]
except:
    empty_names = 0
try:
    null_names = selection.filter(pl.col('name').is_null()).shape[0]
except:
    null_names = 0
print(f'   空字符串名称: {empty_names}')
print(f'   Null名称: {null_names}')

# 5. 检查原始评分数据中的名称
print('\n5️⃣ 评分数据中的名称情况:')
try:
    empty_score_names = scores.filter(pl.col('name') == '').shape[0]
except:
    empty_score_names = 0
try:
    null_score_names = scores.filter(pl.col('name').is_null()).shape[0]
except:
    null_score_names = 0
print(f'   空字符串名称: {empty_score_names}')
print(f'   Null名称: {null_score_names}')

# 6. 检查股票列表中的名称
print('\n6️⃣ 股票列表中的名称情况:')
print(f'   样本:')
for row in stock_list.head(5).to_dicts():
    print(f'     {row["code"]}: {row.get("name", "N/A")}')

# 7. 检查合并后的数据
print('\n7️⃣ 合并数据检查:')
merged = scores.join(stock_list, on='code', how='left')
print(f'   合并后行数: {len(merged)}')

# 检查名称列
if 'name_right' in merged.columns:
    try:
        empty_right = merged.filter(pl.col('name_right') == '').shape[0]
    except:
        empty_right = 0
    try:
        null_right = merged.filter(pl.col('name_right').is_null()).shape[0]
    except:
        null_right = 0
    print(f'   name_right 空字符串: {empty_right}')
    print(f'   name_right Null: {null_right}')

print('\n' + '=' * 60)
print('📅 数据新鲜度:')
print(f'   当前日期: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print(f'   评分数据日期: {scores["trade_date"].max()}')
print(f'   选股结果日期: {selection["trade_date"].max()}')
print('=' * 60)
