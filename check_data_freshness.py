#!/usr/bin/env python3
"""验证数据新鲜度"""
import sys
sys.path.insert(0, '.')

import polars as pl
from pathlib import Path
from datetime import datetime
from collections import Counter

print('=' * 60)
print('🔍 验证数据新鲜度')
print('=' * 60)

kline_dir = Path('data/kline')
parquet_files = list(kline_dir.glob('*.parquet'))

print(f'\n📊 总股票数: {len(parquet_files)}')

# 检查所有文件的最新日期
latest_dates = []
outdated_stocks = []
error_files = []

for i, f in enumerate(parquet_files):
    try:
        df = pl.read_parquet(f)
        code = f.stem

        # 获取最新日期
        if 'date' in df.columns:
            latest = str(df['date'].max())
        elif 'trade_date' in df.columns:
            latest = str(df['trade_date'].max())
        else:
            continue

        latest_dates.append((code, latest))

        # 检查是否最新（2026-04-24）
        if latest != '2026-04-24':
            outdated_stocks.append((code, latest))

    except Exception as e:
        error_files.append((f.stem, str(e)))

# 统计日期分布
date_counts = Counter([d for _, d in latest_dates])

print(f'\n📋 最新日期分布（前10）:')
for date, count in sorted(date_counts.items(), reverse=True)[:10]:
    status = '✅' if date == '2026-04-24' else '⚠️'
    print(f'  {status} {date}: {count} 只')

# 计算最新数据占比
latest_count = sum(1 for _, d in latest_dates if d == '2026-04-24')
print(f'\n📈 数据新鲜度统计:')
print(f'  ✅ 最新 (2026-04-24): {latest_count} 只 ({latest_count/len(latest_dates)*100:.1f}%)')
print(f'  ⚠️  需要更新: {len(outdated_stocks)} 只 ({len(outdated_stocks)/len(latest_dates)*100:.1f}%)')
print(f'  ❌ 读取错误: {len(error_files)} 只')

# 显示需要更新的股票示例
if outdated_stocks:
    print(f'\n⚠️  需要更新的股票示例（前10）:')
    for code, date in sorted(outdated_stocks, key=lambda x: x[1])[:10]:
        print(f'  {code}: {date}')

# 检查是否有错误文件
if error_files:
    print(f'\n❌ 读取错误的文件（前5）:')
    for code, error in error_files[:5]:
        print(f'  {code}: {error}')

print('\n' + '=' * 60)
