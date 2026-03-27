"""检查分析统计信息"""
import sys
sys.path.insert(0, 'D:/workstation/xcnstock')

import pandas as pd
import os
from datetime import datetime

# 检查实时行情股票数
realtime = pd.read_parquet('data/realtime/20260316.parquet')
total_stocks = len(realtime)

# 过滤后的有效股票数
realtime = realtime[realtime['volume'] > 0]
realtime = realtime[~realtime['name'].str.contains('ST', case=False, na=False)]
realtime = realtime[realtime['price'] > 1]
valid_stocks = len(realtime)

# 已分析的股票数
analyzed = pd.read_parquet('data/enhanced_full_temp.parquet')
analyzed_count = len(analyzed)

# 获取文件创建时间
temp_time = os.path.getmtime('data/enhanced_full_temp.parquet')
realtime_time = os.path.getmtime('data/realtime/20260316.parquet')

# 估算时间
# 假设每只股票需要约0.3秒（网络请求+计算）
time_per_stock = 0.3
estimated_total = valid_stocks * time_per_stock / 60  # 分钟

print('='*50)
print('XCNStock 分析统计')
print('='*50)
print()
print('[数据量]')
print(f'  总股票数: {total_stocks}')
print(f'  有效股票数(过滤后): {valid_stocks}')
print(f'  已分析股票数: {analyzed_count}')
print(f'  分析覆盖率: {analyzed_count/valid_stocks*100:.1f}%')
print()
print('[时间信息]')
print(f'  实时行情采集: {datetime.fromtimestamp(realtime_time).strftime("%Y-%m-%d %H:%M:%S")}')
print(f'  分析完成时间: {datetime.fromtimestamp(temp_time).strftime("%Y-%m-%d %H:%M:%S")}')
print()
print('[时间估算]')
print(f'  单只股票耗时: ~{time_per_stock}秒 (含网络请求)')
print(f'  全量分析预估: ~{estimated_total:.1f}分钟')
print(f'  并行4线程预估: ~{estimated_total/4:.1f}分钟')
print()

# 失败的股票数
failed = valid_stocks - analyzed_count
print(f'[待处理]')
print(f'  未分析股票: {failed}只')
print(f'  补充耗时: ~{failed * time_per_stock / 60:.1f}分钟')
