#!/usr/bin/env python3
"""
检查数据新鲜度
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import polars as pl
from pathlib import Path
from datetime import datetime

print('=' * 60)
print('📊 数据新鲜度检查')
print('=' * 60)

# 当前时间
now = datetime.now()
print(f'\n当前时间: {now.strftime("%Y-%m-%d %H:%M:%S")}')

# 检查股票列表
try:
    stock_list = pl.read_parquet('data/stock_list.parquet')
    print(f'\n✅ 股票列表: {len(stock_list)} 只股票')
except Exception as e:
    print(f'\n❌ 股票列表读取失败: {e}')

# 检查K线数据最新日期
print('\n📈 K线数据最新日期（样本）:')
kline_dir = Path('data/kline')
sample_files = list(kline_dir.glob('*.parquet'))[:10]

latest_dates = []
for f in sample_files:
    try:
        df = pl.read_parquet(f)
        if len(df) > 0 and 'trade_date' in df.columns:
            latest = df['trade_date'].max()
            latest_dates.append(latest)
            code = f.stem
            print(f'  {code}: {latest}')
    except Exception as e:
        pass

if latest_dates:
    # 找出所有样本中的最新日期
    from datetime import datetime as dt
    date_objects = []
    for d in latest_dates:
        if isinstance(d, str):
            try:
                date_objects.append(dt.strptime(d, '%Y-%m-%d'))
            except:
                pass
        else:
            date_objects.append(d)
    
    if date_objects:
        overall_latest = max(date_objects)
        days_diff = (now - overall_latest).days
        print(f'\n📅 数据最新日期: {overall_latest.strftime("%Y-%m-%d")}')
        print(f'⏰ 距离今天: {days_diff} 天')
        
        if days_diff == 0:
            print('✅ 数据已更新至今天（收盘后）')
        elif days_diff == 1:
            print('⚠️  数据截止到昨天，今天收盘后需要更新')
        else:
            print(f'⚠️  数据已落后 {days_diff} 天，建议尽快更新')

# 检查评分数据
try:
    scores = pl.read_parquet('data/enhanced_scores_full.parquet')
    if len(scores) > 0:
        if 'trade_date' in scores.columns:
            latest_score_date = scores['trade_date'].max()
            print(f'\n📊 评分数据最新日期: {latest_score_date}')
        print(f'📊 评分股票数量: {len(scores)}')
except Exception as e:
    print(f'\n❌ 评分数据读取失败: {e}')

print('\n' + '=' * 60)

# 检查是否盘中时段
current_hour = now.hour
current_minute = now.minute
current_time = current_hour * 100 + current_minute

# 判断是否是交易日（简化判断，实际应该检查日历）
weekday = now.weekday()
is_weekday = weekday < 5  # 周一到周五

print('\n⏰ 市场状态判断:')
if is_weekday:
    if 930 <= current_time <= 1500:
        print('⚠️  当前为盘中时段（9:30-15:00）')
        print('   ❌ 禁止采集当日数据（数据不完整）')
        print('   ✅ 可以采集历史数据')
    elif 1500 < current_time < 1530:
        print('⏳ 当前为收盘后整理时段（15:00-15:30）')
        print('   ⚠️  建议15:30后再采集当日数据')
    elif current_time >= 1530:
        print('✅ 当前为收盘后时段（15:30后）')
        print('   ✅ 可以采集当日收盘数据')
    else:
        print('✅ 当前为非交易时段')
        print('   ✅ 可以采集历史数据')
else:
    print('✅ 当前为非交易日（周末）')
    print('   ✅ 可以采集历史数据')

print('\n' + '=' * 60)
