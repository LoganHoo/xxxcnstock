#!/usr/bin/env python3
"""
测试市场守护模块 - 区分当日数据和历史数据
"""
import sys
sys.path.insert(0, '.')

from core.market_guardian import MarketGuardian
from datetime import datetime

print('=' * 70)
print('测试市场守护模块 - 区分当日数据和历史数据')
print('=' * 70)

# 模拟场景测试
now = datetime.now()
today = now.strftime('%Y-%m-%d')
yesterday = '2026-04-17'

print(f'\n当前时间: {now.strftime("%Y-%m-%d %H:%M:%S")}')
print(f'今天: {today}')
print(f'昨天: {yesterday}')

print('\n' + '=' * 70)
print('场景1: 采集当日数据 (今天)')
print('=' * 70)
allowed, msg = MarketGuardian.check_collection_allowed(target_date=today)
print(f'结果: {msg}')
print(f'允许: {"✅ 是" if allowed else "❌ 否"}')

print('\n' + '=' * 70)
print('场景2: 采集历史数据 (昨天)')
print('=' * 70)
allowed, msg = MarketGuardian.check_collection_allowed(target_date=yesterday)
print(f'结果: {msg}')
print(f'允许: {"✅ 是" if allowed else "❌ 否"}')

print('\n' + '=' * 70)
print('场景3: 强制指定历史日期')
print('=' * 70)
allowed, msg = MarketGuardian.check_collection_allowed(force_date='2026-04-15')
print(f'结果: {msg}')
print(f'允许: {"✅ 是" if allowed else "❌ 否"}')

print('\n' + '=' * 70)
print('场景4: 默认采集（今天）')
print('=' * 70)
allowed, msg = MarketGuardian.check_collection_allowed()
print(f'结果: {msg}')
print(f'允许: {"✅ 是" if allowed else "❌ 否"}')

print('\n' + '=' * 70)
print('测试完成')
print('=' * 70)
