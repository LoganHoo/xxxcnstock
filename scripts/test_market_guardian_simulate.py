#!/usr/bin/env python3
"""
测试市场守护模块 - 模拟交易日盘中场景
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime
from unittest.mock import patch

# 模拟 2026-04-17 (周五) 10:30 的盘中场景
mock_now = datetime(2026, 4, 17, 10, 30, 0)  # 周五 10:30

print('=' * 70)
print('模拟测试: 2026-04-17 (周五) 10:30 盘中场景')
print('=' * 70)
print(f'模拟时间: {mock_now.strftime("%Y-%m-%d %H:%M:%S")} 周五')

# 使用 patch 模拟 datetime.now
with patch('core.market_guardian.datetime') as mock_datetime:
    mock_datetime.now.return_value = mock_now
    mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
    
    from core.market_guardian import MarketGuardian
    
    today = '2026-04-17'
    yesterday = '2026-04-16'
    
    print('\n' + '=' * 70)
    print('场景1: 采集当日数据 (2026-04-17) - 应该禁止')
    print('=' * 70)
    allowed, msg = MarketGuardian.check_collection_allowed(target_date=today)
    print(f'结果: {msg}')
    print(f'允许: {"✅ 是" if allowed else "❌ 否"}')
    
    print('\n' + '=' * 70)
    print('场景2: 采集历史数据 (2026-04-16) - 应该允许')
    print('=' * 70)
    allowed, msg = MarketGuardian.check_collection_allowed(target_date=yesterday)
    print(f'结果: {msg}')
    print(f'允许: {"✅ 是" if allowed else "❌ 否"}')
    
    print('\n' + '=' * 70)
    print('场景3: 强制指定历史日期 - 应该允许')
    print('=' * 70)
    allowed, msg = MarketGuardian.check_collection_allowed(force_date='2026-04-15')
    print(f'结果: {msg}')
    print(f'允许: {"✅ 是" if allowed else "❌ 否"}')

print('\n' + '=' * 70)
print('模拟测试完成')
print('=' * 70)
