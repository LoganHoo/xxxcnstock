#!/usr/bin/env python3
"""
测试 GE Checkpoint Validators
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from services.data_service.quality.ge_checkpoint_validators import (
    GECheckpointValidators, GERetryConfig, CheckStatus
)
import polars as pl
import pandas as pd
import numpy as np

print('🧪 测试 GE Checkpoint Validators')
print('=' * 60)

# 初始化验证器
config = GERetryConfig(max_retries=2, retry_delay=0.5)
validator = GECheckpointValidators(config)
print(f'✅ 初始化成功: max_retries={config.max_retries}')

# 测试检查点3: 计算前检查
print('\n📝 测试检查点3: 计算前检查')

# 正常数据
df_good = pl.DataFrame({
    'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03'] * 10,
    'close': np.random.uniform(10, 100, 30),
    'volume': np.random.uniform(1000, 100000, 30),
    'high': np.random.uniform(10, 110, 30),
    'low': np.random.uniform(5, 90, 30),
})
result = validator.pre_scoring_check(df_good, '000001')
print(f'   正常数据: {result.status.value} - {result.message}')

# 数据不足
df_small = pl.DataFrame({
    'trade_date': ['2024-01-01', '2024-01-02'],
    'close': [10.0, 11.0],
    'volume': [1000, 2000],
    'high': [11.0, 12.0],
    'low': [9.0, 10.0],
})
result = validator.pre_scoring_check(df_small, '000002')
print(f'   数据不足: {result.status.value} - {result.message}')

# 测试检查点4: 计算后验证
print('\n📝 测试检查点4: 计算后验证')
scores_df = pl.DataFrame({
    'code': ['000001', '000002', '000003'] * 100,
    'name': ['股票A', '股票B', '股票C'] * 100,
    'enhanced_score': np.random.uniform(20, 90, 300),
    'trade_date': ['2024-01-01'] * 300,
})
result = validator.post_scoring_validation(scores_df)
print(f'   评分结果: {result.status.value} - {result.message}')
ge_rate = result.details.get('success_rate', 0)
print(f'   GE成功率: {ge_rate:.1f}%')

# 测试检查点6: 最终输出验证
print('\n📝 测试检查点6: 最终输出验证')
top_stocks = pd.DataFrame({
    'code': ['000001', '000002', '000003'],
    'name': ['平安银行', '万科A', '国农科技'],
    'score': [85.5, 82.3, 78.9],
})
result = validator.final_output_validation(top_stocks)
print(f'   正常输出: {result.status.value} - {result.message}')

# 包含ST股票
st_stocks = pd.DataFrame({
    'code': ['000001', '000002'],
    'name': ['平安银行', '*ST股票'],
    'score': [85.5, 20.0],
})
result = validator.final_output_validation(st_stocks)
print(f'   包含ST: {result.status.value} - {result.message}')

print('\n' + '=' * 60)
print('✅ 所有 GE 检查点测试完成!')
