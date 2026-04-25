#!/usr/bin/env python3
"""
小批量测试 - 测试20只股票
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from scripts.collect_kline_batch import fetch_kline_data_batched
from core.paths import DATA_DIR

print("=" * 60)
print("小批量采集测试 (20只股票)")
print("=" * 60)

# 计算300天前的日期
end_date = datetime.now()
start_date = end_date - timedelta(days=300)

print(f"\n时间范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

# 获取股票列表
print("\n加载股票列表...")
stock_list = pd.read_parquet(DATA_DIR / 'stock_list.parquet')
codes = stock_list['code'].tolist()[:20]  # 只取前20只
print(f"测试股票数量: {len(codes)} 只")
print(f"股票代码: {', '.join(codes[:5])}...")

# 分批采集
print("\n开始分批采集...")
print("-" * 60)

result = fetch_kline_data_batched(
    codes=codes,
    kline_dir=DATA_DIR / 'kline',
    days=300,
    batch_size=10,  # 每批10只
    batch_pause=3.0,  # 暂停3秒
    filter_delisted=True
)

print("\n" + "=" * 60)
print("测试结果:")
print(f"  成功: {result['success_count']}/{result['total_codes']}")
print(f"  跳过: {result['skipped_count']}")
print(f"  失败: {result['failed_count']}")
print(f"  总行数: {result['total_rows']}")
print("=" * 60)
