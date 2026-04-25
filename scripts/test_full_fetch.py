#!/usr/bin/env python3
"""
测试全量更新 - 使用数据较旧或不存在的股票
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
from datetime import datetime, timedelta
from services.data_service.fetchers.unified_fetcher import get_unified_fetcher
from services.data_service.fetchers.kline_fetcher import get_incremental_date_range
from core.paths import DATA_DIR
from pathlib import Path

async def test_full_fetch():
    """测试全量采集"""
    print("=" * 60)
    print("测试全量更新")
    print("=" * 60)
    
    # 获取获取器
    fetcher = await get_unified_fetcher()
    print(f"\n当前数据源: {fetcher.ds_manager.current_source}")
    
    # 测试股票 - 使用一个可能数据较旧的股票
    code = '000002'  # 万科A
    days = 30  # 只取30天数据测试
    kline_path = DATA_DIR / 'kline'
    
    # 删除现有数据文件（模拟全量更新）
    kline_file = kline_path / f"{code}.parquet"
    if kline_file.exists():
        print(f"\n删除现有数据文件: {kline_file}")
        kline_file.unlink()
    
    # 获取日期范围
    start_date, end_date, is_incremental = get_incremental_date_range(code, days, kline_path)
    print(f"\n股票: {code}")
    print(f"开始日期: {start_date}")
    print(f"结束日期: {end_date}")
    print(f"是否增量: {is_incremental}")
    
    # 采集数据
    print("\n开始采集...")
    df = await fetcher.fetch_kline(code, start_date, end_date)
    
    print(f"\n返回数据行数: {len(df)}")
    if not df.empty:
        print(f"数据列: {list(df.columns)}")
        print(f"数据预览:")
        print(df.head())
        print(f"\n日期范围: {df['date'].min()} 至 {df['date'].max()}")
    else:
        print("❌ 无数据返回")
        print(f"当前数据源: {fetcher.ds_manager.current_source}")

if __name__ == "__main__":
    asyncio.run(test_full_fetch())
