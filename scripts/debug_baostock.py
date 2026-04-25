#!/usr/bin/env python3
"""
调试Baostock批量采集问题
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
from datetime import datetime, timedelta
from services.data_service.fetchers.unified_fetcher import get_unified_fetcher
from services.data_service.fetchers.kline_fetcher import get_incremental_date_range
from core.paths import DATA_DIR

async def debug_fetch():
    """调试采集"""
    print("=" * 60)
    print("调试Baostock批量采集")
    print("=" * 60)
    
    # 获取获取器
    fetcher = await get_unified_fetcher()
    print(f"\n当前数据源: {fetcher.ds_manager.current_source}")
    print(f"主源: {fetcher.ds_manager.primary_provider}")
    print(f"备源: {[p.name for p in fetcher.ds_manager.backup_providers]}")
    
    # 测试股票
    code = '000001'
    days = 300
    kline_path = DATA_DIR / 'kline'
    
    # 获取日期范围
    start_date, end_date, is_incremental = get_incremental_date_range(code, days, kline_path)
    print(f"\n股票: {code}")
    print(f"开始日期: {start_date}")
    print(f"结束日期: {end_date}")
    print(f"是否增量: {is_incremental}")
    
    # 直接调用数据源管理器
    print("\n直接调用数据源管理器...")
    df = await fetcher.ds_manager.fetch_kline(code, start_date, end_date)
    print(f"返回数据行数: {len(df)}")
    if not df.empty:
        print(f"数据列: {list(df.columns)}")
        print(f"数据预览:\n{df.head()}")
    else:
        print("❌ 无数据返回")
        print(f"当前数据源: {fetcher.ds_manager.current_source}")
    
    # 测试 unified_fetcher
    print("\n通过 unified_fetcher 获取...")
    df2 = await fetcher.fetch_kline(code, start_date, end_date)
    print(f"返回数据行数: {len(df2)}")
    if not df2.empty:
        print(f"数据列: {list(df2.columns)}")
        print(f"数据预览:\n{df2.head()}")
    else:
        print("❌ 无数据返回")

if __name__ == "__main__":
    asyncio.run(debug_fetch())
