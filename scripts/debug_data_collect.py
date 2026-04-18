#!/usr/bin/env python3
"""
调试数据采集问题
"""
import sys
sys.path.insert(0, '.')

import asyncio
import baostock as bs
from datetime import datetime


def test_baostock_direct():
    """直接测试 baostock"""
    print("=" * 70)
    print("直接测试 baostock 数据源")
    print("=" * 70)
    
    # 登录
    lg = bs.login()
    print(f"登录结果: {lg.error_msg}")
    
    if lg.error_code != '0':
        print("❌ 登录失败")
        return False
    
    # 测试获取K线
    code = "002119"
    code_bs = f"sz.{code}"
    start_date = "2026-04-17"
    end_date = "2026-04-17"
    
    print(f"\n测试获取 {code} 的K线数据")
    print(f"日期范围: {start_date} ~ {end_date}")
    
    rs = bs.query_history_k_data_plus(
        code_bs,
        "date,code,open,high,low,close,volume,amount",
        start_date=start_date,
        end_date=end_date,
        frequency="d"
    )
    
    print(f"查询结果错误码: {rs.error_code}")
    print(f"查询结果错误信息: {rs.error_msg}")
    
    if rs.error_code == '0':
        count = 0
        while rs.next():
            row = rs.get_row_data()
            print(f"  数据: {row}")
            count += 1
        
        if count == 0:
            print("  ⚠️ 没有数据返回")
        else:
            print(f"  ✅ 成功获取 {count} 条数据")
    else:
        print(f"  ❌ 查询失败")
    
    bs.logout()
    return True


async def test_unified_fetcher():
    """测试统一获取器"""
    print("\n" + "=" * 70)
    print("测试 UnifiedFetcher")
    print("=" * 70)
    
    from services.data_service.fetchers.unified_fetcher import UnifiedFetcher
    
    fetcher = UnifiedFetcher()
    await fetcher.initialize()
    
    code = "002119"
    start_date = "2026-04-17"
    end_date = "2026-04-17"
    
    print(f"\n获取 {code} 的K线数据...")
    df = await fetcher.fetch_kline(code, start_date, end_date)
    
    if df.empty:
        print("❌ 返回空数据框")
    else:
        print(f"✅ 成功获取 {len(df)} 条数据")
        print(df)
    
    await fetcher.shutdown()


async def test_datasource_manager():
    """测试数据源管理器"""
    print("\n" + "=" * 70)
    print("测试 DataSourceManager")
    print("=" * 70)
    
    from services.data_service.datasource import get_datasource_manager
    
    manager = get_datasource_manager()
    await manager.initialize()
    
    code = "002119"
    start_date = "2026-04-17"
    end_date = "2026-04-17"
    
    print(f"\n获取 {code} 的K线数据...")
    df = await manager.fetch_kline(code, start_date, end_date)
    
    if df.empty:
        print("❌ 返回空数据框")
    else:
        print(f"✅ 成功获取 {len(df)} 条数据")
        print(df)
    
    await manager.shutdown()


def main():
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 25 + "数据采集调试" + " " * 31 + "║")
    print("╚" + "=" * 68 + "╝")
    
    # 测试1: 直接测试 baostock
    test_baostock_direct()
    
    # 测试2: 测试 UnifiedFetcher
    asyncio.run(test_unified_fetcher())
    
    # 测试3: 测试 DataSourceManager
    asyncio.run(test_datasource_manager())


if __name__ == "__main__":
    main()
