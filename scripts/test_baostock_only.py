#!/usr/bin/env python3
"""
Baostock 数据源单独测试
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from services.data_service.datasource.providers import BaostockProvider

async def test_baostock():
    """测试Baostock数据源"""
    print("=" * 70)
    print("Baostock 数据源单独测试")
    print("=" * 70)
    
    # 初始化Baostock提供者
    provider = BaostockProvider()
    print(f"\n提供者名称: {provider.name}")
    print(f"提供者状态: {provider.status}")
    
    # 测试股票列表
    test_codes = [
        '600000',  # 浦发银行 - 上海主板
        '000001',  # 平安银行 - 深圳主板
        '300001',  # 特锐德 - 创业板
        '688001',  # 华兴源创 - 科创板
        '000002',  # 万科A
    ]
    
    # 测试日期范围
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    print(f"\n测试时间范围: {start_date} 至 {end_date}")
    print("-" * 70)
    
    results = []
    
    for code in test_codes:
        print(f"\n测试股票: {code}")
        try:
            df = await provider.fetch_kline(code, start_date, end_date)
            
            if df.empty:
                print(f"  ⚠️  无数据返回")
                results.append((code, False, 0, "无数据"))
            else:
                print(f"  ✅ 成功! 获取到 {len(df)} 条数据")
                print(f"  数据列: {list(df.columns)}")
                print(f"  日期范围: {df['date'].min()} 至 {df['date'].max()}")
                results.append((code, True, len(df), "成功"))
                
        except Exception as e:
            print(f"  ❌ 异常: {e}")
            results.append((code, False, 0, f"异常: {e}"))
    
    # 汇总结果
    print("\n" + "=" * 70)
    print("测试结果汇总")
    print("=" * 70)
    
    success_count = sum(1 for r in results if r[1])
    fail_count = len(results) - success_count
    
    print(f"\n总计: {len(results)} 只股票")
    print(f"  ✅ 成功: {success_count}")
    print(f"  ❌ 失败: {fail_count}")
    
    print("\n详细结果:")
    for code, success, count, msg in results:
        status = "✅" if success else "❌"
        print(f"  {status} {code}: {msg} ({count}条)")
    
    print("\n" + "=" * 70)
    
    # 登出
    provider._logout()
    print("Baostock 测试完成!")

if __name__ == "__main__":
    asyncio.run(test_baostock())
