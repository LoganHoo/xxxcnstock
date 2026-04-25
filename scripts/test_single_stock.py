#!/usr/bin/env python3
"""
测试单只股票数据采集
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import asyncio
from datetime import datetime, timedelta
from services.data_service.datasource.manager import DataSourceManager

async def test_single_stock():
    """测试单只股票"""
    print("=" * 60)
    print("单只股票采集测试")
    print("=" * 60)
    
    # 初始化数据源管理器
    manager = DataSourceManager()
    
    # 测试股票
    test_codes = ['600000', '000001', '300001']
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n测试时间范围: {start_date} 至 {end_date}")
    print(f"当前数据源: {manager.current_source}")
    print()
    
    for code in test_codes:
        print(f"\n测试股票: {code}")
        print("-" * 40)
        
        try:
            df = await manager.fetch_kline(code, start_date, end_date)
            
            if df.empty:
                print(f"  ❌ 无数据返回")
            else:
                print(f"  ✅ 成功! 获取到 {len(df)} 条数据")
                print(f"  当前数据源: {manager.current_source}")
                print(f"  数据列: {list(df.columns)}")
                print(f"  数据预览:")
                print(df.head(3).to_string())
                
        except Exception as e:
            print(f"  ❌ 异常: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_single_stock())
