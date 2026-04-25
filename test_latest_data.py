#!/usr/bin/env python3
"""测试数据源最新数据"""
import sys
sys.path.insert(0, '.')

import asyncio
from datetime import datetime
from services.data_service.datasource.manager import get_datasource_manager

async def test_latest_data():
    print("=" * 60)
    print("🔍 测试数据源最新数据")
    print("=" * 60)

    # 初始化数据源
    ds = get_datasource_manager()
    ds.initialize()

    # 测试获取股票列表
    print("\n📊 获取股票列表...")
    stocks = await ds.fetch_stock_list()
    if stocks is not None and len(stocks) > 0:
        print(f"✅ 获取到 {len(stocks)} 只股票")
        sample_codes = stocks['code'].head(5).tolist()
        print(f"\n📝 前5只股票: {sample_codes}")

        # 测试获取单只股票最新K线
        print("\n📈 测试获取K线数据...")
        test_code = sample_codes[0]
        print(f"测试股票: {test_code}")

        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
        kline = await ds.fetch_kline(test_code, start_date=start_date, end_date=end_date)
        if kline is not None and len(kline) > 0:
            print(f"✅ 获取到 {len(kline)} 条K线")
            print(f"\n最新数据:")
            print(kline.tail(3).to_string())

            if 'date' in kline.columns:
                latest_date = kline['date'].max()
                print(f"\n📅 数据源最新日期: {latest_date}")
        else:
            print("❌ 未获取到K线数据")
    else:
        print("❌ 未获取到股票列表")

if __name__ == "__main__":
    asyncio.run(test_latest_data())
