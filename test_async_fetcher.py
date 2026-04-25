#!/usr/bin/env python3
"""测试异步K线数据获取器"""
import sys
sys.path.insert(0, '.')

import asyncio
from pathlib import Path
from services.data_service.fetchers.async_kline_fetcher import AsyncKlineFetcher, AsyncConfig

async def test_async_fetcher():
    print("=" * 60)
    print("🧪 测试异步K线数据获取器")
    print("=" * 60)

    # 创建异步获取器
    config = AsyncConfig(
        max_concurrent=5,  # 测试时使用较低的并发数
        semaphore_value=5,
        batch_size=10,
        batch_pause=1.0,
        request_delay=0.2,
        min_kline_rows=5  # 降低最小行数要求以便测试
    )
    fetcher = AsyncKlineFetcher(config)

    # 测试股票代码
    test_codes = ['000001', '000002', '000333', '000858', '600519']
    kline_dir = Path("data/kline")

    print(f"\n📊 测试采集 {len(test_codes)} 只股票")
    print(f"📁 保存目录: {kline_dir}")
    print(f"🔧 并发数: {config.max_concurrent}")

    # 执行采集
    results = await fetcher.fetch_all(
        codes=test_codes,
        kline_dir=kline_dir,
        days=365,  # 采集最近1年数据
        filter_delisted=True
    )

    print("\n" + "=" * 60)
    print("📊 采集结果")
    print("=" * 60)
    print(f"✅ 成功: {results['success']}")
    print(f"⏭️  跳过: {results['skipped']}")
    print(f"❌ 失败: {results['failed']}")
    print(f"📈 总行数: {results['total_rows']}")

    # 显示详细结果
    print("\n📋 详细结果:")
    for result in results['results']:
        status_icon = "✅" if result.success else "❌"
        print(f"  {status_icon} {result.code}: {result.status} ({result.rows} 行)")
        if result.error:
            print(f"     错误: {result.error}")

if __name__ == "__main__":
    asyncio.run(test_async_fetcher())
