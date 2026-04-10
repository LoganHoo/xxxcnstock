#!/usr/bin/env python3
"""
测试脚本：验证000542是否被正确过滤
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
from datetime import date, timedelta

from filters.market_filter import DataFreshnessFilter
from filters.base_filter import FilterRegistry

def test_000542_filtering():
    """测试000542的过滤逻辑"""
    print("=" * 60)
    print("测试000542数据新鲜度过滤")
    print("=" * 60)
    
    # 1. 加载000542的数据
    kline_path = PROJECT_ROOT / "data/kline/000542.parquet"
    if not kline_path.exists():
        print(f"❌ 文件不存在: {kline_path}")
        return False
    
    df = pl.read_parquet(kline_path)
    print(f"\n✅ 成功加载000542数据")
    print(f"   行数: {len(df)}")
    print(f"   列名: {df.columns}")
    
    # 2. 检查trade_date
    latest_date = df["trade_date"].max()
    print(f"\n📊 最新交易日期: {latest_date}")
    
    # 3. 计算 cutoff_date (30天前)
    cutoff_date = (date.today() - timedelta(days=30)).isoformat()
    print(f"   截止日期 (30天前): {cutoff_date}")
    
    # 4. 检查是否应该被过滤
    should_be_filtered = latest_date < cutoff_date
    print(f"\n🔍 是否应该被过滤: {should_be_filtered}")
    
    if should_be_filtered:
        print(f"   ❌ 000542 已退市/数据过旧 (最新日期: {latest_date} < 截止日期: {cutoff_date})")
    else:
        print(f"   ✅ 000542 数据新鲜 (最新日期: {latest_date} >= 截止日期: {cutoff_date})")
    
    # 5. 测试过滤器
    print("\n" + "-" * 60)
    print("测试 DataFreshnessFilter 过滤器")
    print("-" * 60)
    
    # 创建测试数据（包含000542）
    test_data = pl.DataFrame({
        "code": ["000542", "000001", "000002"],
        "trade_date": [latest_date, "2026-04-09", "2026-04-09"],
        "open": [10.0, 10.0, 10.0],
        "close": [10.0, 10.0, 10.0],
        "high": [10.0, 10.0, 10.0],
        "low": [10.0, 10.0, 10.0],
        "volume": [1000000, 1000000, 1000000]
    })
    
    print(f"\n测试数据:")
    print(test_data)
    
    # 创建过滤器
    filter_params = {
        "enabled": True,
        "max_data_age_days": 30
    }
    filter_instance = DataFreshnessFilter(params=filter_params)
    
    print(f"\n过滤器配置:")
    print(f"  enabled: {filter_instance.enabled}")
    print(f"  max_data_age_days: {filter_instance.max_data_age_days}")
    
    # 执行过滤
    print(f"\n执行过滤...")
    filtered = filter_instance.filter(test_data)
    
    print(f"\n过滤结果:")
    print(f"  原始数量: {len(test_data)}")
    print(f"  过滤后数量: {len(filtered)}")
    print(f"  移除数量: {len(test_data) - len(filtered)}")
    
    print(f"\n过滤后的数据:")
    print(filtered)
    
    # 6. 检查000542是否还在
    if "000542" in filtered["code"].to_list():
        print(f"\n❌ 失败: 000542 仍然在过滤后的数据中!")
        return False
    else:
        print(f"\n✅ 成功: 000542 已被正确过滤!")
        return True

if __name__ == "__main__":
    success = test_000542_filtering()
    sys.exit(0 if success else 1)
