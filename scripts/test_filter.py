"""
过滤器测试脚本
验证过滤系统的功能
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

import filters
from filters.filter_engine import FilterEngine


def create_test_stock_list() -> pl.DataFrame:
    """创建测试股票列表"""
    today = datetime.now().strftime("%Y-%m-%d")
    old_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
    recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    data = {
        "code": [
            "000001", "000002", "000003", "000004", "000005",
            "000006", "000007", "000008", "000009", "000010",
            "000011", "000012", "000013", "000014", "000015"
        ],
        "name": [
            "平安银行", "万科A", "ST国农", "*ST金钰", "国农科技",
            "退市海润", "暂停上市A", "正常股票A", "正常股票B", "正常股票C",
            "小市值A", "中市值B", "大市值C", "停牌股票", "低价股"
        ],
        "list_date": [
            old_date, old_date, old_date, old_date, old_date,
            old_date, old_date, old_date, recent_date, old_date,
            old_date, old_date, old_date, old_date, old_date
        ],
        "market_cap": [
            300_000_000_000, 200_000_000_000, 5_000_000_000, 3_000_000_000, 10_000_000_000,
            1_000_000_000, 2_000_000_000, 50_000_000_000, 80_000_000_000, 100_000_000_000,
            2_000_000_000, 30_000_000_000, 500_000_000_000, 20_000_000_000, 15_000_000_000
        ],
        "close": [
            15.5, 12.3, 5.2, 1.5, 25.8,
            0.8, 2.1, 18.5, 22.0, 35.6,
            8.5, 45.2, 88.0, 10.0, 1.2
        ],
        "volume": [
            50_000_000, 80_000_000, 5_000_000, 1_000_000, 20_000_000,
            100_000, 500_000, 30_000_000, 25_000_000, 40_000_000,
            500_000, 15_000_000, 100_000_000, 0, 2_000_000
        ],
        "trade_status": [
            "正常", "正常", "正常", "正常", "正常",
            "正常", "正常", "正常", "正常", "正常",
            "正常", "正常", "正常", "停牌", "正常"
        ]
    }
    
    return pl.DataFrame(data)


def test_individual_filters():
    """测试单个过滤器"""
    print("\n" + "=" * 60)
    print("测试单个过滤器")
    print("=" * 60)
    
    stock_list = create_test_stock_list()
    print(f"\n原始股票列表: {len(stock_list)} 只")
    
    from filters.stock_filter import STFilter, NewStockFilter, DelistingFilter
    from filters.market_filter import MarketCapFilter, SuspensionFilter, PriceFilter
    
    filters_to_test = [
        ("ST过滤器", STFilter(params={"enabled": True})),
        ("新股过滤器", NewStockFilter(params={"enabled": True, "min_listing_days": 60})),
        ("退市风险过滤器", DelistingFilter(params={"enabled": True})),
        ("市值过滤器", MarketCapFilter(params={"enabled": True, "min_market_cap": 5_000_000_000})),
        ("停牌过滤器", SuspensionFilter(params={"enabled": True})),
        ("价格过滤器", PriceFilter(params={"enabled": True, "min_price": 2.0, "max_price": 300.0})),
    ]
    
    for name, filter_instance in filters_to_test:
        result = filter_instance.filter(stock_list.clone())
        removed = len(stock_list) - len(result)
        print(f"  {name}: {len(stock_list)} -> {len(result)} (移除 {removed})")


def test_filter_engine():
    """测试过滤引擎"""
    print("\n" + "=" * 60)
    print("测试过滤引擎")
    print("=" * 60)
    
    stock_list = create_test_stock_list()
    print(f"\n原始股票列表: {len(stock_list)} 只")
    
    engine = FilterEngine()
    
    print("\n已加载过滤器:")
    for f in engine.list_filters():
        status = "✅" if f["enabled"] else "❌"
        print(f"  {status} {f['name']}: {f['description']}")
    
    print("\n应用过滤器...")
    filtered = engine.apply_filters(stock_list)
    
    print(f"\n过滤结果: {len(stock_list)} -> {len(filtered)}")
    
    print("\n过滤后股票列表:")
    print(filtered.select(["code", "name", "market_cap", "close"]))


def test_filter_stats():
    """测试过滤器统计"""
    print("\n" + "=" * 60)
    print("测试过滤器统计")
    print("=" * 60)
    
    stock_list = create_test_stock_list()
    
    engine = FilterEngine()
    stats = engine.get_filter_stats(stock_list)
    
    print(f"\n原始股票数: {stats['original_count']}")
    print(f"最终股票数: {stats['final_count']}")
    print(f"总共移除: {stats['total_removed']}")
    
    print("\n各过滤器统计:")
    for f in stats["filters"]:
        if "error" in f:
            print(f"  {f['name']}: 错误 - {f['error']}")
        else:
            print(f"  {f['name']}: {f['before']} -> {f['after']} (移除 {f['removed']})")


def test_enable_disable():
    """测试启用/禁用过滤器"""
    print("\n" + "=" * 60)
    print("测试启用/禁用过滤器")
    print("=" * 60)
    
    engine = FilterEngine()
    
    print("\n初始状态:")
    for f in engine.list_filters():
        status = "✅" if f["enabled"] else "❌"
        print(f"  {status} {f['name']}")
    
    print("\n禁用市值过滤器...")
    engine.disable_filter("market_cap_filter")
    
    print("\n禁用后状态:")
    for f in engine.list_filters():
        status = "✅" if f["enabled"] else "❌"
        print(f"  {status} {f['name']}")
    
    print("\n重新启用市值过滤器...")
    engine.enable_filter("market_cap_filter")
    
    print("\n启用后状态:")
    for f in engine.list_filters(enabled_only=True):
        print(f"  ✅ {f['name']}")


if __name__ == "__main__":
    print("=" * 60)
    print("过滤器系统测试")
    print("=" * 60)
    
    test_individual_filters()
    test_filter_engine()
    test_filter_stats()
    test_enable_disable()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
