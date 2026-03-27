"""
过滤器综合测试脚本
验证所有过滤器的功能
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from filters.filter_engine import FilterEngine


def create_comprehensive_test_data() -> pl.DataFrame:
    """创建综合测试数据"""
    today = datetime.now().strftime("%Y-%m-%d")
    old_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
    
    data = {
        "code": [f"00000{i:02d}" for i in range(1, 31)],
        "name": [
            "平安银行", "万科A", "ST国农", "*ST金钰", "国农科技",
            "退市海润", "减持计划A", "正常股票A", "正常股票B", "正常股票C",
            "业绩亏损A", "业绩暴雷B", "小市值A", "中市值B", "大市值C",
            "停牌股票", "低价股A", "高价股B", "低换手A", "高换手B",
            "量比异常A", "量比异常B", "三连板A", "一字板A", "前期大涨A",
            "正常D", "正常E", "正常F", "正常G", "正常H"
        ],
        "list_date": [old_date] * 30,
        "market_cap": [
            300_000_000_000, 200_000_000_000, 5_000_000_000, 3_000_000_000, 10_000_000_000,
            1_000_000_000, 50_000_000_000, 80_000_000_000, 100_000_000_000, 150_000_000_000,
            20_000_000_000, 25_000_000_000, 1_000_000_000, 30_000_000_000, 500_000_000_000,
            20_000_000_000, 15_000_000_000, 80_000_000_000, 40_000_000_000, 60_000_000_000,
            35_000_000_000, 45_000_000_000, 25_000_000_000, 55_000_000_000, 70_000_000_000,
            50_000_000_000, 55_000_000_000, 60_000_000_000, 65_000_000_000, 70_000_000_000
        ],
        "float_market_cap": [
            150_000_000_000, 100_000_000_000, 2_000_000_000, 1_000_000_000, 5_000_000_000,
            500_000_000, 25_000_000_000, 40_000_000_000, 50_000_000_000, 75_000_000_000,
            10_000_000_000, 12_000_000_000, 500_000_000, 15_000_000_000, 250_000_000_000,
            10_000_000_000, 7_000_000_000, 40_000_000_000, 20_000_000_000, 30_000_000_000,
            17_000_000_000, 22_000_000_000, 12_000_000_000, 27_000_000_000, 35_000_000_000,
            25_000_000_000, 27_000_000_000, 30_000_000_000, 32_000_000_000, 35_000_000_000
        ],
        "close": [
            15.5, 12.3, 5.2, 1.5, 25.8,
            0.8, 18.5, 22.0, 35.6, 45.0,
            12.0, 15.0, 3.0, 28.0, 88.0,
            10.0, 1.2, 150.0, 20.0, 30.0,
            25.0, 35.0, 18.0, 40.0, 55.0,
            22.0, 24.0, 26.0, 28.0, 30.0
        ],
        "volume": [
            50_000_000, 80_000_000, 5_000_000, 1_000_000, 20_000_000,
            100_000, 30_000_000, 25_000_000, 40_000_000, 35_000_000,
            15_000_000, 18_000_000, 500_000, 20_000_000, 100_000_000,
            0, 2_000_000, 50_000_000, 5_000_000, 80_000_000,
            10_000_000, 60_000_000, 25_000_000, 5_000_000, 40_000_000,
            30_000_000, 32_000_000, 35_000_000, 38_000_000, 40_000_000
        ],
        "volume_ratio": [
            2.5, 1.8, 0.5, 0.3, 1.5,
            0.1, 2.0, 1.2, 1.8, 2.2,
            0.8, 1.0, 0.2, 1.5, 3.0,
            0.0, 0.4, 6.0, 0.6, 2.5,
            0.3, 8.0, 2.0, 0.1, 4.0,
            1.5, 1.6, 1.7, 1.8, 1.9
        ],
        "turnover_rate": [
            0.08, 0.12, 0.03, 0.02, 0.10,
            0.01, 0.15, 0.08, 0.12, 0.09,
            0.04, 0.06, 0.01, 0.07, 0.25,
            0.00, 0.02, 0.30, 0.03, 0.22,
            0.04, 0.35, 0.15, 0.02, 0.18,
            0.08, 0.09, 0.10, 0.11, 0.12
        ],
        "trade_status": ["正常"] * 15 + ["停牌"] + ["正常"] * 14,
        "net_profit": [
            10_000_000_000, 5_000_000_000, -500_000_000, -800_000_000, 1_000_000_000,
            -200_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000, 5_000_000_000,
            -100_000_000, -150_000_000, 200_000_000, 1_500_000_000, 10_000_000_000,
            800_000_000, 300_000_000, 2_000_000_000, 500_000_000, 1_200_000_000,
            600_000_000, 900_000_000, 700_000_000, 1_100_000_000, 1_800_000_000,
            1_000_000_000, 1_100_000_000, 1_200_000_000, 1_300_000_000, 1_400_000_000
        ],
        "profit_yoy": [
            0.15, 0.10, -0.30, -0.50, 0.08,
            -0.80, 0.12, 0.15, 0.20, 0.18,
            -0.60, -0.70, 0.05, 0.10, 0.25,
            0.08, 0.03, 0.15, 0.02, 0.12,
            0.06, 0.10, 0.08, 0.05, 0.20,
            0.10, 0.11, 0.12, 0.13, 0.14
        ],
        "ma_short": [
            16.0, 13.0, 5.5, 1.8, 26.0,
            1.0, 19.0, 23.0, 36.0, 46.0,
            13.0, 16.0, 3.5, 29.0, 90.0,
            10.5, 1.5, 155.0, 21.0, 31.0,
            26.0, 36.0, 19.0, 41.0, 57.0,
            23.0, 25.0, 27.0, 29.0, 31.0
        ],
        "ma_long": [
            15.0, 12.0, 5.0, 1.5, 25.0,
            1.2, 18.0, 22.0, 35.0, 44.0,
            12.5, 15.5, 3.0, 28.0, 88.0,
            10.0, 1.3, 152.0, 20.5, 30.5,
            25.5, 35.5, 18.5, 40.5, 56.0,
            22.0, 24.0, 26.0, 28.0, 30.0
        ],
        "ma20": [
            15.2, 12.2, 5.1, 1.6, 25.5,
            1.1, 18.3, 22.2, 35.5, 44.5,
            12.2, 15.3, 3.2, 28.2, 88.2,
            10.2, 1.4, 153.0, 20.2, 30.2,
            25.2, 35.2, 18.2, 40.2, 55.2,
            22.2, 24.2, 26.2, 28.2, 30.2
        ],
        "ma60": [
            14.8, 11.8, 4.8, 1.4, 24.8,
            1.3, 17.8, 21.8, 34.8, 43.8,
            12.8, 15.8, 3.8, 27.8, 87.8,
            10.8, 1.6, 154.0, 20.8, 30.8,
            25.8, 35.8, 18.8, 40.8, 56.8,
            22.8, 24.8, 26.8, 28.8, 30.8
        ],
        "macd_dif": [
            0.5, 0.3, -0.2, -0.3, 0.4,
            -0.5, 0.2, 0.3, 0.5, 0.4,
            -0.1, -0.2, 0.1, 0.3, 0.8,
            0.0, 0.1, 0.6, 0.2, 0.4,
            0.3, 0.5, 0.4, 0.2, 0.6,
            0.3, 0.35, 0.4, 0.45, 0.5
        ],
        "macd_dea": [
            0.4, 0.2, -0.1, -0.2, 0.3,
            -0.4, 0.1, 0.2, 0.4, 0.3,
            -0.2, -0.3, 0.05, 0.2, 0.6,
            0.0, 0.05, 0.5, 0.1, 0.3,
            0.2, 0.4, 0.3, 0.1, 0.5,
            0.25, 0.3, 0.35, 0.4, 0.45
        ],
        "continuous_limit_days": [
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 4, 1, 2,
            0, 0, 0, 0, 0
        ],
        "limit_up_type": [
            "", "", "", "", "",
            "", "", "", "", "",
            "", "", "", "", "",
            "", "", "", "", "",
            "", "", "", "一字板", "",
            "", "", "", "", ""
        ],
        "gain_pct_20d": [
            0.05, 0.08, -0.10, -0.15, 0.12,
            -0.30, 0.10, 0.15, 0.20, 0.18,
            -0.05, -0.08, 0.02, 0.10, 0.30,
            0.00, -0.02, 0.25, 0.03, 0.15,
            0.08, 0.12, 0.10, 0.05, 0.60,
            0.10, 0.11, 0.12, 0.13, 0.14
        ],
        "risk_flag": [
            "", "", "ST", "ST", "",
            "退市", "减持计划", "", "", "",
            "", "", "", "", "",
            "", "", "", "", "",
            "", "", "", "", "",
            "", "", "", "", ""
        ],
        "index_change_pct": [0.01] * 30,
    }
    
    return pl.DataFrame(data)


def test_all_filters():
    """测试所有过滤器"""
    print("\n" + "=" * 70)
    print("过滤器综合测试")
    print("=" * 70)
    
    stock_list = create_comprehensive_test_data()
    print(f"\n原始股票列表: {len(stock_list)} 只")
    
    engine = FilterEngine()
    
    print("\n已加载过滤器:")
    for f in engine.list_filters():
        status = "✅" if f["enabled"] else "❌"
        print(f"  {status} {f['name']}: {f['description']}")
    
    print("\n应用过滤器...")
    filtered = engine.apply_filters(stock_list)
    
    print(f"\n过滤结果: {len(stock_list)} -> {len(filtered)}")
    
    if len(filtered) > 0:
        print("\n过滤后股票列表:")
        print(filtered.select(["code", "name", "close", "float_market_cap", "turnover_rate"]))


def test_filter_stats():
    """测试过滤器统计"""
    print("\n" + "=" * 70)
    print("过滤器统计")
    print("=" * 70)
    
    stock_list = create_comprehensive_test_data()
    
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


def test_individual_filter():
    """测试单个过滤器"""
    print("\n" + "=" * 70)
    print("测试单个过滤器")
    print("=" * 70)
    
    stock_list = create_comprehensive_test_data()
    
    from filters.fundamental_filter import PerformanceFilter
    from filters.liquidity_filter import VolumeRatioFilter, TurnoverRateFilter
    from filters.valuation_filter import FloatMarketCapFilter, PriceRangeFilter
    from filters.pattern_filter import OverHypedFilter
    
    filters_to_test = [
        ("业绩过滤器", PerformanceFilter(params={"enabled": True})),
        ("量比过滤器", VolumeRatioFilter(params={"enabled": True, "min_ratio": 1.0, "max_ratio": 5.0})),
        ("换手率过滤器", TurnoverRateFilter(params={"enabled": True, "min_rate": 0.05, "max_rate": 0.20})),
        ("流通市值过滤器", FloatMarketCapFilter(params={"enabled": True, "min_cap": 3_000_000_000, "max_cap": 30_000_000_000})),
        ("价格区间过滤器", PriceRangeFilter(params={"enabled": True, "min_price": 5.0, "max_price": 80.0})),
        ("过度炒作过滤器", OverHypedFilter(params={"enabled": True, "max_limit_days": 3})),
    ]
    
    for name, filter_instance in filters_to_test:
        result = filter_instance.filter(stock_list.clone())
        removed = len(stock_list) - len(result)
        print(f"  {name}: {len(stock_list)} -> {len(result)} (移除 {removed})")


if __name__ == "__main__":
    print("=" * 70)
    print("过滤器系统测试")
    print("=" * 70)
    
    test_individual_filter()
    test_all_filters()
    test_filter_stats()
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)
