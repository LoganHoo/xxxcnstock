"""
K线形态识别测试脚本
"""
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from patterns import PatternEngine, SignalType, PatternStrength


def create_test_data():
    """创建测试K线数据"""
    dates = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(20, 0, -1)
    ]
    
    test_cases = {
        "涨停板": pl.DataFrame({
            "trade_date": dates,
            "open": [10.0] * 20,
            "high": [11.0] * 20,
            "low": [10.0] * 20,
            "close": [11.0] * 20,
            "volume": [1000000] * 20
        }),
        "锤子线": pl.DataFrame({
            "trade_date": dates,
            "open": [10.0, 10.2, 10.1, 10.0, 9.8, 9.5, 9.3, 9.0, 8.8, 8.5,
                     8.3, 8.0, 7.8, 7.5, 7.3, 7.0, 6.8, 6.5, 6.3, 6.0],
            "high": [10.5, 10.3, 10.2, 10.1, 9.9, 9.6, 9.4, 9.1, 8.9, 8.6,
                     8.4, 8.1, 7.9, 7.6, 7.4, 7.1, 6.9, 6.6, 6.4, 6.1],
            "low": [9.8, 10.0, 9.9, 9.7, 9.5, 9.2, 9.0, 8.7, 8.5, 8.2,
                    8.0, 7.7, 7.5, 7.2, 7.0, 6.7, 6.5, 6.2, 6.0, 5.0],
            "close": [10.2, 10.1, 10.0, 9.8, 9.6, 9.3, 9.1, 8.8, 8.6, 8.3,
                      8.1, 7.8, 7.6, 7.3, 7.1, 6.8, 6.6, 6.3, 6.1, 6.0],
            "volume": [1000000] * 20
        }),
        "看涨吞没": pl.DataFrame({
            "trade_date": dates,
            "open": [10.0, 10.2, 10.1, 10.0, 9.8, 9.5, 9.3, 9.0, 8.8, 8.5,
                     8.3, 8.0, 7.8, 7.5, 7.3, 7.0, 6.8, 6.5, 6.3, 6.0],
            "high": [10.5, 10.3, 10.2, 10.1, 9.9, 9.6, 9.4, 9.1, 8.9, 8.6,
                     8.4, 8.1, 7.9, 7.6, 7.4, 7.1, 6.9, 6.6, 6.4, 7.0],
            "low": [9.8, 10.0, 9.9, 9.7, 9.5, 9.2, 9.0, 8.7, 8.5, 8.2,
                    8.0, 7.7, 7.5, 7.2, 7.0, 6.7, 6.5, 6.2, 6.0, 5.8],
            "close": [10.2, 10.1, 10.0, 9.8, 9.6, 9.3, 9.1, 8.8, 8.6, 8.3,
                      8.1, 7.8, 7.6, 7.3, 7.1, 6.8, 6.6, 6.3, 6.1, 6.8],
            "volume": [1000000] * 20
        }),
        "十字星": pl.DataFrame({
            "trade_date": dates,
            "open": [10.0, 10.2, 10.1, 10.0, 9.8, 9.5, 9.3, 9.0, 8.8, 8.5,
                     8.3, 8.0, 7.8, 7.5, 7.3, 7.0, 6.8, 6.5, 6.3, 6.0],
            "high": [10.5, 10.3, 10.2, 10.1, 9.9, 9.6, 9.4, 9.1, 8.9, 8.6,
                     8.4, 8.1, 7.9, 7.6, 7.4, 7.1, 6.9, 6.6, 6.4, 6.2],
            "low": [9.8, 10.0, 9.9, 9.7, 9.5, 9.2, 9.0, 8.7, 8.5, 8.2,
                    8.0, 7.7, 7.5, 7.2, 7.0, 6.7, 6.5, 6.2, 6.0, 5.8],
            "close": [10.2, 10.1, 10.0, 9.8, 9.6, 9.3, 9.1, 8.8, 8.6, 8.3,
                      8.1, 7.8, 7.6, 7.3, 7.1, 6.8, 6.6, 6.3, 6.1, 6.0],
            "volume": [1000000] * 20
        }),
        "上升窗口": pl.DataFrame({
            "trade_date": dates,
            "open": [10.0, 10.2, 10.1, 10.0, 9.8, 9.5, 9.3, 9.0, 8.8, 8.5,
                     8.3, 8.0, 7.8, 7.5, 7.3, 7.0, 6.8, 6.5, 6.3, 6.0],
            "high": [10.5, 10.3, 10.2, 10.1, 9.9, 9.6, 9.4, 9.1, 8.9, 8.6,
                     8.4, 8.1, 7.9, 7.6, 7.4, 7.1, 6.9, 6.6, 6.4, 6.5],
            "low": [9.8, 10.0, 9.9, 9.7, 9.5, 9.2, 9.0, 8.7, 8.5, 8.2,
                    8.0, 7.7, 7.5, 7.2, 7.0, 6.7, 6.5, 6.2, 6.0, 6.4],
            "close": [10.2, 10.1, 10.0, 9.8, 9.6, 9.3, 9.1, 8.8, 8.6, 8.3,
                      8.1, 7.8, 7.6, 7.3, 7.1, 6.8, 6.6, 6.3, 6.1, 6.4],
            "volume": [1000000] * 20
        }),
    }
    
    return test_cases


def test_pattern_engine():
    """测试形态识别引擎"""
    print("=" * 60)
    print("K线形态识别测试")
    print("=" * 60)
    
    engine = PatternEngine()
    
    print("\n可用形态列表:")
    patterns = engine.list_available_patterns()
    for p in patterns:
        print(f"  - {p['name']}: {p['description']} (需要{p['min_periods']}根K线)")
    
    print(f"\n共 {len(patterns)} 种形态")
    
    test_cases = create_test_data()
    
    print("\n" + "=" * 60)
    print("形态识别测试结果")
    print("=" * 60)
    
    for name, df in test_cases.items():
        print(f"\n【{name}】")
        
        result = engine.analyze(df)
        
        print(f"  综合信号: {result['overall_signal']}")
        print(f"  信号强度: {result['overall_strength']}")
        print(f"  看涨得分: {result['bullish_score']:.2f}")
        print(f"  看跌得分: {result['bearish_score']:.2f}")
        print(f"  发现形态数: {result['total_patterns_found']}")
        
        if result['bullish_patterns']:
            print("  看涨形态:")
            for p in result['bullish_patterns']:
                print(f"    - {p['name']}: {p['description']} (置信度: {p['confidence']:.2f})")
        
        if result['bearish_patterns']:
            print("  看跌形态:")
            for p in result['bearish_patterns']:
                print(f"    - {p['name']}: {p['description']} (置信度: {p['confidence']:.2f})")
        
        if result['neutral_patterns']:
            print("  中性形态:")
            for p in result['neutral_patterns']:
                print(f"    - {p['name']}: {p['description']} (置信度: {p['confidence']:.2f})")


def test_single_pattern():
    """测试单个形态识别"""
    print("\n" + "=" * 60)
    print("单个形态识别测试")
    print("=" * 60)
    
    engine = PatternEngine()
    
    dates = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(5, 0, -1)
    ]
    
    df = pl.DataFrame({
        "trade_date": dates,
        "open": [10.0, 10.2, 10.1, 10.0, 9.8],
        "high": [10.5, 10.3, 10.2, 10.1, 9.9],
        "low": [9.8, 10.0, 9.9, 9.7, 9.5],
        "close": [10.2, 10.1, 10.0, 9.8, 9.6],
        "volume": [1000000] * 5
    })
    
    pattern_names = ["doji", "hammer", "bullish_engulfing", "limit_up"]
    
    for pattern_name in pattern_names:
        result = engine.detect_single_pattern(df, pattern_name)
        if result:
            print(f"\n{pattern_name}:")
            print(f"  信号: {result.signal.value}")
            print(f"  强度: {result.strength.value}")
            print(f"  置信度: {result.confidence:.2f}")
            print(f"  描述: {result.description}")
        else:
            print(f"\n{pattern_name}: 未检测到")


def test_real_data():
    """测试真实数据"""
    print("\n" + "=" * 60)
    print("真实数据测试")
    print("=" * 60)
    
    import os
    data_path = "/Volumes/Xdata/workstation/xxxcnstock/data/kline/000001.parquet"
    
    if os.path.exists(data_path):
        df = pl.read_parquet(data_path)
        
        df = df.rename({
            "trade_date": "trade_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        })
        
        df = df.tail(30)
        
        engine = PatternEngine()
        result = engine.analyze(df)
        
        print(f"\n股票: 000001")
        print(f"综合信号: {result['overall_signal']}")
        print(f"信号强度: {result['overall_strength']}")
        print(f"看涨得分: {result['bullish_score']:.2f}")
        print(f"看跌得分: {result['bearish_score']:.2f}")
        
        if result['bullish_patterns']:
            print("\n看涨形态:")
            for p in result['bullish_patterns']:
                print(f"  - {p['name']}: {p['description']}")
        
        if result['bearish_patterns']:
            print("\n看跌形态:")
            for p in result['bearish_patterns']:
                print(f"  - {p['name']}: {p['description']}")
    else:
        print(f"数据文件不存在: {data_path}")


if __name__ == "__main__":
    test_pattern_engine()
    test_single_pattern()
    test_real_data()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
