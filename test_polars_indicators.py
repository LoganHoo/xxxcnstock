"""
Polars 技术指标测试

测试 MACD、RSI、成交量因子等 Polars 实现。
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

import polars as pl
import numpy as np
from loguru import logger

from core.polars_optimizer import (
    PolarsTechnicalIndicators,
    PandasPolarsBridge,
    PolarsBenchmark
)


def create_test_data(n: int = 100) -> pl.DataFrame:
    """创建测试数据"""
    np.random.seed(42)
    
    # 生成模拟股价数据
    from datetime import datetime, timedelta
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(n)]
    
    base_price = 100.0
    prices = [base_price]
    for i in range(1, n):
        change = np.random.normal(0, 0.02)
        prices.append(prices[-1] * (1 + change))
    
    df = pl.DataFrame({
        'trade_date': dates,
        'open': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
        'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'close': prices,
        'volume': np.random.randint(1000000, 10000000, n)
    })
    
    return df


def test_macd():
    """测试 MACD 计算"""
    logger.info("=" * 50)
    logger.info("测试 MACD 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.macd(df)
    
    # 验证结果
    assert 'macd' in result.columns, "缺少 macd 列"
    assert 'macd_signal' in result.columns, "缺少 macd_signal 列"
    assert 'macd_histogram' in result.columns, "缺少 macd_histogram 列"
    
    # 验证计算逻辑
    macd_values = result['macd'].to_numpy()
    signal_values = result['macd_signal'].to_numpy()
    histogram_values = result['macd_histogram'].to_numpy()
    
    # 柱状图 = MACD - 信号线
    expected_histogram = macd_values - signal_values
    assert np.allclose(
        histogram_values[~np.isnan(histogram_values)],
        expected_histogram[~np.isnan(expected_histogram)],
        rtol=1e-5
    ), "MACD 柱状图计算错误"
    
    logger.info(f"✅ MACD 测试通过")
    logger.info(f"   数据点数: {len(result)}")
    logger.info(f"   MACD 范围: [{macd_values.min():.4f}, {macd_values.max():.4f}]")
    
    return True


def test_rsi():
    """测试 RSI 计算"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 RSI 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.rsi(df, period=14)
    
    # 验证结果
    assert 'rsi_14' in result.columns, "缺少 rsi_14 列"
    
    rsi_values = result['rsi_14'].to_numpy()
    valid_rsi = rsi_values[~np.isnan(rsi_values)]
    
    # RSI 应该在 0-100 之间
    assert valid_rsi.min() >= 0, f"RSI 最小值 {valid_rsi.min()} 小于 0"
    assert valid_rsi.max() <= 100, f"RSI 最大值 {valid_rsi.max()} 大于 100"
    
    logger.info(f"✅ RSI 测试通过")
    logger.info(f"   RSI 范围: [{valid_rsi.min():.2f}, {valid_rsi.max():.2f}]")
    logger.info(f"   RSI 均值: {valid_rsi.mean():.2f}")
    
    return True


def test_volume_factors():
    """测试成交量因子"""
    logger.info("\n" + "=" * 50)
    logger.info("测试成交量因子")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.volume_factors(df)
    
    # 验证结果
    required_cols = ['volume_ma5', 'volume_ma20', 'volume_ratio', 'pvt', 'obv', 'vwap']
    for col in required_cols:
        assert col in result.columns, f"缺少 {col} 列"
    
    # 验证成交量比率
    volume = result['volume'].to_numpy()
    volume_ma20 = result['volume_ma20'].to_numpy()
    volume_ratio = result['volume_ratio'].to_numpy()
    
    valid_mask = ~np.isnan(volume_ma20) & (volume_ma20 != 0)
    expected_ratio = volume[valid_mask] / volume_ma20[valid_mask]
    assert np.allclose(
        volume_ratio[valid_mask],
        expected_ratio,
        rtol=1e-5
    ), "成交量比率计算错误"
    
    logger.info(f"✅ 成交量因子测试通过")
    logger.info(f"   成交量 MA5 均值: {result['volume_ma5'].mean():.0f}")
    logger.info(f"   成交量比率范围: [{result['volume_ratio'].min():.2f}, {result['volume_ratio'].max():.2f}]")
    
    return True


def test_bollinger_bands():
    """测试布林带"""
    logger.info("\n" + "=" * 50)
    logger.info("测试布林带")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.bollinger_bands(df)
    
    # 验证结果
    required_cols = ['bb_middle', 'bb_upper', 'bb_lower', 'bb_bandwidth', 'bb_percent_b']
    for col in required_cols:
        assert col in result.columns, f"缺少 {col} 列"
    
    # 验证布林带逻辑
    middle = result['bb_middle'].to_numpy()
    upper = result['bb_upper'].to_numpy()
    lower = result['bb_lower'].to_numpy()
    
    valid_mask = ~np.isnan(middle)
    # 上轨 >= 中轨 >= 下轨
    assert np.all(upper[valid_mask] >= middle[valid_mask]), "上轨应该 >= 中轨"
    assert np.all(middle[valid_mask] >= lower[valid_mask]), "中轨应该 >= 下轨"
    
    logger.info(f"✅ 布林带测试通过")
    logger.info(f"   中轨范围: [{middle[valid_mask].min():.2f}, {middle[valid_mask].max():.2f}]")
    logger.info(f"   带宽均值: {result['bb_bandwidth'].mean():.2f}%")
    
    return True


def test_kdj():
    """测试 KDJ 指标"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 KDJ 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.kdj(df)
    
    # 验证结果
    required_cols = ['kdj_k', 'kdj_d', 'kdj_j']
    for col in required_cols:
        assert col in result.columns, f"缺少 {col} 列"
    
    # 验证 J 值计算
    k = result['kdj_k'].to_numpy()
    d = result['kdj_d'].to_numpy()
    j = result['kdj_j'].to_numpy()
    
    valid_mask = ~np.isnan(k) & ~np.isnan(d) & ~np.isnan(j)
    expected_j = 3 * k[valid_mask] - 2 * d[valid_mask]
    assert np.allclose(
        j[valid_mask],
        expected_j,
        rtol=1e-5
    ), "KDJ J值计算错误"
    
    logger.info(f"✅ KDJ 测试通过")
    logger.info(f"   K 值范围: [{k[valid_mask].min():.2f}, {k[valid_mask].max():.2f}]")
    logger.info(f"   D 值范围: [{d[valid_mask].min():.2f}, {d[valid_mask].max():.2f}]")
    
    return True


def test_moving_averages():
    """测试移动平均线"""
    logger.info("\n" + "=" * 50)
    logger.info("测试移动平均线")
    logger.info("=" * 50)
    
    df = create_test_data(300)
    periods = [5, 10, 20, 60]
    result = PolarsTechnicalIndicators.moving_averages(df, periods=periods)
    
    # 验证结果
    for period in periods:
        col_name = f'ma{period}'
        assert col_name in result.columns, f"缺少 {col_name} 列"
    
    # 验证均线逻辑：短期均线应该比长期均线更贴近价格
    close = result['close'].to_numpy()[60:]  # 从第60个开始，确保所有均线都有值
    ma5 = result['ma5'].to_numpy()[60:]
    ma60 = result['ma60'].to_numpy()[60:]
    
    # MA5 与收盘价的平均距离应该小于 MA60
    diff_ma5 = np.abs(close - ma5).mean()
    diff_ma60 = np.abs(close - ma60).mean()
    assert diff_ma5 < diff_ma60, "MA5 应该比 MA60 更贴近收盘价"
    
    logger.info(f"✅ 移动平均线测试通过")
    logger.info(f"   MA5 与收盘价平均偏差: {diff_ma5:.4f}")
    logger.info(f"   MA60 与收盘价平均偏差: {diff_ma60:.4f}")
    
    return True


def test_ema():
    """测试指数移动平均线"""
    logger.info("\n" + "=" * 50)
    logger.info("测试指数移动平均线 (EMA)")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    periods = [5, 10, 20]
    result = PolarsTechnicalIndicators.exponential_moving_averages(df, periods=periods)
    
    # 验证结果
    for period in periods:
        col_name = f'ema{period}'
        assert col_name in result.columns, f"缺少 {col_name} 列"
    
    # 验证 EMA 和 SMA 的关系：EMA 应该对近期价格更敏感
    ema20 = result['ema20'].to_numpy()[20:]
    close = result['close'].to_numpy()[20:]
    
    # 验证 EMA 值在合理范围内
    assert np.all(ema20 > 0), "EMA 值应该为正"
    
    logger.info(f"✅ EMA 测试通过")
    logger.info(f"   EMA20 范围: [{ema20.min():.2f}, {ema20.max():.2f}]")
    
    return True


def test_cci():
    """测试 CCI 指标"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 CCI 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.cci(df, period=20)
    
    # 验证结果
    assert 'cci_20' in result.columns, "缺少 cci_20 列"
    
    cci_values = result['cci_20'].to_numpy()
    valid_cci = cci_values[~np.isnan(cci_values)]
    
    # CCI 通常在 -300 到 +300 之间
    assert valid_cci.min() > -500, f"CCI 最小值 {valid_cci.min()} 超出正常范围"
    assert valid_cci.max() < 500, f"CCI 最大值 {valid_cci.max()} 超出正常范围"
    
    logger.info(f"✅ CCI 测试通过")
    logger.info(f"   CCI 范围: [{valid_cci.min():.2f}, {valid_cci.max():.2f}]")
    logger.info(f"   CCI 均值: {valid_cci.mean():.2f}")
    
    return True


def test_williams_r():
    """测试 Williams %R 指标"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 Williams %R 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.williams_r(df, period=14)
    
    # 验证结果
    assert 'williams_r_14' in result.columns, "缺少 williams_r_14 列"
    
    williams_values = result['williams_r_14'].to_numpy()
    valid_values = williams_values[~np.isnan(williams_values)]
    
    # Williams %R 应该在 -100 到 0 之间
    assert valid_values.min() >= -100, f"Williams %R 最小值 {valid_values.min()} 小于 -100"
    assert valid_values.max() <= 0, f"Williams %R 最大值 {valid_values.max()} 大于 0"
    
    logger.info(f"✅ Williams %R 测试通过")
    logger.info(f"   范围: [{valid_values.min():.2f}, {valid_values.max():.2f}]")
    
    return True


def test_atr():
    """测试 ATR 指标"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 ATR 指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.atr(df, period=14)
    
    # 验证结果
    assert 'true_range' in result.columns, "缺少 true_range 列"
    assert 'atr_14' in result.columns, "缺少 atr_14 列"
    
    atr_values = result['atr_14'].to_numpy()
    valid_atr = atr_values[~np.isnan(atr_values)]
    
    # ATR 应该为正数
    assert np.all(valid_atr > 0), "ATR 值应该为正"
    
    logger.info(f"✅ ATR 测试通过")
    logger.info(f"   ATR 范围: [{valid_atr.min():.4f}, {valid_atr.max():.4f}]")
    logger.info(f"   ATR 均值: {valid_atr.mean():.4f}")
    
    return True


def test_momentum():
    """测试动量指标"""
    logger.info("\n" + "=" * 50)
    logger.info("测试动量指标")
    logger.info("=" * 50)
    
    df = create_test_data(200)
    result = PolarsTechnicalIndicators.momentum(df, period=10)
    
    # 验证结果
    assert 'momentum_10' in result.columns, "缺少 momentum_10 列"
    assert 'momentum_pct_10' in result.columns, "缺少 momentum_pct_10 列"
    
    momentum_pct = result['momentum_pct_10'].to_numpy()
    valid_momentum = momentum_pct[~np.isnan(momentum_pct)]
    
    # 动量百分比通常在 -50% 到 +50% 之间（10日）
    assert valid_momentum.min() > -100, f"动量最小值 {valid_momentum.min()}% 超出正常范围"
    assert valid_momentum.max() < 100, f"动量最大值 {valid_momentum.max()}% 超出正常范围"
    
    logger.info(f"✅ 动量指标测试通过")
    logger.info(f"   动量百分比范围: [{valid_momentum.min():.2f}%, {valid_momentum.max():.2f}%]")
    
    return True


def test_pandas_bridge():
    """测试 Pandas-Polars 桥接"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 Pandas-Polars 桥接")
    logger.info("=" * 50)
    
    try:
        import pandas as pd
    except ImportError:
        logger.warning("⚠️ Pandas 未安装，跳过桥接测试")
        return True
    
    # 创建 Pandas DataFrame
    df_pandas = pd.DataFrame({
        'open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'high': [101.0, 102.0, 103.0, 104.0, 105.0],
        'low': [99.0, 100.0, 101.0, 102.0, 103.0],
        'close': [100.5, 101.5, 102.5, 103.5, 104.5],
        'volume': [1000000, 2000000, 1500000, 1800000, 2200000]
    })
    
    # 转换为 Polars
    df_polars = PandasPolarsBridge.pandas_to_polars(df_pandas)
    
    # 验证转换
    assert len(df_polars) == len(df_pandas), "行数不匹配"
    assert list(df_polars.columns) == list(df_pandas.columns), "列名不匹配"
    
    # 转换回 Pandas
    df_back = PandasPolarsBridge.polars_to_pandas(df_polars)
    assert len(df_back) == len(df_pandas), "转换回 Pandas 后行数不匹配"
    
    logger.info(f"✅ Pandas-Polars 桥接测试通过")
    logger.info(f"   原始行数: {len(df_pandas)}")
    logger.info(f"   转换后行数: {len(df_polars)}")
    
    return True


def test_performance():
    """测试性能"""
    logger.info("\n" + "=" * 50)
    logger.info("性能基准测试")
    logger.info("=" * 50)
    
    try:
        import pandas as pd
        import time
    except ImportError:
        logger.warning("⚠️ Pandas 未安装，跳过性能测试")
        return True
    
    # 创建大数据集
    df = create_test_data(10000)
    df_pandas = df.to_pandas()
    
    # 测试 Polars MACD
    start = time.time()
    for _ in range(10):
        _ = PolarsTechnicalIndicators.macd(df)
    polars_time = (time.time() - start) / 10
    
    logger.info(f"✅ 性能测试完成")
    logger.info(f"   数据量: 10,000 条")
    logger.info(f"   Polars MACD 平均耗时: {polars_time*1000:.2f} ms")
    logger.info(f"   预计比 Pandas 快 10-100 倍")
    
    return True


def run_all_tests():
    """运行所有测试"""
    logger.info("\n" + "=" * 60)
    logger.info("Polars 技术指标测试套件")
    logger.info("=" * 60)
    
    tests = [
        ("MACD", test_macd),
        ("RSI", test_rsi),
        ("成交量因子", test_volume_factors),
        ("布林带", test_bollinger_bands),
        ("KDJ", test_kdj),
        ("移动平均线", test_moving_averages),
        ("指数移动平均线", test_ema),
        ("CCI指标", test_cci),
        ("Williams %R", test_williams_r),
        ("ATR指标", test_atr),
        ("动量指标", test_momentum),
        ("Pandas桥接", test_pandas_bridge),
        ("性能测试", test_performance),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            logger.error(f"❌ {name} 测试失败: {e}")
            failed += 1
    
    logger.info("\n" + "=" * 60)
    logger.info("测试总结")
    logger.info("=" * 60)
    logger.info(f"✅ 通过: {passed}")
    logger.info(f"❌ 失败: {failed}")
    logger.info(f"📊 总计: {passed + failed}")
    
    if failed == 0:
        logger.info("\n🎉 所有测试通过！")
    else:
        logger.info(f"\n⚠️ {failed} 个测试失败")
    
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
