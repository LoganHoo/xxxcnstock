import pytest
import pandas as pd
import numpy as np
from services.stock_service.filters.volume_price import VolumePriceFilter


def test_volume_price_filter_init():
    """测试量价筛选器初始化"""
    f = VolumePriceFilter()
    assert f is not None


def test_calculate_rsi():
    """测试RSI计算"""
    f = VolumePriceFilter()
    
    # 模拟价格上涨序列
    prices = pd.Series([10, 10.5, 11, 10.8, 11.2, 11.5, 11.3, 11.8, 12, 11.9, 
                        12.2, 12.5, 12.3, 12.8, 13, 13.2, 13.5, 13.3, 13.8, 14])
    rsi = f.calculate_rsi(prices)
    
    assert 0 <= rsi <= 100


def test_calculate_macd():
    """测试MACD计算"""
    f = VolumePriceFilter()
    
    prices = pd.Series([10, 10.5, 11, 10.8, 11.2, 11.5, 11.3, 11.8, 12, 11.9,
                        12.2, 12.5, 12.3, 12.8, 13, 13.2, 13.5, 13.3, 13.8, 14])
    macd, signal, hist = f.calculate_macd(prices)
    
    assert macd is not None
    assert signal is not None


def test_calculate_score_with_valid_data():
    """测试有效数据的评分计算"""
    f = VolumePriceFilter()
    
    # 创建模拟K线数据
    np.random.seed(42)
    n = 60
    dates = pd.date_range('2024-01-01', periods=n, freq='D')
    prices = 10 + np.cumsum(np.random.randn(n) * 0.1)
    volumes = 1000000 + np.random.randint(0, 500000, n)
    
    kline_df = pd.DataFrame({
        'date': dates,
        'open': prices,
        'close': prices + np.random.randn(n) * 0.05,
        'high': prices + 0.2,
        'low': prices - 0.2,
        'volume': volumes
    })
    
    score = f.calculate_score(kline_df)
    assert 0 <= score <= 100
