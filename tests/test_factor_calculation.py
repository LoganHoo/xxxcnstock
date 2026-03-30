import pytest
import polars as pl
from core.factor_engine import FactorEngine

@pytest.fixture
def test_data():
    """创建测试数据"""
    data = {
        'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07', '2024-01-08', '2024-01-09', '2024-01-10', '2024-01-11', '2024-01-12', '2024-01-13', '2024-01-14', '2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19', '2024-01-20', '2024-01-21', '2024-01-22', '2024-01-23', '2024-01-24', '2024-01-25', '2024-01-26', '2024-01-27', '2024-01-28', '2024-01-29', '2024-01-30'],
        'open': [10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9],
        'close': [10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 13.0, 13.1, 13.2, 13.3, 13.4],
        'high': [10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 13.0, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6],
        'low': [9.9, 10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 11.0, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8],
        'volume': [1000000, 1200000, 1500000, 1800000, 2000000, 2200000, 2500000, 2800000, 3000000, 3200000, 3500000, 3800000, 4000000, 4200000, 4500000, 4800000, 5000000, 5200000, 5500000, 5800000, 6000000, 6200000, 6500000, 6800000, 7000000, 7200000, 7500000, 7800000, 8000000, 8200000]
    }
    df = pl.DataFrame(data)
    return df

def test_factor_engine_initialization():
    """测试因子引擎初始化"""
    engine = FactorEngine()
    assert engine is not None, "因子引擎初始化失败"

def test_calculate_factor(test_data):
    """测试计算单个因子"""
    engine = FactorEngine()
    # 测试计算MA因子
    result = engine.calculate_factor(test_data, 'ma_trend')
    assert 'factor_ma_trend' in result.columns, "缺少 factor_ma_trend 列"
    assert len(result) == len(test_data), "数据长度不匹配"

def test_calculate_all_factors(test_data):
    """测试计算多个因子"""
    engine = FactorEngine()
    # 测试计算所有因子
    result = engine.calculate_all_factors(test_data, factor_names=['ma_trend', 'macd'])
    assert 'factor_ma_trend' in result.columns, "缺少 factor_ma_trend 列"
    assert 'factor_macd' in result.columns, "缺少 factor_macd 列"
    assert len(result) == len(test_data), "数据长度不匹配"

def test_list_factors():
    """测试列出因子"""
    engine = FactorEngine()
    factors = engine.list_factors()
    assert len(factors) > 0, "因子列表为空"
    assert all('name' in factor for factor in factors), "因子缺少名称"

def test_get_factor_info():
    """测试获取因子信息"""
    engine = FactorEngine()
    info = engine.get_factor_info('ma_trend')
    assert info is not None, "因子信息不存在"
    assert 'name' in info, "因子信息缺少名称"
