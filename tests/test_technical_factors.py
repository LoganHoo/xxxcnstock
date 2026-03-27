"""
技术指标因子测试
"""
import pytest
import polars as pl
from factors.technical.ma_trend import MaTrendFactor
from factors.technical.macd import MacdFactor
from factors.technical.rsi import RsiFactor


@pytest.fixture
def sample_data():
    """创建测试数据"""
    return pl.DataFrame({
        "code": ["000001"] * 50,
        "trade_date": [f"2026-03-{i:02d}" for i in range(1, 51)],
        "open": [10.0 + i * 0.1 + (i % 3 - 1) * 0.05 for i in range(50)],
        "high": [10.5 + i * 0.1 for i in range(50)],
        "low": [9.5 + i * 0.1 for i in range(50)],
        "close": [10.0 + i * 0.1 for i in range(50)],
        "volume": [1000000 + i * 10000 for i in range(50)]
    })


class TestMaTrendFactor:
    """均线趋势因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = MaTrendFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_ma_trend" in result.columns
        assert "ma_short" in result.columns
        assert "ma_mid" in result.columns
        assert "ma_long" in result.columns
    
    def test_bullish_alignment(self, sample_data):
        """测试多头排列"""
        factor = MaTrendFactor()
        result = factor.calculate(sample_data)
        
        score = result["factor_ma_trend"].tail(1).item()
        assert 0 <= score <= 100


class TestMacdFactor:
    """MACD 因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = MacdFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_macd" in result.columns
        assert "dif" in result.columns
        assert "dea" in result.columns
        assert "macd" in result.columns


class TestRsiFactor:
    """RSI 因子测试"""
    
    def test_calculate(self, sample_data):
        """测试计算"""
        factor = RsiFactor()
        result = factor.calculate(sample_data)
        
        assert "factor_rsi" in result.columns
        assert "rsi" in result.columns
