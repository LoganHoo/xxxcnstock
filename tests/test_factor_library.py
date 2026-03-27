"""
因子基类测试
"""
import pytest
import polars as pl
from core.factor_library import BaseFactor, FactorRegistry, register_factor


class TestBaseFactor:
    """BaseFactor 测试"""
    
    def test_normalize(self):
        """测试标准化函数"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("test", "test")
        
        assert factor.normalize(50, 0, 100) == 0.5
        assert factor.normalize(0, 0, 100) == 0.0
        assert factor.normalize(100, 0, 100) == 1.0
        assert factor.normalize(150, 0, 100) == 1.0
        assert factor.normalize(-50, 0, 100) == 0.0
    
    def test_get_score(self):
        """测试得分计算"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("test", "test")
        
        assert factor.get_score(50) == 50.0
        assert factor.get_score(0) == 0.0
        assert factor.get_score(100) == 100.0
    
    def test_get_factor_column_name(self):
        """测试因子列名"""
        class DummyFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor = DummyFactor("ma_trend", "technical")
        assert factor.get_factor_column_name() == "factor_ma_trend"


class TestFactorRegistry:
    """FactorRegistry 测试"""
    
    def test_register_and_get(self):
        """测试注册和获取"""
        @register_factor("test_factor")
        class TestFactor(BaseFactor):
            def calculate(self, data):
                return data
        
        factor_class = FactorRegistry.get("test_factor")
        assert factor_class is TestFactor
    
    def test_list_all(self):
        """测试列出所有因子"""
        factors = FactorRegistry.list_all()
        assert isinstance(factors, dict)
