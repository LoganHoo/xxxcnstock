"""
因子引擎测试
"""
import pytest
import polars as pl
from core.factor_engine import FactorEngine


class TestFactorEngine:
    """FactorEngine 测试"""
    
    @pytest.fixture
    def engine(self):
        """创建引擎实例"""
        return FactorEngine(config_dir="config/factors")
    
    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        return pl.DataFrame({
            "code": ["000001"] * 30,
            "trade_date": [f"2026-03-{i:02d}" for i in range(1, 31)],
            "open": [10.0 + i * 0.1 for i in range(30)],
            "high": [10.5 + i * 0.1 for i in range(30)],
            "low": [9.5 + i * 0.1 for i in range(30)],
            "close": [10.0 + i * 0.1 for i in range(30)],
            "volume": [1000000 + i * 10000 for i in range(30)]
        })
    
    def test_load_configs(self, engine):
        """测试加载配置"""
        assert len(engine.factor_configs) > 0
        assert "ma_trend" in engine.factor_configs
    
    def test_list_factors(self, engine):
        """测试列出因子"""
        factors = engine.list_factors()
        assert len(factors) > 0
        
        technical_factors = engine.list_factors(category="technical")
        assert all(f["category"] == "technical" for f in technical_factors)
    
    def test_get_factor_info(self, engine):
        """测试获取因子信息"""
        info = engine.get_factor_info("ma_trend")
        assert info is not None
        assert info["category"] == "technical"
    
    def test_calculate_factor_missing(self, engine, sample_data):
        """测试计算不存在的因子"""
        result = engine.calculate_factor(sample_data, "nonexistent_factor")
        assert "factor_nonexistent_factor" in result.columns
