"""
策略引擎测试
"""
import pytest
import polars as pl
from core.strategy_engine import StrategyEngine


@pytest.fixture
def sample_data():
    """创建测试数据"""
    data = pl.DataFrame({
        "code": [f"00000{i}" for i in range(1, 11)],
        "trade_date": ["2026-03-26"] * 10,
        "open": [10.0 + i for i in range(10)],
        "high": [10.5 + i for i in range(10)],
        "low": [9.5 + i for i in range(10)],
        "close": [10.0 + i for i in range(10)],
        "volume": [1000000 + i * 100000 for i in range(10)],
        "change_pct": [1.0 + i * 0.5 for i in range(10)]
    })
    return data


class TestStrategyEngine:
    """StrategyEngine 测试"""
    
    def test_load_config(self):
        """测试加载配置"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        
        assert engine.strategy_name == "趋势跟踪策略"
        assert len(engine.factors) > 0
    
    def test_get_strategy_info(self):
        """测试获取策略信息"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        info = engine.get_strategy_info()
        
        assert "name" in info
        assert "factors" in info
    
    def test_calculate_weighted_score(self, sample_data):
        """测试加权得分计算"""
        engine = StrategyEngine("config/strategies/trend_following.yaml")
        
        df = sample_data.with_columns([
            pl.lit(70.0).alias("factor_ma_trend"),
            pl.lit(60.0).alias("factor_macd"),
            pl.lit(50.0).alias("factor_rsi"),
            pl.lit(80.0).alias("factor_volume_ratio"),
        ])
        
        result = engine.calculate_weighted_score(df)
        
        assert "strategy_score" in result.columns
