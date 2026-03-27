import pytest
from services.limit_service.analyzers.pre_limit import PreLimitPredictor


def test_pre_limit_predictor_init():
    """测试涨停预判器初始化"""
    predictor = PreLimitPredictor()
    assert predictor is not None


def test_predict_limit_probability():
    """测试涨停概率预测"""
    predictor = PreLimitPredictor()
    
    stock_data = {
        "change_pct": 8.5,
        "volume_ratio": 2.5,
        "turnover_rate": 8.0,
        "sector_change": 3.0,
        "sector_limit_count": 5
    }
    
    result = predictor.predict(stock_data)
    assert "probability" in result
    assert 0 <= result["probability"] <= 100
    assert "factors" in result
    assert "prediction" in result


def test_predict_already_limit():
    """测试已涨停股票的预测"""
    predictor = PreLimitPredictor()
    
    stock_data = {
        "change_pct": 10.0,  # 已涨停
        "volume_ratio": 3.0,
        "turnover_rate": 5.0,
        "seal_amount": 100000000,
        "seal_ratio": 5.0,  # 强封
        "sector_change": 2.0,
        "sector_limit_count": 3
    }
    
    result = predictor.predict(stock_data)
    assert result["probability"] >= 80  # 高概率
