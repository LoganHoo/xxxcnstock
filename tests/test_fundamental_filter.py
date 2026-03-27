import pytest
from services.stock_service.filters.fundamental import FundamentalFilter


def test_fundamental_filter_init():
    """测试基本面筛选器初始化"""
    f = FundamentalFilter()
    assert f is not None


def test_fundamental_filter_score():
    """测试基本面评分"""
    f = FundamentalFilter()
    
    # 优质股票数据（各项指标均表现优秀）
    good_data = {
        "pe": 25.0,
        "pb": 5.0,
        "roe": 50.0,       # ROE较高
        "revenue_growth": 100.0,   # 营收增长高
        "profit_growth": 100.0,    # 利润增长高
        "debt_ratio": 20.0  # 负债率低
    }
    
    score = f.calculate_score(good_data)
    assert 0 <= score <= 100
    assert score > 60  # 优质股应该得分高


def test_fundamental_filter_pass():
    """测试筛选通过条件"""
    f = FundamentalFilter()
    
    good_data = {
        "pe": 25.0,
        "pb": 2.5,
        "roe": 18.0,
        "revenue_growth": 25.0,
        "profit_growth": 30.0,
        "debt_ratio": 35.0
    }
    
    result = f.filter(good_data)
    assert result["passed"] is True


def test_fundamental_filter_fail():
    """测试筛选失败条件"""
    f = FundamentalFilter()
    
    bad_data = {
        "pe": 100.0,  # PE过高
        "pb": 15.0,
        "roe": 5.0,   # ROE过低
        "revenue_growth": 5.0,
        "profit_growth": 5.0,
        "debt_ratio": 80.0  # 负债过高
    }
    
    result = f.filter(bad_data)
    assert result["passed"] is False
