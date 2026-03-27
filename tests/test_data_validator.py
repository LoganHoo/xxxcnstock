import pytest
import polars as pl
from services.data_validator import DataValidator


def test_data_validator_init():
    """测试 DataValidator 初始化"""
    config = {
        'min_records': 1000,
        'max_age_days': 7,
        'price_range': [0.1, 1000],
        'change_pct_range': [-20, 20]
    }
    validator = DataValidator(config)
    assert validator.config == config


def test_check_completeness():
    """测试完整性检查"""
    config = {'min_records': 100}
    validator = DataValidator(config)
    
    df = pl.DataFrame({
        'code': ['000001'] * 150,
        'name': ['平安银行'] * 150,
        'price': [10.0] * 150,
        'grade': ['S'] * 150,
        'enhanced_score': [85.0] * 150
    })
    
    result = validator.check_completeness(df)
    assert result['passed'] is True
    assert result['record_count'] == 150


def test_check_validity():
    """测试有效性检查"""
    config = {
        'price_range': [0.1, 1000],
        'change_pct_range': [-20, 20]
    }
    validator = DataValidator(config)
    
    df = pl.DataFrame({
        'price': [10.0, 20.0, 30.0],
        'change_pct': [1.5, -2.0, 0.5]
    })
    
    result = validator.check_validity(df)
    assert result['passed'] is True


def test_check_consistency():
    """测试一致性检查"""
    config = {}
    validator = DataValidator(config)
    
    df = pl.DataFrame({
        'grade': ['S', 'A', 'B'],
        'enhanced_score': [85.0, 77.0, 70.0]
    })
    
    result = validator.check_consistency(df)
    assert result['passed'] is True
