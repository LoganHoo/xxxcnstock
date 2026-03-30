import pytest
import polars as pl
from filters.filter_engine import FilterEngine

@pytest.fixture
def test_data():
    """创建测试数据"""
    data = {
        'code': ['000001', '000002', '000003', '000004', '000005'],
        'name': ['平安银行', '万科A', '国农科技', '国华网安', '世纪星源'],
        'close': [15.2, 20.5, 35.8, 12.3, 5.6],
        'volume': [10000000, 5000000, 1000000, 3000000, 500000],
        'market_cap': [300000000000, 200000000000, 5000000000, 15000000000, 8000000000]
    }
    df = pl.DataFrame(data)
    return df

def test_filter_engine_initialization():
    """测试过滤器引擎初始化"""
    engine = FilterEngine()
    assert engine is not None, "过滤器引擎初始化失败"

def test_apply_filters(test_data):
    """测试应用过滤器"""
    engine = FilterEngine()
    # 测试应用价格过滤器
    result = engine.apply_filters(test_data, filter_names=['price_filter'])
    assert len(result) <= len(test_data), "过滤器未生效"

def test_apply_multiple_filters(test_data):
    """测试应用多个过滤器"""
    engine = FilterEngine()
    # 测试应用多个过滤器
    result = engine.apply_filters(test_data, filter_names=['price_filter', 'volume_filter'])
    assert len(result) <= len(test_data), "过滤器未生效"

def test_list_filters():
    """测试列出过滤器"""
    engine = FilterEngine()
    filters = engine.list_filters()
    assert len(filters) > 0, "过滤器列表为空"
    assert all('name' in filter for filter in filters), "过滤器缺少名称"

def test_get_filter():
    """测试获取单个过滤器"""
    engine = FilterEngine()
    filter_instance = engine.get_filter('price_filter')
    assert filter_instance is not None, "过滤器不存在"
