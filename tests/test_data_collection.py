import pytest
import os
import pandas as pd
from scripts.fetch_history_klines_parquet import HistoryKlineFetcher

@pytest.fixture
def test_stock_codes():
    return ['000001', '000002']

@pytest.fixture
def test_output_dir():
    return 'data/kline/test'

@pytest.fixture
def setup_test_dir(test_output_dir):
    os.makedirs(test_output_dir, exist_ok=True)
    yield test_output_dir
    # 清理测试数据
    for file in os.listdir(test_output_dir):
        os.remove(os.path.join(test_output_dir, file))
    os.rmdir(test_output_dir)

def test_fetch_kline_tencent(test_output_dir):
    """测试腾讯API获取K线数据功能"""
    fetcher = HistoryKlineFetcher(data_dir=test_output_dir)
    
    # 测试获取单只股票的K线数据
    code = '000001'
    df = fetcher.fetch_kline_tencent(code, days=30)
    
    assert df is not None, f"获取 {code} 的K线数据失败"
    assert not df.empty, f"获取 {code} 的K线数据为空"
    assert 'trade_date' in df.columns, "缺少 trade_date 列"
    assert 'open' in df.columns, "缺少 open 列"
    assert 'close' in df.columns, "缺少 close 列"
    assert 'high' in df.columns, "缺少 high 列"
    assert 'low' in df.columns, "缺少 low 列"
    assert 'volume' in df.columns, "缺少 volume 列"

def test_save_kline_data(test_output_dir):
    """测试保存K线数据功能"""
    fetcher = HistoryKlineFetcher(data_dir=test_output_dir)
    
    # 创建测试数据
    test_data = {
        'code': ['000001', '000001'],
        'trade_date': ['2024-01-01', '2024-01-02'],
        'open': [10.0, 10.1],
        'close': [10.5, 10.6],
        'high': [10.7, 10.8],
        'low': [9.9, 10.0],
        'volume': [1000000, 1200000]
    }
    df = pd.DataFrame(test_data)
    
    # 保存数据
    fetcher.save_kline_data(df, '000001')
    
    # 检查文件是否生成
    file_path = os.path.join(test_output_dir, '000001.parquet')
    assert os.path.exists(file_path), "文件未生成"
    
    # 检查文件内容
    saved_df = pd.read_parquet(file_path)
    assert not saved_df.empty, "文件为空"
    assert len(saved_df) == 2, "数据行数不正确"

def test_data_quality():
    """测试数据质量"""
    # 这里可以添加数据质量检查的测试
    pass
