import pytest
import os
import pandas as pd
from scripts.audit_data import DataAuditor

@pytest.fixture
def test_output_dir():
    return 'data/kline/test'

@pytest.fixture
def setup_test_dir(test_output_dir):
    os.makedirs(test_output_dir, exist_ok=True)
    # 创建测试数据
    test_data = {
        'code': ['000001', '000001', '000001'],
        'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'open': [10.0, 10.1, 10.2],
        'close': [10.5, 10.6, 10.7],
        'high': [10.7, 10.8, 10.9],
        'low': [9.9, 10.0, 10.1],
        'volume': [1000000, 1200000, 1500000]
    }
    df = pd.DataFrame(test_data)
    df.to_parquet(os.path.join(test_output_dir, '000001.parquet'), index=False)
    yield test_output_dir
    # 清理测试数据
    for file in os.listdir(test_output_dir):
        os.remove(os.path.join(test_output_dir, file))
    os.rmdir(test_output_dir)

def test_audit_data(setup_test_dir):
    """测试数据质量检查功能"""
    auditor = DataAuditor(data_dir=setup_test_dir)
    
    # 执行数据质量检查
    report = auditor.audit()
    
    # 检查报告内容
    assert 'total_stocks' in report, "报告缺少 total_stocks 字段"
    assert 'total_records' in report, "报告缺少 total_records 字段"
    assert 'date_range' in report, "报告缺少 date_range 字段"
    assert 'quality_issues' in report, "报告缺少 quality_issues 字段"
    
    # 检查数据质量
    assert report['total_stocks'] == 1, "股票数量不正确"
    assert report['total_records'] == 3, "记录数量不正确"
    assert report['date_range'] == ('2024-01-01', '2024-01-03'), "日期范围不正确"
    assert len(report['quality_issues']) == 0, "不应有数据质量问题"

def test_check_data_completeness():
    """测试数据完整性检查"""
    # 这里可以添加数据完整性检查的测试
    pass

def test_check_data_accuracy():
    """测试数据准确性检查"""
    # 这里可以添加数据准确性检查的测试
    pass
