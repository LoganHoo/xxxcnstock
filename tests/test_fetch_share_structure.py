import importlib
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

ShareDataFetcher = importlib.import_module("scripts.fetch_share_structure").ShareDataFetcher


def test_fetch_share_structure_init():
    """测试 ShareDataFetcher 初始化"""
    fetcher = ShareDataFetcher()
    assert fetcher is not None


def test_fetch_share_structure_save():
    """测试保存股本数据"""
    fetcher = ShareDataFetcher()
    
    test_data = [
        {'code': '000001', 'name': '平安银行', 'total_share': 1940592, 'float_share': 1940592},
        {'code': '000002', 'name': '万科A', 'total_share': 1103915, 'float_share': 1103915}
    ]
    
    output_path = 'data/test_share_structure.parquet'
    fetcher.save_to_parquet(test_data, output_path)
    
    assert Path(output_path).exists()
    
    Path(output_path).unlink()
