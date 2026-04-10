import pandas as pd
from services.data_service.storage.parquet_manager import ParquetManager


def test_parquet_manager_init():
    """测试存储管理器初始化"""
    manager = ParquetManager()
    assert manager.data_dir is not None


def test_save_and_read_parquet(tmp_path):
    """测试保存和读取Parquet"""
    manager = ParquetManager(data_dir=str(tmp_path))
    
    df = pd.DataFrame({
        "code": ["000001", "000002"],
        "name": ["平安银行", "万科A"],
        "price": [10.5, 15.2]
    })
    
    # 保存
    result = manager.save(df, "test/quotes.parquet")
    assert result is True
    
    # 读取
    result_df = manager.read("test/quotes.parquet")
    assert result_df is not None
    assert len(result_df) == 2
    assert result_df.iloc[0]["code"] == "000001"


def test_append_parquet(tmp_path):
    """测试追加数据"""
    manager = ParquetManager(data_dir=str(tmp_path))
    
    df1 = pd.DataFrame({"code": ["000001"], "price": [10.0]})
    df2 = pd.DataFrame({"code": ["000002"], "price": [15.0]})
    
    manager.save(df1, "test/quotes.parquet")
    manager.append(df2, "test/quotes.parquet")
    
    result = manager.read("test/quotes.parquet")
    assert len(result) == 2
