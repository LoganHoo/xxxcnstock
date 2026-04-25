"""
增量数据处理测试
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import polars as pl

from core.incremental_processor import (
    IncrementalDetector,
    DataHashChecker,
    check_date_continuity,
    validate_data_integrity,
    IncrementalDataLoader
)


class TestIncrementalDetector:
    """测试增量检测器"""
    
    def test_check_stock_file_not_exists(self, tmp_path):
        """测试文件不存在的情况"""
        detector = IncrementalDetector(tmp_path)
        
        result = detector.check_stock("000001", "2024-01-01", "2024-01-31")
        
        assert result.needs_update is True
        assert result.reason == "文件不存在"
    
    def test_check_stock_up_to_date(self, tmp_path):
        """测试数据已是最新的情况"""
        # 创建测试数据文件
        kline_dir = tmp_path / "kline"
        kline_dir.mkdir()
        
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        df = pl.DataFrame({
            "trade_date": [yesterday.strftime("%Y-%m-%d")],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [10000]
        })
        df.write_parquet(kline_dir / "000001.parquet")
        
        detector = IncrementalDetector(kline_dir)
        result = detector.check_stock("000001", "2024-01-01", yesterday.strftime("%Y-%m-%d"))
        
        assert result.needs_update is False
        assert "已是最新" in result.reason
    
    def test_check_stock_needs_update(self, tmp_path):
        """测试需要更新的情况"""
        kline_dir = tmp_path / "kline"
        kline_dir.mkdir()
        
        # 创建旧数据
        old_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        df = pl.DataFrame({
            "trade_date": [old_date],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [10000]
        })
        df.write_parquet(kline_dir / "000001.parquet")
        
        detector = IncrementalDetector(kline_dir)
        today = datetime.now().strftime("%Y-%m-%d")
        result = detector.check_stock("000001", "2024-01-01", today)
        
        assert result.needs_update is True
        assert result.missing_dates is not None
        assert len(result.missing_dates) > 0


class TestDataHashChecker:
    """测试数据哈希检查器"""
    
    def test_compute_hash(self, tmp_path):
        """测试哈希计算"""
        checker = DataHashChecker(tmp_path)
        
        df1 = pl.DataFrame({
            "a": [1, 2, 3],
            "b": ["x", "y", "z"]
        })
        
        df2 = pl.DataFrame({
            "a": [1, 2, 3],
            "b": ["x", "y", "z"]
        })
        
        hash1 = checker.compute_hash(df1)
        hash2 = checker.compute_hash(df2)
        
        # 相同数据应该产生相同哈希
        assert hash1 == hash2
        
        # 不同数据应该产生不同哈希
        df3 = pl.DataFrame({
            "a": [1, 2, 4],
            "b": ["x", "y", "z"]
        })
        hash3 = checker.compute_hash(df3)
        assert hash1 != hash3


class TestCheckDateContinuity:
    """测试日期连续性检查"""
    
    def test_continuous_dates(self):
        """测试连续日期"""
        df = pl.DataFrame({
            "trade_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        })
        
        is_continuous, gaps = check_date_continuity(df, "trade_date")
        
        assert is_continuous is True
        assert len(gaps) == 0
    
    def test_discontinuous_dates(self):
        """测试不连续日期"""
        df = pl.DataFrame({
            "trade_date": ["2024-01-01", "2024-01-02", "2024-01-10"]
        })
        
        is_continuous, gaps = check_date_continuity(df, "trade_date", max_gap_days=5)
        
        assert is_continuous is False
        assert len(gaps) > 0


class TestValidateDataIntegrity:
    """测试数据完整性验证"""
    
    def test_valid_data(self):
        """测试有效数据"""
        df = pl.DataFrame({
            "open": [10.0, 10.5],
            "high": [11.0, 11.5],
            "low": [9.0, 9.5],
            "close": [10.5, 11.0],
            "volume": [1000, 2000]
        })
        
        is_valid, msg = validate_data_integrity(df, "000001", min_rows=1)
        
        assert is_valid is True
        assert msg == "验证通过"
    
    def test_invalid_ohlc(self):
        """测试无效 OHLC"""
        df = pl.DataFrame({
            "open": [10.0],
            "high": [9.0],  # high < open，无效
            "low": [8.0],
            "close": [9.5],
            "volume": [1000]
        })
        
        is_valid, msg = validate_data_integrity(df, "000001", min_rows=1)
        
        assert is_valid is False
        assert "OHLC" in msg
    
    def test_insufficient_rows(self):
        """测试行数不足"""
        df = pl.DataFrame({
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000]
        })
        
        is_valid, msg = validate_data_integrity(df, "000001", min_rows=10)
        
        assert is_valid is False
        assert "行数不足" in msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
