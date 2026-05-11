"""DataAccess 单元测试"""
import pytest
from pathlib import Path


class TestDataAccess:
    def test_get_stock_list(self, data_access):
        stock_list = data_access.get_stock_list()
        assert stock_list is not None

    def test_get_stock_names(self, data_access):
        names = data_access.get_stock_names()
        assert isinstance(names, dict)

    def test_get_available_codes(self, data_access):
        codes = data_access.get_available_codes()
        assert isinstance(codes, list)

    def test_check_data_freshness_invalid_code(self, data_access):
        result = data_access.check_data_freshness("999999", max_age_days=30)
        assert result is False

    def test_get_kline_nonexistent(self, data_access):
        result = data_access.get_kline("999999")
        assert result is None
