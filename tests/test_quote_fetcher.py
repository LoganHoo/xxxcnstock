import pytest
import importlib
import sys
from unittest.mock import patch, MagicMock
from services.data_service.fetchers.quote import QuoteFetcher


def test_quote_fetcher_init():
    """测试行情获取器初始化"""
    fetcher = QuoteFetcher()
    assert fetcher is not None


def test_quote_fetcher_module_does_not_import_akshare_eagerly():
    """测试模块导入阶段不会急切加载 akshare"""
    sys.modules.pop("akshare", None)
    sys.modules.pop("services.data_service.fetchers.quote", None)

    module = importlib.import_module("services.data_service.fetchers.quote")

    assert "akshare" not in sys.modules
    assert not hasattr(module, "ak")


@pytest.mark.anyio
async def test_fetch_realtime_quotes():
    """测试获取实时行情"""
    fetcher = QuoteFetcher()
    
    with patch('services.data_service.fetchers.quote._get_akshare_client') as mock_get_akshare:
        mock_akshare = MagicMock()
        mock_df = MagicMock()
        mock_df.iterrows.return_value = []
        mock_df.empty = True
        mock_akshare.return_value = mock_df
        mock_get_akshare.return_value = mock_akshare
        
        result = await fetcher.fetch_realtime_quotes()
        assert isinstance(result, list)


@pytest.mark.anyio
async def test_fetch_kline():
    """测试获取K线数据"""
    fetcher = QuoteFetcher()
    
    with patch('services.data_service.fetchers.quote._get_akshare_client') as mock_get_akshare:
        mock_akshare = MagicMock()
        mock_akshare.return_value = None
        mock_get_akshare.return_value = mock_akshare
        
        result = await fetcher.fetch_kline("000001")
        # 如果没有数据返回None
        assert result is None or hasattr(result, 'empty')
