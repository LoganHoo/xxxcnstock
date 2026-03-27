import pytest
import anyio
from unittest.mock import patch, MagicMock, AsyncMock
from services.data_service.fetchers.quote import QuoteFetcher


def test_quote_fetcher_init():
    """测试行情获取器初始化"""
    fetcher = QuoteFetcher()
    assert fetcher is not None


@pytest.mark.anyio
async def test_fetch_realtime_quotes():
    """测试获取实时行情"""
    fetcher = QuoteFetcher()
    
    with patch('services.data_service.fetchers.quote.ak.stock_zh_a_spot_em') as mock_akshare:
        mock_df = MagicMock()
        mock_df.iterrows.return_value = []
        mock_df.empty = True
        mock_akshare.return_value = mock_df
        
        result = await fetcher.fetch_realtime_quotes()
        assert isinstance(result, list)


@pytest.mark.anyio
async def test_fetch_kline():
    """测试获取K线数据"""
    fetcher = QuoteFetcher()
    
    with patch('services.data_service.fetchers.quote.ak.stock_zh_a_hist') as mock_akshare:
        mock_akshare.return_value = None
        
        result = await fetcher.fetch_kline("000001")
        # 如果没有数据返回None
        assert result is None or hasattr(result, 'empty')
