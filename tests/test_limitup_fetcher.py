import pytest
from unittest.mock import patch, MagicMock
from services.data_service.fetchers.limitup import LimitUpFetcher, LimitUpStock


def test_limitup_fetcher_init():
    """测试涨停获取器初始化"""
    fetcher = LimitUpFetcher()
    assert fetcher is not None


@pytest.mark.anyio
async def test_fetch_limit_up_pool():
    """测试获取涨停池"""
    fetcher = LimitUpFetcher()
    
    with patch('akshare.stock_zt_pool_em') as mock_akshare:
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.iterrows.return_value = []
        mock_akshare.return_value = mock_df
        
        result = await fetcher.fetch_limit_up_pool()
        assert isinstance(result, list)


def test_limit_up_stock_creation():
    """测试涨停股票数据类"""
    stock = LimitUpStock(
        code="000001",
        name="平安银行",
        change_pct=10.0,
        limit_time="09:30:00",
        seal_amount=50000,
        open_count=0,
        continuous_limit=1
    )
    assert stock.code == "000001"
    assert stock.continuous_limit == 1
