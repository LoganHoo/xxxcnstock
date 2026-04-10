import pytest
import importlib
import sys
from unittest.mock import patch, MagicMock
from services.data_service.fetchers.limitup import LimitUpFetcher, LimitUpStock


def test_limitup_fetcher_init():
    """测试涨停获取器初始化"""
    fetcher = LimitUpFetcher()
    assert fetcher is not None


def test_limitup_fetcher_module_does_not_import_akshare_eagerly():
    """测试模块导入阶段不会急切加载 akshare"""
    sys.modules.pop("akshare", None)
    sys.modules.pop("services.data_service.fetchers.limitup", None)

    module = importlib.import_module("services.data_service.fetchers.limitup")

    assert "akshare" not in sys.modules
    assert not hasattr(module, "ak")


@pytest.mark.anyio
async def test_fetch_limit_up_pool():
    """测试获取涨停池"""
    fetcher = LimitUpFetcher()
    
    with patch('services.data_service.fetchers.limitup._get_akshare_client') as mock_get_akshare:
        mock_akshare = MagicMock()
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.iterrows.return_value = []
        mock_akshare.return_value = mock_df
        mock_get_akshare.return_value = mock_akshare
        
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
