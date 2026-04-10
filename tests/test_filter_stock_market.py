from datetime import date, timedelta

import polars as pl

from filters.market_filter import (
    MarketCapFilter,
    PriceFilter,
    SuspensionFilter,
    VolumeFilter,
)
from filters.stock_filter import DelistingFilter, NewStockFilter, STFilter


def test_st_filter_removes_st_and_star_st_names():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "name": ["平安银行", "ST中珠", "*ST金钰"],
        }
    )

    result = STFilter(params={"enabled": True}).filter(stock_list)

    assert result["code"].to_list() == ["000001"]


def test_new_stock_filter_supports_date_column_values():
    old_date = date.today() - timedelta(days=120)
    new_date = date.today() - timedelta(days=10)
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002"],
            "list_date": [old_date, new_date],
        }
    )

    result = NewStockFilter(
        params={"enabled": True, "min_listing_days": 60}
    ).filter(stock_list)

    assert result["code"].to_list() == ["000001"]


def test_new_stock_filter_skips_when_list_date_missing():
    stock_list = pl.DataFrame({"code": ["000001", "000002"]})

    result = NewStockFilter(params={"enabled": True}).filter(stock_list)

    assert result.equals(stock_list)


def test_delisting_filter_removes_delisting_risk_names():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "name": ["正常股票", "退市海润", "风险警示A"],
        }
    )

    result = DelistingFilter(params={"enabled": True}).filter(stock_list)

    assert result["code"].to_list() == ["000001"]


def test_market_cap_filter_supports_total_mv_column():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "total_mv": [4_000_000_000, 5_000_000_000, 8_000_000_000],
        }
    )

    result = MarketCapFilter(
        params={"enabled": True, "min_market_cap": 5_000_000_000}
    ).filter(stock_list)

    assert result["code"].to_list() == ["000002", "000003"]


def test_suspension_filter_uses_volume_when_trade_status_missing():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "volume": [1000, 0, 50],
        }
    )

    result = SuspensionFilter(params={"enabled": True}).filter(stock_list)

    assert result["code"].to_list() == ["000001", "000003"]


def test_price_filter_respects_min_and_max_price():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "close": [1.5, 20.0, 301.0],
        }
    )

    result = PriceFilter(
        params={"enabled": True, "min_price": 2.0, "max_price": 300.0}
    ).filter(stock_list)

    assert result["code"].to_list() == ["000002"]


def test_volume_filter_removes_low_volume_rows():
    stock_list = pl.DataFrame(
        {
            "code": ["000001", "000002", "000003"],
            "volume": [900_000, 1_000_000, 2_500_000],
        }
    )

    result = VolumeFilter(
        params={"enabled": True, "min_volume": 1_000_000}
    ).filter(stock_list)

    assert result["code"].to_list() == ["000002", "000003"]
