from datetime import date, timedelta

import polars as pl
import pytest

import factors.market  # noqa: F401
from core.factor_library import FactorRegistry


@pytest.fixture
def market_data():
    trade_dates = [date(2026, 1, 1) + timedelta(days=index) for index in range(30)]
    rows = []
    for index, trade_date in enumerate(trade_dates):
        rows.append(
            {
                "code": "000001",
                "trade_date": trade_date,
                "open": 10.0 + index * 0.2,
                "high": 10.5 + index * 0.2,
                "low": 9.8 + index * 0.2,
                "close": 10.3 + index * 0.2,
                "volume": 1_000_000 + index * 20_000,
            }
        )
        rows.append(
            {
                "code": "300255",
                "trade_date": trade_date,
                "open": 8.0 + index * 0.1,
                "high": 8.3 + index * 0.1,
                "low": 7.7 + index * 0.1,
                "close": 8.15 + index * 0.1,
                "volume": 800_000 + index * 15_000,
            }
        )
    return pl.DataFrame(rows)


@pytest.mark.parametrize(
    "factor_name",
    [
        "cost_peak",
        "limit_up_score",
        "pioneer_status",
        "market_breadth",
        "market_sentiment",
        "market_temperature",
        "market_trend",
    ],
)
def test_market_factor_generates_factor_column(factor_name, market_data):
    factor_class = FactorRegistry.get(factor_name)
    factor = factor_class()

    result = factor.calculate(market_data)

    factor_column = f"factor_{factor_name}"
    assert factor_column in result.columns
    assert result.height == market_data.height


def test_pioneer_status_returns_zero_when_target_code_absent(market_data):
    factor = FactorRegistry.get("pioneer_status")(params={"pioneer_codes": ["688001"]})

    result = factor.calculate(market_data.filter(pl.col("code") != "688001"))

    assert result["factor_pioneer_status"].fill_null(0).eq(0).all()


def test_market_sentiment_fills_null_values_with_zero_on_short_window():
    short_data = pl.DataFrame(
        {
            "code": ["000001", "000001"],
            "trade_date": [date(2026, 1, 1), date(2026, 1, 2)],
            "open": [10.0, 10.2],
            "high": [10.5, 10.6],
            "low": [9.8, 10.0],
            "close": [10.1, 10.3],
            "volume": [1_000_000, 1_050_000],
        }
    )
    factor = FactorRegistry.get("market_sentiment")()

    result = factor.calculate(short_data)

    assert result["factor_market_sentiment"].fill_null(0).is_not_null().all()
