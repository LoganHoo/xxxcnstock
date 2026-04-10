from datetime import date, timedelta

import polars as pl
import pytest

import factors.technical  # noqa: F401
from core.factor_library import FactorRegistry


TECHNICAL_FACTOR_NAMES = [
    "asi",
    "atr",
    "bollinger",
    "cci",
    "dmi",
    "emv",
    "kdj",
    "ma5_bias",
    "ma_trend",
    "macd",
    "mtm",
    "psy",
    "roc",
    "rsi",
    "wr",
]


@pytest.fixture
def technical_data():
    trade_dates = [date(2026, 1, 1) + timedelta(days=index) for index in range(60)]
    return pl.DataFrame(
        {
            "code": ["000001"] * len(trade_dates),
            "trade_date": trade_dates,
            "open": [10.0 + index * 0.2 for index in range(len(trade_dates))],
            "high": [10.5 + index * 0.2 for index in range(len(trade_dates))],
            "low": [9.7 + index * 0.2 for index in range(len(trade_dates))],
            "close": [10.2 + index * 0.2 for index in range(len(trade_dates))],
            "volume": [1_000_000 + index * 10_000 for index in range(len(trade_dates))],
        }
    )


@pytest.mark.parametrize("factor_name", TECHNICAL_FACTOR_NAMES)
def test_technical_factor_generates_factor_column(factor_name, technical_data):
    factor = FactorRegistry.get(factor_name)()

    result = factor.calculate(technical_data)

    factor_column = f"factor_{factor_name}"
    assert factor_column in result.columns
    assert result.height == technical_data.height


def test_macd_returns_neutral_score_when_window_is_too_short():
    short_data = pl.DataFrame(
        {
            "code": ["000001"],
            "trade_date": [date(2026, 1, 1)],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "volume": [1_000_000],
        }
    )
    factor = FactorRegistry.get("macd")()

    result = factor.calculate(short_data)

    assert result["factor_macd"].item() == 50.0
