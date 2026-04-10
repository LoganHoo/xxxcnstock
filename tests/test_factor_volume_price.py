from datetime import date, timedelta

import polars as pl
import pytest

import factors.volume_price  # noqa: F401
from core.factor_library import FactorRegistry


VOLUME_PRICE_FACTOR_NAMES = [
    "mfi",
    "obv",
    "turnover",
    "v_ratio10",
    "v_total",
    "vma",
    "volume_ratio",
    "vosc",
    "vr",
    "wvad",
]


@pytest.fixture
def volume_price_data():
    trade_dates = [date(2026, 1, 1) + timedelta(days=index) for index in range(40)]
    return pl.DataFrame(
        {
            "code": ["000001"] * len(trade_dates),
            "trade_date": trade_dates,
            "open": [10.0 + index * 0.1 for index in range(len(trade_dates))],
            "high": [10.5 + index * 0.1 for index in range(len(trade_dates))],
            "low": [9.8 + index * 0.1 for index in range(len(trade_dates))],
            "close": [10.2 + index * 0.1 for index in range(len(trade_dates))],
            "volume": [1_000_000 + index * 12_000 for index in range(len(trade_dates))],
            "amount": [20_000_000 + index * 300_000 for index in range(len(trade_dates))],
        }
    )


@pytest.mark.parametrize("factor_name", VOLUME_PRICE_FACTOR_NAMES)
def test_volume_price_factor_generates_factor_column(factor_name, volume_price_data):
    factor = FactorRegistry.get(factor_name)()

    result = factor.calculate(volume_price_data)

    factor_column = f"factor_{factor_name}"
    assert factor_column in result.columns
    assert result.height == volume_price_data.height


def test_turnover_handles_zero_amount_without_infinite_values(volume_price_data):
    factor = FactorRegistry.get("turnover")()
    zero_amount_data = volume_price_data.with_columns(pl.lit(0).alias("amount"))

    result = factor.calculate(zero_amount_data)

    assert result["factor_turnover"].is_finite().all()
    assert result["turnover_rate"].is_finite().all()


def test_v_ratio10_handles_zero_previous_volume_without_infinite_values(volume_price_data):
    factor = FactorRegistry.get("v_ratio10")()
    zero_previous_data = volume_price_data.with_columns(
        pl.when(pl.arange(0, pl.len()) == 0)
        .then(0)
        .otherwise(pl.col("volume"))
        .alias("volume")
    )

    result = factor.calculate(zero_previous_data)

    assert result["factor_v_ratio10"].is_finite().all()
