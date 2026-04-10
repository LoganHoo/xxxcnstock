import polars as pl
import pytest

from core.factor_engine import FactorEngine


@pytest.fixture
def sample_data():
    return pl.DataFrame(
        {
            "code": ["000001"] * 40,
            "trade_date": [f"2026-03-{index:02d}" for index in range(1, 41)],
            "open": [10.0 + index * 0.1 for index in range(40)],
            "high": [10.5 + index * 0.1 for index in range(40)],
            "low": [9.7 + index * 0.1 for index in range(40)],
            "close": [10.2 + index * 0.1 for index in range(40)],
            "volume": [1_000_000 + index * 10_000 for index in range(40)],
            "amount": [20_000_000 + index * 200_000 for index in range(40)],
        }
    )


def test_factor_engine_loads_nested_factor_configs(tmp_path):
    config_dir = tmp_path / "factors"
    technical_dir = config_dir / "technical"
    technical_dir.mkdir(parents=True)
    (technical_dir / "macd.yaml").write_text(
        """
factor:
  name: macd
  category: technical
  description: MACD 因子
  params:
    default:
      fast: 12
      slow: 26
      signal: 9
  scoring:
    weight: 0.06
""".strip(),
        encoding="utf-8",
    )

    engine = FactorEngine(config_dir=str(config_dir))

    assert engine.get_factor_info("macd") is not None
    assert engine.get_factor_info("macd")["category"] == "technical"


def test_calculate_factor_returns_expected_column_for_known_and_unknown_factor(sample_data):
    engine = FactorEngine(config_dir="config/factors")

    known = engine.calculate_factor(sample_data, "macd")
    unknown = engine.calculate_factor(sample_data, "nonexistent_factor")

    assert "factor_macd" in known.columns
    assert "factor_nonexistent_factor" in unknown.columns
    assert unknown["factor_nonexistent_factor"].unique().to_list() == [50.0]


def test_calculate_all_factors_combines_multiple_categories(sample_data):
    engine = FactorEngine(config_dir="config/factors")

    result = engine.calculate_all_factors(
        sample_data,
        factor_names=["market_sentiment", "macd", "volume_ratio"],
    )

    assert "factor_market_sentiment" in result.columns
    assert "factor_macd" in result.columns
    assert "factor_volume_ratio" in result.columns


def test_list_factors_filters_by_category_and_enabled_flag():
    engine = FactorEngine(config_dir="config/factors")

    technical = engine.list_factors(category="technical", enabled_only=True)

    assert technical
    assert all(item["category"] == "technical" for item in technical)
