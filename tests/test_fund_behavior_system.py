"""
资金行为学系统测试
"""
from pathlib import Path
import importlib
import sys

import polars as pl
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

FactorEngine = importlib.import_module("core.factor_engine").FactorEngine
config_manager = importlib.import_module("core.fund_behavior_config").config_manager
FundBehaviorIndicatorEngine = importlib.import_module(
    "core.fund_behavior_indicator"
).FundBehaviorIndicatorEngine
FundBehaviorStrategyEngine = importlib.import_module(
    "core.fund_behavior_strategy"
).FundBehaviorStrategyEngine

FACTOR_NAMES = [
    "v_ratio10",
    "v_total",
    "cost_peak",
    "limit_up_score",
    "pioneer_status",
    "ma5_bias",
]


@pytest.fixture
def fund_behavior_config():
    """同步资金行为策略配置"""
    config_path = PROJECT_ROOT / "config" / "strategies" / "fund_behavior_config.yaml"
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    config_manager.config = config
    return config


@pytest.fixture
def test_data():
    """构造测试行情数据"""
    return pl.DataFrame(
        {
            "code": ["000001"] * 40 + ["300255"] * 40,
            "trade_date": [f"2023-02-{i:02d}" for i in range(1, 41)] * 2,
            "open": [10.0 + i * 0.1 for i in range(40)] * 2,
            "close": [10.1 + i * 0.1 for i in range(40)] * 2,
            "high": [10.2 + i * 0.1 for i in range(40)] * 2,
            "low": [9.9 + i * 0.1 for i in range(40)] * 2,
            "volume": [1000000 + i * 10000 for i in range(40)] * 2,
        }
    )


@pytest.fixture
def factor_data(test_data, fund_behavior_config):
    """计算测试因子"""
    factor_engine = FactorEngine()
    return factor_engine.calculate_all_factors(test_data, FACTOR_NAMES)


def test_factor_calculation(factor_data):
    """测试因子计算结果包含核心因子列"""
    expected_columns = {
        "factor_v_ratio10",
        "factor_v_total",
        "factor_cost_peak",
        "factor_limit_up_score",
        "factor_pioneer_status",
        "factor_ma5_bias",
    }

    assert factor_data.height == 80
    assert expected_columns.issubset(set(factor_data.columns))


def test_indicator_calculation(factor_data):
    """测试指标引擎返回完整指标结构"""
    indicator_engine = FundBehaviorIndicatorEngine()

    indicators = indicator_engine.calculate_all_indicators(factor_data)

    assert set(indicators.keys()) == {"market_sentiment", "10am_pivot", "exit_lines"}
    assert len(indicators["market_sentiment"]["market_state"]) > 0
    assert len(indicators["10am_pivot"]["upward_pivot"]) > 0
    assert len(indicators["exit_lines"]["closing_condition"]) > 0


def test_strategy_execution(factor_data, fund_behavior_config):
    """测试策略引擎返回标准执行结果"""
    strategy_engine = FundBehaviorStrategyEngine()

    result = strategy_engine.execute_strategy(factor_data, 1000000, "10:00")

    assert set(result.keys()) == {
        "market_state",
        "upward_pivot",
        "hedge_effect",
        "is_strong_region",
        "trend_stocks",
        "short_term_stocks",
        "position_size",
        "exit_signals",
        "cost_peak",
        "current_price",
        "v_total",
        "sentiment_temperature",
        "delta_temperature",
        "market_sentiment_indicators",
    }
    assert result["position_size"] == {
        "trend": 500000.0,
        "short_term": 400000.0,
        "cash": 100000.0,
    }
    assert isinstance(result["trend_stocks"], list)
    assert isinstance(result["short_term_stocks"], list)
    assert isinstance(result["exit_signals"], dict)
    assert isinstance(result["market_sentiment_indicators"], dict)
