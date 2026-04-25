#!/usr/bin/env python3
"""
财务数据处理模块

提供财务数据的处理和指标计算功能:
- indicator_engine: 财务指标计算引擎
- financial_metrics: 财务指标定义
"""

from .indicator_engine import (
    FinancialIndicatorEngine,
    FinancialIndicators,
    calculate_roe,
    calculate_roa,
    calculate_gross_margin,
    calculate_net_margin,
    calculate_current_ratio,
    calculate_debt_to_asset,
    calculate_all_indicators
)

__all__ = [
    'FinancialIndicatorEngine',
    'FinancialIndicators',
    'calculate_roe',
    'calculate_roa',
    'calculate_gross_margin',
    'calculate_net_margin',
    'calculate_current_ratio',
    'calculate_debt_to_asset',
    'calculate_all_indicators',
]
