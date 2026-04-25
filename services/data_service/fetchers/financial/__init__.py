#!/usr/bin/env python3
"""
财务数据获取器模块

提供三大财务报表及财务指标的获取功能:
- balance_sheet_fetcher: 资产负债表获取器
- income_statement_fetcher: 利润表获取器
- cash_flow_fetcher: 现金流量表获取器
- financial_indicator_engine: 财务指标计算引擎
"""

from .balance_sheet_fetcher import (
    BalanceSheetFetcher,
    BalanceSheetData,
    fetch_balance_sheet,
    fetch_balance_sheet_batch
)

from .income_statement_fetcher import (
    IncomeStatementFetcher,
    IncomeStatementData,
    fetch_income_statement,
    fetch_income_statement_batch
)

from .cash_flow_fetcher import (
    CashFlowFetcher,
    CashFlowData,
    fetch_cash_flow,
    fetch_cash_flow_batch
)

__all__ = [
    # 资产负债表
    'BalanceSheetFetcher',
    'BalanceSheetData',
    'fetch_balance_sheet',
    'fetch_balance_sheet_batch',
    # 利润表
    'IncomeStatementFetcher',
    'IncomeStatementData',
    'fetch_income_statement',
    'fetch_income_statement_batch',
    # 现金流量表
    'CashFlowFetcher',
    'CashFlowData',
    'fetch_cash_flow',
    'fetch_cash_flow_batch',
]
