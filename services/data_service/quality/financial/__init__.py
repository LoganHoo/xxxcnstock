#!/usr/bin/env python3
"""
财务数据质量验证模块

提供财务数据的验证和质量检查功能:
- financial_validator: 财务数据验证器
- accounting_rules: 会计规则验证
- indicator_checker: 财务指标合理性检查
"""

from .financial_validator import (
    FinancialDataValidator,
    ValidationResult,
    ValidationRule,
    validate_balance_sheet,
    validate_income_statement,
    validate_cash_flow,
    validate_accounting_equation
)

__all__ = [
    'FinancialDataValidator',
    'ValidationResult',
    'ValidationRule',
    'validate_balance_sheet',
    'validate_income_statement',
    'validate_cash_flow',
    'validate_accounting_equation',
]
