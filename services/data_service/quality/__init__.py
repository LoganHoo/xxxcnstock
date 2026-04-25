#!/usr/bin/env python3
"""
数据质量服务模块

提供数据质量验证、监控和报告功能。
基于 Great Expectations 风格的声明式验证。
"""

from .gx_validator import (
    GreatExpectationsValidator,
    ExpectationResult,
    ValidationSuiteResult,
    KlineDataQualitySuite,
    StockListQualitySuite,
    validate_kline_data,
    generate_quality_report
)

from .monitor import (
    DataQualityMonitor,
    Alert,
    AlertLevel,
    DataFreshnessMetric
)

__all__ = [
    # Validator
    'GreatExpectationsValidator',
    'ExpectationResult',
    'ValidationSuiteResult',
    'KlineDataQualitySuite',
    'StockListQualitySuite',
    'validate_kline_data',
    'generate_quality_report',
    # Monitor
    'DataQualityMonitor',
    'Alert',
    'AlertLevel',
    'DataFreshnessMetric'
]

__version__ = '0.1.0'
