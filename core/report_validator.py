#!/usr/bin/env python3
"""
报告内容验证模块

用于验证报告数据的完整性、正确性，检测异常值（空值、0、NA等）
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """验证严重级别"""
    INFO = "info"           # 信息
    WARNING = "warning"     # 警告
    ERROR = "error"         # 错误
    CRITICAL = "critical"   # 严重错误


@dataclass
class ValidationIssue:
    """验证问题"""
    field: str
    severity: ValidationSeverity
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """验证结果"""
    report_type: str
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    empty_fields: List[str] = field(default_factory=list)
    zero_fields: List[str] = field(default_factory=list)
    na_fields: List[str] = field(default_factory=list)

    def add_issue(self, field: str, severity: ValidationSeverity, message: str, value: Any = None):
        """添加问题"""
        self.issues.append(ValidationIssue(field, severity, message, value))

    def has_errors(self) -> bool:
        """是否有错误级别的问题"""
        return any(i.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL] 
                   for i in self.issues)


class ReportValidator:
    """报告验证器"""

    # NA值集合
    NA_VALUES = {'NA', 'N/A', 'na', 'n/a', 'None', 'null', '', 'NULL', 'nan', 'NaN'}

    def __init__(self):
        """初始化验证器"""
        pass

    def _is_empty(self, value: Any) -> bool:
        """检查是否为空值"""
        if value is None:
            return True
        if isinstance(value, (str,)):
            return len(value.strip()) == 0
        if isinstance(value, (list, dict)):
            return len(value) == 0
        return False

    def _is_zero(self, value: Any) -> bool:
        """检查是否为0值"""
        if isinstance(value, (int, float)):
            return value == 0
        return False

    def _is_na(self, value: Any) -> bool:
        """检查是否为NA值"""
        if isinstance(value, str):
            return value.strip() in self.NA_VALUES
        return False

    def _validate_value(self, field_path: str, value: Any, result: ValidationResult):
        """验证单个值"""
        if self._is_empty(value):
            result.empty_fields.append(field_path)
            result.add_issue(field_path, ValidationSeverity.WARNING, "字段为空", value)
            return

        if self._is_zero(value):
            result.zero_fields.append(field_path)
            result.add_issue(field_path, ValidationSeverity.INFO, "字段值为0", value)
            return

        if self._is_na(value):
            result.na_fields.append(field_path)
            result.add_issue(field_path, ValidationSeverity.WARNING, "字段为NA值", value)
            return

    def _validate_dict(self, data: Dict, result: ValidationResult, prefix: str = ""):
        """递归验证字典"""
        for key, value in data.items():
            field_path = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                self._validate_dict(value, result, field_path)
            elif isinstance(value, list):
                self._validate_list(value, result, field_path)
            else:
                self._validate_value(field_path, value, result)

    def _validate_list(self, data: List, result: ValidationResult, prefix: str = ""):
        """递归验证列表"""
        for i, item in enumerate(data):
            field_path = f"{prefix}[{i}]"

            if isinstance(item, dict):
                self._validate_dict(item, result, field_path)
            elif isinstance(item, list):
                self._validate_list(item, result, field_path)
            else:
                self._validate_value(field_path, item, result)

    def validate_data(self, report_type: str, data: Dict) -> ValidationResult:
        """
        验证报告数据

        Args:
            report_type: 报告类型
            data: 报告数据

        Returns:
            ValidationResult
        """
        result = ValidationResult(report_type=report_type, is_valid=True)

        if not data:
            result.is_valid = False
            result.add_issue("root", ValidationSeverity.CRITICAL, "数据为空")
            return result

        self._validate_dict(data, result)

        # 根据问题数量判断是否有效
        error_count = sum(1 for i in result.issues if i.severity == ValidationSeverity.ERROR)
        critical_count = sum(1 for i in result.issues if i.severity == ValidationSeverity.CRITICAL)

        result.is_valid = (error_count == 0 and critical_count == 0)

        return result

    def validate_foreign_data(self, data: Dict) -> ValidationResult:
        """验证外盘数据"""
        result = self.validate_data("foreign_index", data)

        # 特定字段检查
        required_sections = ['us_index', 'asia_index', 'commodity']
        for section in required_sections:
            if section not in data:
                result.add_issue(section, ValidationSeverity.ERROR, f"缺少{section}数据")
                result.missing_fields.append(section)

        return result

    def validate_market_analysis(self, data: Dict) -> ValidationResult:
        """验证大盘分析数据"""
        result = self.validate_data("market_analysis", data)

        # 检查关键字段
        if 'indices' not in data or not data['indices']:
            result.add_issue("indices", ValidationSeverity.ERROR, "缺少指数数据")

        if 'summary' not in data:
            result.add_issue("summary", ValidationSeverity.ERROR, "缺少摘要数据")

        return result

    def validate_daily_picks(self, data: Dict) -> ValidationResult:
        """验证选股数据"""
        result = self.validate_data("daily_picks", data)

        # 检查选股结果
        filters = data.get('filters', {})
        s_grade = filters.get('s_grade', {})
        a_grade = filters.get('a_grade', {})

        s_count = s_grade.get('count', 0)
        a_count = a_grade.get('count', 0)

        if s_count == 0 and a_count == 0:
            result.add_issue("filters", ValidationSeverity.WARNING, "S级和A级选股均为0")

        return result

    def validate_fund_behavior(self, data: Dict) -> ValidationResult:
        """验证资金行为学数据"""
        result = self.validate_data("fund_behavior", data)

        # 检查关键字段
        required_fields = ['market_state', 'upward_pivot', 'hedge_effect']
        for field in required_fields:
            if field not in data:
                result.add_issue(field, ValidationSeverity.ERROR, f"缺少{field}数据")

        return result


class ReportQualityChecker:
    """报告质量检查器"""

    def __init__(self):
        self.validator = ReportValidator()

    def check_report_completeness(self, report_type: str, **data_sources) -> Dict[str, Any]:
        """
        检查报告完整性

        Args:
            report_type: 报告类型
            **data_sources: 各种数据源

        Returns:
            Dict
        """
        results = {
            'report_type': report_type,
            'timestamp': datetime.now().isoformat(),
            'data_sources': {},
            'overall_valid': True,
            'critical_issues': [],
            'warnings': []
        }

        for source_name, source_data in data_sources.items():
            if source_data is None:
                results['data_sources'][source_name] = {
                    'exists': False,
                    'valid': False,
                    'issues': ['数据不存在']
                }
                results['critical_issues'].append(f"{source_name}: 数据不存在")
                results['overall_valid'] = False
            else:
                validation = self.validator.validate_data(source_name, source_data)
                results['data_sources'][source_name] = {
                    'exists': True,
                    'valid': validation.is_valid,
                    'empty_fields': validation.empty_fields,
                    'zero_fields': validation.zero_fields,
                    'na_fields': validation.na_fields,
                    'issues': [f"{i.field}: {i.message}" for i in validation.issues]
                }

                if not validation.is_valid:
                    results['overall_valid'] = False

                # 收集警告
                for issue in validation.issues:
                    if issue.severity == ValidationSeverity.WARNING:
                        results['warnings'].append(f"{source_name}.{issue.field}: {issue.message}")
                    elif issue.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]:
                        results['critical_issues'].append(f"{source_name}.{issue.field}: {issue.message}")

        return results

    def generate_quality_report(self, check_result: Dict) -> str:
        """生成质量检查报告"""
        lines = [
            "=" * 70,
            "【报告质量检查】",
            f"报告类型: {check_result['report_type']}",
            f"检查时间: {check_result['timestamp']}",
            "=" * 70,
            ""
        ]

        # 总体状态
        if check_result['overall_valid']:
            lines.append("✅ 整体状态: 通过")
        else:
            lines.append("❌ 整体状态: 未通过")
        lines.append("")

        # 数据源检查
        lines.append("📊 数据源检查:")
        for source_name, source_result in check_result['data_sources'].items():
            status = "✓" if source_result['valid'] else "✗"
            exists = "存在" if source_result['exists'] else "缺失"
            lines.append(f"  {status} {source_name}: {exists}")

            if source_result.get('empty_fields'):
                lines.append(f"      空值字段: {source_result['empty_fields'][:3]}")
            if source_result.get('na_fields'):
                lines.append(f"      NA值字段: {source_result['na_fields'][:3]}")
        lines.append("")

        # 严重问题
        if check_result['critical_issues']:
            lines.append("❌ 严重问题:")
            for issue in check_result['critical_issues']:
                lines.append(f"  • {issue}")
            lines.append("")

        # 警告
        if check_result['warnings']:
            lines.append("⚠️  警告:")
            for warning in check_result['warnings']:
                lines.append(f"  • {warning}")
            lines.append("")

        lines.append("=" * 70)

        return "\n".join(lines)


# 全局验证器实例
_validator: Optional[ReportValidator] = None
_quality_checker: Optional[ReportQualityChecker] = None


def get_validator() -> ReportValidator:
    """获取验证器实例"""
    global _validator
    if _validator is None:
        _validator = ReportValidator()
    return _validator


def get_quality_checker() -> ReportQualityChecker:
    """获取质量检查器实例"""
    global _quality_checker
    if _quality_checker is None:
        _quality_checker = ReportQualityChecker()
    return _quality_checker


def validate_report_data(report_type: str, data: Dict) -> ValidationResult:
    """便捷函数：验证报告数据"""
    return get_validator().validate_data(report_type, data)


def check_report_quality(report_type: str, **data_sources) -> Dict[str, Any]:
    """便捷函数：检查报告质量"""
    return get_quality_checker().check_report_completeness(report_type, **data_sources)
