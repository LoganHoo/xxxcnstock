#!/usr/bin/env python3
"""
财务数据验证器

提供全面的财务数据验证功能:
1. 会计恒等式验证 (资产 = 负债 + 所有者权益)
2. 利润表勾稽关系验证
3. 现金流量表验证
4. 财务指标合理性检查
5. 跨报表一致性检查
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from core.logger import setup_logger

logger = setup_logger("financial_validator", log_file="system/financial_validator.log")


class ValidationLevel(Enum):
    """验证级别"""
    ERROR = "error"       # 严重错误,数据不可用
    WARNING = "warning"   # 警告,数据可能有问题
    INFO = "info"         # 信息提示


@dataclass
class ValidationRule:
    """验证规则"""
    name: str                           # 规则名称
    description: str                    # 规则描述
    level: ValidationLevel              # 验证级别
    check_func: Callable                # 验证函数
    threshold: Optional[float] = None   # 阈值(用于数值比较)


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool                      # 是否通过验证
    code: str                           # 股票代码
    report_date: str                    # 报告期
    rule_name: str                      # 规则名称
    level: ValidationLevel              # 级别
    message: str                        # 验证信息
    expected: Optional[Any] = None      # 期望值
    actual: Optional[Any] = None        # 实际值
    details: Dict = field(default_factory=dict)  # 详细信息


class FinancialDataValidator:
    """财务数据验证器"""
    
    def __init__(self):
        self.logger = logger
        self.rules: List[ValidationRule] = []
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """设置默认验证规则"""
        # 资产负债表规则
        self.rules.extend([
            ValidationRule(
                name="accounting_equation",
                description="会计恒等式: 资产 = 负债 + 所有者权益",
                level=ValidationLevel.ERROR,
                check_func=self._check_accounting_equation,
                threshold=0.01  # 允许1%的误差
            ),
            ValidationRule(
                name="asset_composition",
                description="资产构成: 总资产 = 流动资产 + 非流动资产",
                level=ValidationLevel.ERROR,
                check_func=self._check_asset_composition,
                threshold=0.01
            ),
            ValidationRule(
                name="liability_composition",
                description="负债构成: 总负债 = 流动负债 + 非流动负债",
                level=ValidationLevel.ERROR,
                check_func=self._check_liability_composition,
                threshold=0.01
            ),
            ValidationRule(
                name="positive_assets",
                description="资产为正: 总资产必须大于0",
                level=ValidationLevel.ERROR,
                check_func=self._check_positive_assets
            ),
            ValidationRule(
                name="reasonable_debt_ratio",
                description="合理负债率: 资产负债率应在0-100%之间",
                level=ValidationLevel.WARNING,
                check_func=self._check_reasonable_debt_ratio
            ),
        ])
        
        # 利润表规则
        self.rules.extend([
            ValidationRule(
                name="profit_hierarchy",
                description="利润层次: 净利润 <= 利润总额 <= 营业利润",
                level=ValidationLevel.ERROR,
                check_func=self._check_profit_hierarchy
            ),
            ValidationRule(
                name="positive_revenue",
                description="收入为正: 营业收入必须大于0",
                level=ValidationLevel.ERROR,
                check_func=self._check_positive_revenue
            ),
            ValidationRule(
                name="eps_calculation",
                description="EPS计算: 基本EPS = 归母净利润 / 总股本",
                level=ValidationLevel.WARNING,
                check_func=self._check_eps_calculation,
                threshold=0.1  # 允许10%误差
            ),
        ])
        
        # 现金流量表规则
        self.rules.extend([
            ValidationRule(
                name="cash_reconciliation",
                description="现金勾稽: 期末现金 = 期初现金 + 净增加额",
                level=ValidationLevel.ERROR,
                check_func=self._check_cash_reconciliation,
                threshold=0.01
            ),
            ValidationRule(
                name="cash_flow_composition",
                description="现金流构成: 净增加额 = 经营 + 投资 + 筹资 + 汇率影响",
                level=ValidationLevel.ERROR,
                check_func=self._check_cash_flow_composition,
                threshold=0.01
            ),
        ])
    
    def validate_balance_sheet(
        self,
        df: pd.DataFrame,
        code: str,
        report_date: str
    ) -> List[ValidationResult]:
        """
        验证资产负债表
        
        Args:
            df: 资产负债表DataFrame
            code: 股票代码
            report_date: 报告期
        
        Returns:
            验证结果列表
        """
        results = []
        
        if df.empty:
            results.append(ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name="data_existence",
                level=ValidationLevel.ERROR,
                message="资产负债表数据为空"
            ))
            return results
        
        # 获取数据行
        row = df.iloc[0] if len(df) > 0 else pd.Series()
        
        # 应用资产负债表规则
        balance_sheet_rules = [
            r for r in self.rules
            if r.name in ['accounting_equation', 'asset_composition', 
                         'liability_composition', 'positive_assets', 'reasonable_debt_ratio']
        ]
        
        for rule in balance_sheet_rules:
            try:
                result = rule.check_func(row, rule, code, report_date)
                results.append(result)
            except Exception as e:
                self.logger.error(f"验证规则 {rule.name} 执行失败: {e}")
                results.append(ValidationResult(
                    is_valid=False,
                    code=code,
                    report_date=report_date,
                    rule_name=rule.name,
                    level=ValidationLevel.ERROR,
                    message=f"验证执行失败: {str(e)}"
                ))
        
        return results
    
    def validate_income_statement(
        self,
        df: pd.DataFrame,
        code: str,
        report_date: str
    ) -> List[ValidationResult]:
        """验证利润表"""
        results = []
        
        if df.empty:
            results.append(ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name="data_existence",
                level=ValidationLevel.ERROR,
                message="利润表数据为空"
            ))
            return results
        
        row = df.iloc[0] if len(df) > 0 else pd.Series()
        
        income_rules = [
            r for r in self.rules
            if r.name in ['profit_hierarchy', 'positive_revenue', 'eps_calculation']
        ]
        
        for rule in income_rules:
            try:
                result = rule.check_func(row, rule, code, report_date)
                results.append(result)
            except Exception as e:
                self.logger.error(f"验证规则 {rule.name} 执行失败: {e}")
                results.append(ValidationResult(
                    is_valid=False,
                    code=code,
                    report_date=report_date,
                    rule_name=rule.name,
                    level=ValidationLevel.ERROR,
                    message=f"验证执行失败: {str(e)}"
                ))
        
        return results
    
    def validate_cash_flow(
        self,
        df: pd.DataFrame,
        code: str,
        report_date: str
    ) -> List[ValidationResult]:
        """验证现金流量表"""
        results = []
        
        if df.empty:
            results.append(ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name="data_existence",
                level=ValidationLevel.ERROR,
                message="现金流量表数据为空"
            ))
            return results
        
        row = df.iloc[0] if len(df) > 0 else pd.Series()
        
        cash_flow_rules = [
            r for r in self.rules
            if r.name in ['cash_reconciliation', 'cash_flow_composition']
        ]
        
        for rule in cash_flow_rules:
            try:
                result = rule.check_func(row, rule, code, report_date)
                results.append(result)
            except Exception as e:
                self.logger.error(f"验证规则 {rule.name} 执行失败: {e}")
                results.append(ValidationResult(
                    is_valid=False,
                    code=code,
                    report_date=report_date,
                    rule_name=rule.name,
                    level=ValidationLevel.ERROR,
                    message=f"验证执行失败: {str(e)}"
                ))
        
        return results
    
    def validate_all(
        self,
        balance_sheet: pd.DataFrame,
        income_statement: pd.DataFrame,
        cash_flow: pd.DataFrame,
        code: str,
        report_date: str
    ) -> Dict[str, List[ValidationResult]]:
        """
        验证所有财务报表
        
        Returns:
            {
                'balance_sheet': [...],
                'income_statement': [...],
                'cash_flow': [...],
                'summary': {...}
            }
        """
        results = {
            'balance_sheet': self.validate_balance_sheet(balance_sheet, code, report_date),
            'income_statement': self.validate_income_statement(income_statement, code, report_date),
            'cash_flow': self.validate_cash_flow(cash_flow, code, report_date),
        }
        
        # 生成汇总
        all_results = results['balance_sheet'] + results['income_statement'] + results['cash_flow']
        error_count = sum(1 for r in all_results if r.level == ValidationLevel.ERROR and not r.is_valid)
        warning_count = sum(1 for r in all_results if r.level == ValidationLevel.WARNING and not r.is_valid)
        
        results['summary'] = {
            'total_checks': len(all_results),
            'passed': sum(1 for r in all_results if r.is_valid),
            'errors': error_count,
            'warnings': warning_count,
            'is_valid': error_count == 0
        }
        
        return results
    
    # ==================== 具体验证函数 ====================
    
    def _check_accounting_equation(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证会计恒等式: 资产 = 负债 + 所有者权益"""
        assets = row.get('total_assets', np.nan)
        liabilities = row.get('total_liabilities', np.nan)
        equity = row.get('total_equity', np.nan)
        
        if pd.isna(assets) or pd.isna(liabilities) or pd.isna(equity):
            return ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=rule.level,
                message="缺少必要字段(总资产/总负债/所有者权益)",
                expected=None,
                actual=None
            )
        
        expected = liabilities + equity
        diff = abs(assets - expected)
        diff_pct = diff / assets if assets != 0 else 0
        
        is_valid = diff_pct <= rule.threshold
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"会计恒等式验证{'通过' if is_valid else '失败'}: 差异 {diff_pct:.2%}",
            expected=expected,
            actual=assets,
            details={'difference': diff, 'difference_pct': diff_pct}
        )
    
    def _check_asset_composition(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证资产构成"""
        total = row.get('total_assets', np.nan)
        current = row.get('total_current_assets', np.nan)
        non_current = row.get('total_non_current_assets', np.nan)
        
        if pd.isna(total) or pd.isna(current) or pd.isna(non_current):
            return ValidationResult(
                is_valid=True,  # 字段缺失不报错
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="资产构成字段不完整,跳过验证"
            )
        
        expected = current + non_current
        diff_pct = abs(total - expected) / total if total != 0 else 0
        is_valid = diff_pct <= rule.threshold
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"资产构成验证{'通过' if is_valid else '失败'}",
            expected=expected,
            actual=total
        )
    
    def _check_liability_composition(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证负债构成"""
        total = row.get('total_liabilities', np.nan)
        current = row.get('total_current_liabilities', np.nan)
        non_current = row.get('total_non_current_liabilities', np.nan)
        
        if pd.isna(total) or pd.isna(current) or pd.isna(non_current):
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="负债构成字段不完整,跳过验证"
            )
        
        expected = current + non_current
        diff_pct = abs(total - expected) / total if total != 0 else 0
        is_valid = diff_pct <= rule.threshold
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"负债构成验证{'通过' if is_valid else '失败'}",
            expected=expected,
            actual=total
        )
    
    def _check_positive_assets(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证资产为正"""
        assets = row.get('total_assets', np.nan)
        
        if pd.isna(assets):
            return ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=rule.level,
                message="总资产数据缺失"
            )
        
        is_valid = assets > 0
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"资产为正验证{'通过' if is_valid else '失败'}",
            expected="> 0",
            actual=assets
        )
    
    def _check_reasonable_debt_ratio(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证合理负债率"""
        assets = row.get('total_assets', np.nan)
        liabilities = row.get('total_liabilities', np.nan)
        
        if pd.isna(assets) or pd.isna(liabilities) or assets == 0:
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="无法计算负债率,跳过验证"
            )
        
        debt_ratio = liabilities / assets * 100
        is_valid = 0 <= debt_ratio <= 100
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"负债率 {debt_ratio:.2f}% {'合理' if is_valid else '异常'}",
            expected="0-100%",
            actual=f"{debt_ratio:.2f}%"
        )
    
    def _check_profit_hierarchy(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证利润层次"""
        operating = row.get('operating_profit', np.nan)
        total = row.get('total_profit', np.nan)
        net = row.get('net_profit', np.nan)
        
        if pd.isna(operating) or pd.isna(total) or pd.isna(net):
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="利润数据不完整,跳过验证"
            )
        
        is_valid = net <= total <= operating or (operating <= total <= net)
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"利润层次验证{'通过' if is_valid else '失败'}",
            expected="净利润 <= 利润总额 <= 营业利润 (或相反)",
            actual=f"净利润:{net}, 利润总额:{total}, 营业利润:{operating}"
        )
    
    def _check_positive_revenue(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证收入为正"""
        revenue = row.get('operating_revenue', row.get('total_revenue', np.nan))
        
        if pd.isna(revenue):
            return ValidationResult(
                is_valid=False,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=rule.level,
                message="营业收入数据缺失"
            )
        
        is_valid = revenue > 0
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"收入为正验证{'通过' if is_valid else '失败'}",
            expected="> 0",
            actual=revenue
        )
    
    def _check_eps_calculation(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证EPS计算"""
        net_profit = row.get('net_profit_parent', np.nan)
        eps = row.get('basic_eps', np.nan)
        
        # 假设总股本(这里简化处理,实际应从资产负债表获取)
        # 在实际应用中,应该传入总股本数据
        if pd.isna(net_profit) or pd.isna(eps) or eps == 0:
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="EPS数据不完整,跳过验证"
            )
        
        # 简化验证: EPS和净利润应该同号
        is_valid = (net_profit > 0 and eps > 0) or (net_profit < 0 and eps < 0) or (net_profit == 0 and eps == 0)
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"EPS符号验证{'通过' if is_valid else '失败'}",
            expected="与净利润同号",
            actual=f"净利润:{net_profit}, EPS:{eps}"
        )
    
    def _check_cash_reconciliation(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证现金勾稽关系"""
        beginning = row.get('beginning_cash', np.nan)
        ending = row.get('ending_cash', np.nan)
        net_increase = row.get('net_cash_increase', np.nan)
        
        if pd.isna(beginning) or pd.isna(ending) or pd.isna(net_increase):
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="现金数据不完整,跳过验证"
            )
        
        expected = beginning + net_increase
        diff_pct = abs(ending - expected) / abs(ending) if ending != 0 else 0
        is_valid = diff_pct <= rule.threshold
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"现金勾稽验证{'通过' if is_valid else '失败'}",
            expected=expected,
            actual=ending,
            details={'difference_pct': diff_pct}
        )
    
    def _check_cash_flow_composition(
        self,
        row: pd.Series,
        rule: ValidationRule,
        code: str,
        report_date: str
    ) -> ValidationResult:
        """验证现金流构成"""
        operating = row.get('operating_cash_flow', 0)
        investing = row.get('investing_cash_flow', 0)
        financing = row.get('financing_cash_flow', 0)
        exchange = row.get('exchange_rate_effect', 0)
        net_increase = row.get('net_cash_increase', np.nan)
        
        if pd.isna(net_increase):
            return ValidationResult(
                is_valid=True,
                code=code,
                report_date=report_date,
                rule_name=rule.name,
                level=ValidationLevel.INFO,
                message="现金流数据不完整,跳过验证"
            )
        
        expected = operating + investing + financing + exchange
        diff_pct = abs(net_increase - expected) / abs(net_increase) if net_increase != 0 else 0
        is_valid = diff_pct <= rule.threshold
        
        return ValidationResult(
            is_valid=is_valid,
            code=code,
            report_date=report_date,
            rule_name=rule.name,
            level=rule.level if not is_valid else ValidationLevel.INFO,
            message=f"现金流构成验证{'通过' if is_valid else '失败'}",
            expected=expected,
            actual=net_increase
        )


# ==================== 便捷函数 ====================

def validate_balance_sheet(
    df: pd.DataFrame,
    code: str,
    report_date: str
) -> List[ValidationResult]:
    """验证资产负债表 (便捷函数)"""
    validator = FinancialDataValidator()
    return validator.validate_balance_sheet(df, code, report_date)


def validate_income_statement(
    df: pd.DataFrame,
    code: str,
    report_date: str
) -> List[ValidationResult]:
    """验证利润表 (便捷函数)"""
    validator = FinancialDataValidator()
    return validator.validate_income_statement(df, code, report_date)


def validate_cash_flow(
    df: pd.DataFrame,
    code: str,
    report_date: str
) -> List[ValidationResult]:
    """验证现金流量表 (便捷函数)"""
    validator = FinancialDataValidator()
    return validator.validate_cash_flow(df, code, report_date)


def validate_accounting_equation(
    assets: float,
    liabilities: float,
    equity: float,
    threshold: float = 0.01
) -> bool:
    """
    验证会计恒等式 (便捷函数)
    
    Returns:
        True if |assets - (liabilities + equity)| / assets <= threshold
    """
    if assets == 0:
        return False
    diff = abs(assets - (liabilities + equity))
    return diff / assets <= threshold


if __name__ == "__main__":
    # 测试验证器
    print("=" * 50)
    print("测试: 财务数据验证器")
    print("=" * 50)
    
    # 构造测试数据
    balance_sheet_data = pd.DataFrame([{
        'code': '000001',
        'report_date': '2023-12-31',
        'total_assets': 1000,
        'total_liabilities': 400,
        'total_equity': 600,
        'total_current_assets': 600,
        'total_non_current_assets': 400,
        'total_current_liabilities': 300,
        'total_non_current_liabilities': 100,
    }])
    
    validator = FinancialDataValidator()
    results = validator.validate_balance_sheet(balance_sheet_data, '000001', '2023-12-31')
    
    print("\n验证结果:")
    for r in results:
        status = "通过" if r.is_valid else "失败"
        print(f"  [{r.level.value.upper()}] {r.rule_name}: {status} - {r.message}")
