#!/usr/bin/env python3
"""
Great Expectations 数据质量验证集成

提供声明式的数据质量验证，支持：
- 数据完整性检查
- 数据类型验证
- 数值范围验证
- 业务规则验证
"""
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json

import pandas as pd
import polars as pl
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ExpectationResult:
    """期望验证结果"""
    expectation_type: str
    success: bool
    column: Optional[str]
    unexpected_count: int
    unexpected_percent: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationSuiteResult:
    """验证套件结果"""
    suite_name: str
    success: bool
    results: List[ExpectationResult]
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.success) / len(self.results)
    
    @property
    def failed_expectations(self) -> List[ExpectationResult]:
        """失败的期望"""
        return [r for r in self.results if not r.success]


class GreatExpectationsValidator:
    """
    Great Expectations 风格的验证器
    
    使用声明式期望来验证数据质量
    """
    
    def __init__(self, data_context: Optional[Dict] = None):
        self.data_context = data_context or {}
        self.expectations: List[Dict] = []
        
    def expect_column_to_exist(self, column: str) -> 'GreatExpectationsValidator':
        """期望列存在"""
        self.expectations.append({
            'type': 'expect_column_to_exist',
            'column': column
        })
        return self
    
    def expect_column_values_to_not_be_null(
        self, 
        column: str,
        mostly: float = 1.0
    ) -> 'GreatExpectationsValidator':
        """期望列值不为空"""
        self.expectations.append({
            'type': 'expect_column_values_to_not_be_null',
            'column': column,
            'mostly': mostly
        })
        return self
    
    def expect_column_values_to_be_between(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        mostly: float = 1.0
    ) -> 'GreatExpectationsValidator':
        """期望列值在范围内"""
        self.expectations.append({
            'type': 'expect_column_values_to_be_between',
            'column': column,
            'min_value': min_value,
            'max_value': max_value,
            'mostly': mostly
        })
        return self
    
    def expect_column_values_to_be_of_type(
        self,
        column: str,
        type_: str
    ) -> 'GreatExpectationsValidator':
        """期望列类型"""
        self.expectations.append({
            'type': 'expect_column_values_to_be_of_type',
            'column': column,
            'type_': type_
        })
        return self
    
    def expect_column_pair_values_to_be_equal(
        self,
        column_A: str,
        column_B: str
    ) -> 'GreatExpectationsValidator':
        """期望两列相等"""
        self.expectations.append({
            'type': 'expect_column_pair_values_to_be_equal',
            'column_A': column_A,
            'column_B': column_B
        })
        return self
    
    def expect_column_values_to_be_in_set(
        self,
        column: str,
        value_set: List[Any]
    ) -> 'GreatExpectationsValidator':
        """期望列值在指定集合中"""
        self.expectations.append({
            'type': 'expect_column_values_to_be_in_set',
            'column': column,
            'value_set': value_set
        })
        return self
    
    def expect_table_row_count_to_be_between(
        self,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None
    ) -> 'GreatExpectationsValidator':
        """期望表行数在范围内"""
        self.expectations.append({
            'type': 'expect_table_row_count_to_be_between',
            'min_value': min_value,
            'max_value': max_value
        })
        return self
    
    def expect_compound_columns_to_be_unique(
        self,
        column_list: List[str]
    ) -> 'GreatExpectationsValidator':
        """期望组合列唯一"""
        self.expectations.append({
            'type': 'expect_compound_columns_to_be_unique',
            'column_list': column_list
        })
        return self

    def expect_ohlc_logic(self, mostly: float = 1.0) -> 'GreatExpectationsValidator':
        """
        期望OHLC逻辑正确
        high >= max(open, close) >= min(open, close) >= low
        """
        self.expectations.append({
            'type': 'expect_ohlc_logic',
            'mostly': mostly
        })
        return self

    def _validate_expectation(
        self,
        df: pd.DataFrame,
        expectation: Dict
    ) -> ExpectationResult:
        """验证单个期望"""
        exp_type = expectation['type']
        column = expectation.get('column')
        
        if exp_type == 'expect_column_to_exist':
            success = column in df.columns
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=column,
                unexpected_count=0 if success else 1,
                unexpected_percent=0.0 if success else 100.0
            )
        
        elif exp_type == 'expect_column_values_to_not_be_null':
            mostly = expectation.get('mostly', 1.0)
            null_count = df[column].isnull().sum()
            total_count = len(df)
            null_percent = null_count / total_count
            success = null_percent <= (1 - mostly)
            
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=column,
                unexpected_count=null_count,
                unexpected_percent=null_percent * 100,
                details={'mostly': mostly}
            )
        
        elif exp_type == 'expect_column_values_to_be_between':
            min_val = expectation.get('min_value')
            max_val = expectation.get('max_value')
            mostly = expectation.get('mostly', 1.0)

            series = df[column]
            # 排除 NaN 值后再计算
            non_null_series = series.dropna()
            mask = pd.Series(True, index=non_null_series.index)

            if min_val is not None:
                mask &= (non_null_series >= min_val)
            if max_val is not None:
                mask &= (non_null_series <= max_val)

            valid_count = mask.sum()
            total_count = len(non_null_series)  # 使用非空值总数
            valid_percent = valid_count / total_count if total_count > 0 else 1.0
            success = valid_percent >= mostly
            
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=column,
                unexpected_count=total_count - valid_count,
                unexpected_percent=(1 - valid_percent) * 100,
                details={'min_value': min_val, 'max_value': max_val, 'mostly': mostly}
            )
        
        elif exp_type == 'expect_column_values_to_be_of_type':
            expected_type = expectation.get('type_')
            actual_type = str(df[column].dtype)

            type_mapping = {
                'int': ['int64', 'int32', 'int16', 'int8'],
                'float': ['float64', 'float32', 'int64', 'int32', 'int16', 'int8'],  # 数值类型兼容
                'str': ['object', 'string'],
                'bool': ['bool'],
                'datetime': ['datetime64[ns]']
            }

            success = actual_type in type_mapping.get(expected_type, [expected_type])
            
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=column,
                unexpected_count=0 if success else len(df),
                unexpected_percent=0.0 if success else 100.0,
                details={'expected_type': expected_type, 'actual_type': actual_type}
            )
        
        elif exp_type == 'expect_column_pair_values_to_be_equal':
            col_A = expectation['column_A']
            col_B = expectation['column_B']
            
            equal_count = (df[col_A] == df[col_B]).sum()
            total_count = len(df)
            equal_percent = equal_count / total_count
            success = equal_percent == 1.0
            
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=f"{col_A} == {col_B}",
                unexpected_count=total_count - equal_count,
                unexpected_percent=(1 - equal_percent) * 100
            )
        
        elif exp_type == 'expect_column_values_to_be_in_set':
            value_set = set(expectation['value_set'])
            series = df[column]
            
            in_set_count = series.isin(value_set).sum()
            total_count = len(df)
            in_set_percent = in_set_count / total_count
            success = in_set_percent == 1.0
            
            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=column,
                unexpected_count=total_count - in_set_count,
                unexpected_percent=(1 - in_set_percent) * 100,
                details={'value_set': list(value_set)[:10]}  # 只显示前10个
            )
        
        elif exp_type == 'expect_table_row_count_to_be_between':
            min_val = expectation.get('min_value') or 0
            max_val = expectation.get('max_value') or float('inf')
            row_count = len(df)

            success = min_val <= row_count <= max_val

            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=None,
                unexpected_count=0,
                unexpected_percent=0.0,
                details={'row_count': row_count, 'min_value': min_val, 'max_value': max_val}
            )
        
        elif exp_type == 'expect_compound_columns_to_be_unique':
            column_list = expectation['column_list']

            duplicate_count = df.duplicated(subset=column_list).sum()
            total_count = len(df)
            unique_percent = (total_count - duplicate_count) / total_count
            success = duplicate_count == 0

            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column=','.join(column_list),
                unexpected_count=duplicate_count,
                unexpected_percent=(1 - unique_percent) * 100
            )

        elif exp_type == 'expect_ohlc_logic':
            mostly = expectation.get('mostly', 1.0)

            # OHLC逻辑: high >= max(open, close) >= min(open, close) >= low
            logic_check = (
                (df['high'] >= df[['open', 'close']].max(axis=1)) &
                (df[['open', 'close']].max(axis=1) >= df[['open', 'close']].min(axis=1)) &
                (df[['open', 'close']].min(axis=1) >= df['low'])
            )

            valid_count = logic_check.sum()
            total_count = len(df)
            valid_percent = valid_count / total_count
            success = valid_percent >= mostly

            return ExpectationResult(
                expectation_type=exp_type,
                success=success,
                column='OHLC',
                unexpected_count=total_count - valid_count,
                unexpected_percent=(1 - valid_percent) * 100,
                details={'mostly': mostly}
            )

        else:
            return ExpectationResult(
                expectation_type=exp_type,
                success=False,
                column=column,
                unexpected_count=len(df),
                unexpected_percent=100.0,
                details={'error': f'Unknown expectation type: {exp_type}'}
            )
    
    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化列名，处理列名差异
        将 'date' 映射为 'trade_date' 以兼容不同数据源
        """
        df = df.copy()
        column_mapping = {
            'date': 'trade_date',
            'trade_date': 'trade_date'  # 保持原样
        }

        # 如果存在 'date' 列但不存在 'trade_date' 列，则重命名
        if 'date' in df.columns and 'trade_date' not in df.columns:
            df = df.rename(columns={'date': 'trade_date'})

        return df

    def validate(self, df: pd.DataFrame, suite_name: str = "default") -> ValidationSuiteResult:
        """执行所有期望验证"""
        # 标准化列名
        df = self._normalize_column_names(df)

        results = []
        
        for expectation in self.expectations:
            try:
                result = self._validate_expectation(df, expectation)
                results.append(result)
            except Exception as e:
                logger.error(f"验证期望失败: {expectation}, 错误: {e}")
                results.append(ExpectationResult(
                    expectation_type=expectation.get('type', 'unknown'),
                    success=False,
                    column=expectation.get('column'),
                    unexpected_count=len(df),
                    unexpected_percent=100.0,
                    details={'error': str(e)}
                ))
        
        # 统计信息
        statistics = {
            'total_expectations': len(results),
            'successful_expectations': sum(1 for r in results if r.success),
            'failed_expectations': sum(1 for r in results if not r.success),
            'validation_time': datetime.now().isoformat(),
            'row_count': len(df),
            'column_count': len(df.columns)
        }
        
        return ValidationSuiteResult(
            suite_name=suite_name,
            success=all(r.success for r in results),
            results=results,
            statistics=statistics
        )


class KlineDataQualitySuite:
    """K线数据质量验证套件"""
    
    @staticmethod
    def create_validator() -> GreatExpectationsValidator:
        """创建K线数据验证器"""
        validator = GreatExpectationsValidator()
        
        # 表级验证
        validator.expect_table_row_count_to_be_between(min_value=1)
        
        # 列存在性验证 - 核心列（必需）
        required_columns = [
            'trade_date', 'code', 'open', 'high', 'low',
            'close', 'volume'
        ]
        for col in required_columns:
            validator.expect_column_to_exist(col)

        # 可选列（不强制要求存在）
        # amount: 部分退市/停牌股票可能缺少
        # turnover, pct_chg: 取决于数据源
        optional_columns = ['amount', 'turnover', 'pct_chg']

        # 数据完整性验证
        validator.expect_column_values_to_not_be_null('trade_date')
        validator.expect_column_values_to_not_be_null('code')
        validator.expect_column_values_to_not_be_null('open', mostly=0.99)
        validator.expect_column_values_to_not_be_null('close', mostly=0.99)

        # 数据类型验证
        validator.expect_column_values_to_be_of_type('open', 'float')
        validator.expect_column_values_to_be_of_type('high', 'float')
        validator.expect_column_values_to_be_of_type('low', 'float')
        validator.expect_column_values_to_be_of_type('close', 'float')
        validator.expect_column_values_to_be_of_type('volume', 'float')
        
        # 数值范围验证 - 个股价格范围
        validator.expect_column_values_to_be_between('open', min_value=0, max_value=5000)
        validator.expect_column_values_to_be_between('high', min_value=0, max_value=5000)
        validator.expect_column_values_to_be_between('low', min_value=0, max_value=5000)
        validator.expect_column_values_to_be_between('close', min_value=0, max_value=5000)
        # volume 范围：允许99%的数据在范围内（排除极端异常值）
        # 上限1000亿覆盖所有正常情况（包括大盘股和指数）
        validator.expect_column_values_to_be_between('volume', min_value=0, max_value=1e12, mostly=0.99)

        # 业务逻辑验证
        validator.expect_compound_columns_to_be_unique(['trade_date', 'code'])
        validator.expect_ohlc_logic(mostly=0.99)  # 99%的数据满足OHLC逻辑

        return validator


class StockListQualitySuite:
    """股票列表数据质量验证套件"""

    @staticmethod
    def create_validator() -> GreatExpectationsValidator:
        """创建股票列表验证器"""
        validator = GreatExpectationsValidator()

        # 表级验证
        validator.expect_table_row_count_to_be_between(min_value=1000, max_value=10000)

        # 列存在性
        validator.expect_column_to_exist('code')
        validator.expect_column_to_exist('name')

        # 数据完整性
        validator.expect_column_values_to_not_be_null('code')
        validator.expect_column_values_to_not_be_null('name', mostly=0.95)

        # 代码格式验证 (6位数字)
        validator.expect_column_values_to_be_of_type('code', 'str')

        # 唯一性验证 - 使用组合键 (code + name)
        # 因为股票列表可能同时包含指数和个股，代码可能重复
        validator.expect_compound_columns_to_be_unique(['code', 'name'])

        return validator


def validate_kline_data(file_path: Path) -> ValidationSuiteResult:
    """验证K线数据文件"""
    logger.info(f"验证K线数据: {file_path}")
    
    try:
        df = pl.read_parquet(file_path).to_pandas()
    except Exception as e:
        logger.error(f"读取文件失败: {e}")
        return ValidationSuiteResult(
            suite_name="kline_validation",
            success=False,
            results=[],
            statistics={'error': str(e)}
        )
    
    validator = KlineDataQualitySuite.create_validator()
    result = validator.validate(df, suite_name=f"kline_{file_path.stem}")
    
    return result


def validate_all_kline_data(kline_dir: Path, sample_size: int = 10) -> Dict[str, ValidationSuiteResult]:
    """验证所有K线数据（抽样）"""
    results = {}
    
    parquet_files = list(kline_dir.glob("*.parquet"))
    
    # 抽样
    if len(parquet_files) > sample_size:
        import random
        random.seed(42)
        parquet_files = random.sample(parquet_files, sample_size)
    
    logger.info(f"抽样验证 {len(parquet_files)} 个K线数据文件")
    
    for file_path in parquet_files:
        result = validate_kline_data(file_path)
        results[file_path.stem] = result
        
        if result.success:
            logger.info(f"✅ {file_path.stem}: 通过")
        else:
            logger.warning(f"❌ {file_path.stem}: 失败 - {len(result.failed_expectations)} 个期望未通过")
    
    return results


def generate_quality_report(results: Dict[str, ValidationSuiteResult], output_path: Path):
    """生成质量报告"""
    report = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_files': len(results),
            'passed_files': sum(1 for r in results.values() if r.success),
            'failed_files': sum(1 for r in results.values() if not r.success),
            'overall_success_rate': sum(r.success_rate for r in results.values()) / len(results) if results else 0
        },
        'details': {}
    }
    
    for code, result in results.items():
        report['details'][code] = {
            'success': result.success,
            'success_rate': result.success_rate,
            'statistics': result.statistics,
            'failed_expectations': [
                {
                    'type': r.expectation_type,
                    'column': r.column,
                    'unexpected_percent': r.unexpected_percent
                }
                for r in result.failed_expectations
            ]
        }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"质量报告已生成: {output_path}")
    return report


if __name__ == "__main__":
    # 示例用法
    from pathlib import Path
    
    # 验证单个文件
    kline_file = Path("data/kline/000001.parquet")
    if kline_file.exists():
        result = validate_kline_data(kline_file)
        print(f"\n验证结果: {'✅ 通过' if result.success else '❌ 失败'}")
        print(f"成功率: {result.success_rate:.1%}")
        print(f"统计: {result.statistics}")
        
        if result.failed_expectations:
            print("\n失败的期望:")
            for r in result.failed_expectations:
                print(f"  - {r.expectation_type} ({r.column}): {r.unexpected_percent:.1f}% 未通过")
