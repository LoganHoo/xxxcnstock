#!/usr/bin/env python3
"""
GE-based 数据质量检查点验证器（带重试机制）

所有数据质量检查都使用 Great Expectations
支持失败重试
"""
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import time
import re
import uuid

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
import pandas as pd
import numpy as np
import great_expectations as ge
import great_expectations.expectations as gxe
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from core.logger import setup_logger


class CheckStatus(Enum):
    """检查状态"""
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class CheckResult:
    """检查结果"""
    checkpoint: str
    status: CheckStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0
    ge_results: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'checkpoint': self.checkpoint,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp,
            'retry_count': self.retry_count,
            'ge_results': self.ge_results
        }


class GERetryConfig:
    """GE重试配置"""
    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        backoff_factor: float = 2.0,
        retry_on_status: List[CheckStatus] = None
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.retry_on_status = retry_on_status or [CheckStatus.FAILED, CheckStatus.RETRY]


class GECheckpointValidators:
    """GE-based 数据质量检查点验证器"""

    def __init__(self, retry_config: GERetryConfig = None):
        self.logger = setup_logger("ge_checkpoint_validators")
        self.retry_config = retry_config or GERetryConfig()
        self.results: List[CheckResult] = []
        self._context = None

    def _get_context(self):
        """获取或创建 GE DataContext"""
        if self._context is None:
            self._context = ge.get_context(mode="ephemeral")
        return self._context

    def _run_with_retry(
        self,
        check_func: Callable,
        *args,
        **kwargs
    ) -> CheckResult:
        """带重试机制的执行"""
        last_result = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                result = check_func(*args, **kwargs)
                result.retry_count = attempt

                if result.status not in self.retry_config.retry_on_status:
                    return result

                last_result = result

                if attempt < self.retry_config.max_retries:
                    delay = self.retry_config.retry_delay * (self.retry_config.backoff_factor ** attempt)
                    self.logger.warning(
                        f"{result.checkpoint} 检查失败，{delay:.1f}秒后重试 ({attempt + 1}/{self.retry_config.max_retries})"
                    )
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(f"检查执行异常: {e}")
                last_result = CheckResult(
                    checkpoint="unknown",
                    status=CheckStatus.FAILED,
                    message=f"执行异常: {str(e)}",
                    retry_count=attempt
                )

                if attempt < self.retry_config.max_retries:
                    delay = self.retry_config.retry_delay * (self.retry_config.backoff_factor ** attempt)
                    time.sleep(delay)

        return last_result

    def _create_suite_and_validate(
        self,
        df,
        expectations: List[Dict[str, Any]],
        suite_name: str
    ) -> Tuple[CheckStatus, str, Dict]:
        """
        创建期望套件并执行验证

        Args:
            df: 数据 (pandas DataFrame 或 polars DataFrame)
            expectations: 期望配置列表
            suite_name: 套件名称

        Returns:
            (状态, 消息, 详细结果)
        """
        try:
            # 转换 polars DataFrame 为 pandas
            if hasattr(df, 'to_pandas'):
                df = df.to_pandas()
            elif hasattr(df, 'toPandas'):
                df = df.toPandas()

            context = self._get_context()

            # 创建期望套件
            suite = context.suites.add(ExpectationSuite(name=suite_name))

            # 添加期望
            for exp_config in expectations:
                exp_type = exp_config.pop('type')
                if exp_type == 'expect_table_row_count_to_be_between':
                    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(**exp_config))
                elif exp_type == 'expect_column_to_exist':
                    suite.add_expectation(gxe.ExpectColumnToExist(**exp_config))
                elif exp_type == 'expect_column_values_to_not_be_null':
                    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(**exp_config))
                elif exp_type == 'expect_column_values_to_be_between':
                    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(**exp_config))
                elif exp_type == 'expect_column_values_to_match_regex':
                    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(**exp_config))
                elif exp_type == 'expect_table_columns_to_match_ordered_list':
                    suite.add_expectation(gxe.ExpectTableColumnsToMatchOrderedList(**exp_config))
                elif exp_type == 'expect_column_mean_to_be_between':
                    suite.add_expectation(gxe.ExpectColumnMeanToBeBetween(**exp_config))
                elif exp_type == 'expect_column_stdev_to_be_between':
                    suite.add_expectation(gxe.ExpectColumnStdevToBeBetween(**exp_config))

            # 创建 datasource 和 batch
            datasource = context.data_sources.add_pandas(f"{suite_name}_datasource")
            data_asset = datasource.add_dataframe_asset(name=f"{suite_name}_asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")
            batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

            # 执行验证
            validator = context.get_validator(
                batch_request=batch.batch_request,
                expectation_suite=suite
            )
            ge_result = validator.validate()

            # 解析结果
            success_rate = ge_result.statistics.get('success_percent', 0) or 0
            if success_rate == 0 and ge_result.statistics.get('evaluated_expectations', 0) > 0:
                # 重新计算成功率
                successful = ge_result.statistics.get('successful_expectations', 0)
                evaluated = ge_result.statistics.get('evaluated_expectations', 0)
                success_rate = (successful / evaluated * 100) if evaluated > 0 else 0
            evaluated = ge_result.statistics.get('evaluated_expectations', 0)
            successful = ge_result.statistics.get('successful_expectations', 0)

            details = {
                'success_rate': success_rate,
                'evaluated_expectations': evaluated,
                'successful_expectations': successful,
                'unsuccessful_expectations': ge_result.statistics.get('unsuccessful_expectations', 0),
            }

            if success_rate >= 95:
                status = CheckStatus.PASSED
                message = f"GE验证通过: {success_rate:.1f}%"
            elif success_rate >= 80:
                status = CheckStatus.WARNING
                message = f"GE验证警告: {success_rate:.1f}%"
            else:
                status = CheckStatus.FAILED
                message = f"GE验证失败: {success_rate:.1f}%"

            return status, message, details

        except Exception as e:
            self.logger.error(f"GE验证异常: {e}")
            return CheckStatus.FAILED, f"GE验证异常: {str(e)}", {'error': str(e)}

    # ========== 检查点1: 采集前检查 (GE增强版) ==========

    def pre_collection_check(
        self,
        date: str,
        data_service_health_check: Callable = None,
        min_storage_gb: float = 1.0
    ) -> CheckResult:
        """
        采集前检查 - GE增强版

        使用GE验证系统状态数据
        """
        def _check():
            self.logger.info(f"执行采集前检查: {date}")

            # 1. 市场状态检查
            try:
                from core.market_guardian import enforce_market_closed
                enforce_market_closed(target_date=datetime.strptime(date, '%Y-%m-%d'))
            except SystemExit:
                return CheckResult(
                    checkpoint="pre_collection_check",
                    status=CheckStatus.FAILED,
                    message="市场未收盘，不能采集当日数据",
                    details={'date': date}
                )
            except Exception as e:
                self.logger.warning(f"市场状态检查异常: {e}")

            # 2. 数据源可用性检查
            if data_service_health_check:
                try:
                    if not data_service_health_check():
                        return CheckResult(
                            checkpoint="pre_collection_check",
                            status=CheckStatus.FAILED,
                            message="数据源不可用",
                            details={'health_check': 'failed'}
                        )
                except Exception as e:
                    self.logger.warning(f"数据源健康检查异常: {e}")

            # 3. 存储空间检查
            try:
                import shutil
                stat = shutil.disk_usage("/Volumes/Xdata")
                free_gb = stat.free / (1024**3)
                if free_gb < min_storage_gb:
                    return CheckResult(
                        checkpoint="pre_collection_check",
                        status=CheckStatus.FAILED,
                        message=f"存储空间不足: {free_gb:.1f}GB < {min_storage_gb}GB",
                        details={'free_gb': free_gb, 'required_gb': min_storage_gb}
                    )
            except Exception as e:
                self.logger.warning(f"存储空间检查异常: {e}")

            return CheckResult(
                checkpoint="pre_collection_check",
                status=CheckStatus.PASSED,
                message="采集前检查通过",
                details={'date': date}
            )

        return self._run_with_retry(_check)

    # ========== 检查点2: 采集后验证 (GE完全版) ==========

    def post_collection_validation(
        self,
        data: pl.DataFrame,
        data_type: str = "kline",
        min_quality_score: float = 80.0
    ) -> CheckResult:
        """
        采集后验证 - GE完全版

        所有验证都通过GE期望套件执行
        """
        def _check():
            self.logger.info(f"执行采集后GE验证: {data_type}, 行数={len(data)}")

            if len(data) == 0:
                return CheckResult(
                    checkpoint="post_collection_validation",
                    status=CheckStatus.FAILED,
                    message="数据为空",
                    details={'data_type': data_type}
                )

            # 转换为pandas
            df_pd = data.to_pandas()

            # 根据数据类型定义GE期望
            if data_type == "stock_list":
                expectations = [
                    {'type': 'expect_table_row_count_to_be_between', 'min_value': 1000, 'max_value': 10000},
                    {'type': 'expect_column_to_exist', 'column': 'code'},
                    {'type': 'expect_column_to_exist', 'column': 'name'},
                    {'type': 'expect_column_values_to_not_be_null', 'column': 'code'},
                    {'type': 'expect_column_values_to_match_regex', 'column': 'code', 'regex': r'^\d{6}$'},
                ]
            elif data_type == "kline":
                expectations = [
                    {'type': 'expect_table_row_count_to_be_between', 'min_value': 1, 'max_value': 10000},
                    {'type': 'expect_column_to_exist', 'column': 'code'},
                    {'type': 'expect_column_to_exist', 'column': 'trade_date'},
                    {'type': 'expect_column_to_exist', 'column': 'close'},
                    {'type': 'expect_column_values_to_not_be_null', 'column': 'trade_date'},
                    {'type': 'expect_column_values_to_not_be_null', 'column': 'close'},
                    {'type': 'expect_column_values_to_be_between', 'column': 'close', 'min_value': 0.01, 'max_value': 10000},
                ]
            else:
                expectations = [
                    {'type': 'expect_table_row_count_to_be_between', 'min_value': 1, 'max_value': 100000},
                ]

            # 执行GE验证
            suite_name = f"{data_type}_collection_suite_{uuid.uuid4().hex[:8]}"
            status, message, ge_details = self._create_suite_and_validate(
                df_pd, expectations, suite_name
            )

            # 计算综合质量分数
            quality_score = ge_details.get('success_rate', 0)

            # 数据新鲜度检查
            freshness_score = self._check_data_freshness(data, data_type)

            # 综合评分
            final_score = (quality_score * 0.7) + (freshness_score * 0.3)

            details = {
                'data_type': data_type,
                'rows': len(data),
                'columns': len(data.columns),
                'ge_success_rate': quality_score,
                'freshness_score': freshness_score,
                'final_quality_score': final_score,
                **ge_details
            }

            if final_score >= min_quality_score:
                final_status = CheckStatus.PASSED
                final_message = f"GE验证通过: {final_score:.1f}分"
            elif final_score >= min_quality_score * 0.8:
                final_status = CheckStatus.WARNING
                final_message = f"GE验证警告: {final_score:.1f}分"
            else:
                final_status = CheckStatus.FAILED
                final_message = f"GE验证失败: {final_score:.1f}分 < {min_quality_score}分"

            return CheckResult(
                checkpoint="post_collection_validation",
                status=final_status,
                message=final_message,
                details=details,
                ge_results=[ge_details]
            )

        return self._run_with_retry(_check)

    # ========== 检查点3: 计算前检查 (快速检查版) ==========

    def pre_scoring_check(
        self,
        df: pl.DataFrame,
        code: str,
        min_rows: int = 20,
        max_missing_ratio: float = 0.1
    ) -> CheckResult:
        """
        计算前检查 - 快速版

        使用简单检查（非GE）以提高性能
        在批量评分时避免对每个股票都执行GE验证
        """
        # 基础检查 - 数据行数
        if len(df) < min_rows:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 数据不足{min_rows}天",
                details={'code': code, 'rows': len(df), 'required': min_rows}
            )

        # 必要列检查
        required_cols = ['close', 'volume', 'high', 'low']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 缺少必要列 {missing_cols}",
                details={'code': code, 'missing_cols': missing_cols}
            )

        # 缺失值检查
        for col in required_cols:
            null_count = df[col].null_count()
            null_ratio = null_count / len(df) if len(df) > 0 else 1
            if null_ratio > max_missing_ratio:
                return CheckResult(
                    checkpoint="pre_scoring_check",
                    status=CheckStatus.FAILED,
                    message=f"{code}: {col}缺失值比例{null_ratio:.1%}过高",
                    details={'code': code, 'column': col, 'null_ratio': null_ratio}
                )

        # 数据合理性检查
        close_values = df['close'].drop_nulls()
        if len(close_values) == 0 or close_values.min() <= 0:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 收盘价数据异常",
                details={'code': code}
            )

        return CheckResult(
            checkpoint="pre_scoring_check",
            status=CheckStatus.PASSED,
            message=f"{code}: 数据检查通过",
            details={'code': code, 'rows': len(df)}
        )

    # ========== 检查点4: 计算后验证 (GE版) ==========

    def post_scoring_validation(
        self,
        scores_df: pl.DataFrame,
        min_stocks: int = 10,
        score_range: Tuple[float, float] = (0, 100),
        min_std: float = 1.0
    ) -> CheckResult:
        """
        计算后验证 - GE版

        对整个评分结果集执行GE验证（不是每只股票）
        """
        def _check():
            if len(scores_df) == 0:
                return CheckResult(
                    checkpoint="post_scoring_validation",
                    status=CheckStatus.FAILED,
                    message="评分结果为空",
                    details={'total_stocks': 0}
                )

            df_pd = scores_df.to_pandas()
            score_col = 'enhanced_score' if 'enhanced_score' in scores_df.columns else 'total_score'

            expectations = [
                {'type': 'expect_table_row_count_to_be_between', 'min_value': min_stocks, 'max_value': 10000},
                {'type': 'expect_column_to_exist', 'column': score_col},
                {'type': 'expect_column_values_to_be_between', 'column': score_col, 'min_value': score_range[0], 'max_value': score_range[1]},
            ]

            suite_name = f"post_scoring_suite_{uuid.uuid4().hex[:8]}"
            status, message, ge_details = self._create_suite_and_validate(
                df_pd, expectations, suite_name
            )

            # 额外检查：评分分布标准差
            if score_col in scores_df.columns:
                score_std = scores_df[score_col].std()
                if score_std < min_std:
                    status = CheckStatus.WARNING
                    message = f"评分分布异常集中 (std={score_std:.2f})"

            return CheckResult(
                checkpoint="post_scoring_validation",
                status=status,
                message=message,
                details={'total_stocks': len(scores_df), **ge_details},
                ge_results=[ge_details]
            )

        return self._run_with_retry(_check)

    # ========== 检查点5: 选股前检查 (GE版) ==========

    def pre_selection_check(
        self,
        stock_pool: pd.DataFrame,
        date: str,
        min_pool_size: int = 100,
        required_cols: List[str] = None
    ) -> CheckResult:
        """
        选股前检查 - GE版
        
        注意：选股前检查只验证股票池基本信息，不验证价格数据
        价格数据在 _load_stock_data 后验证
        """
        # 在函数外部处理默认值，避免闭包变量问题
        # 选股前只检查 code 和 name，close/volume 在数据加载后检查
        cols = required_cols or ['code', 'name']
        
        def _check():
            nonlocal cols

            expectations = [
                {'type': 'expect_table_row_count_to_be_between', 'min_value': min_pool_size, 'max_value': 10000},
            ]

            for col in cols:
                expectations.append({'type': 'expect_column_to_exist', 'column': col})

            suite_name = f"pre_selection_suite_{uuid.uuid4().hex[:8]}"
            status, message, ge_details = self._create_suite_and_validate(
                stock_pool, expectations, suite_name
            )

            return CheckResult(
                checkpoint="pre_selection_check",
                status=status,
                message=message,
                details={'pool_size': len(stock_pool), 'date': date, **ge_details},
                ge_results=[ge_details]
            )

        return self._run_with_retry(_check)

    # ========== 检查点6: 最终输出验证 (GE版) ==========

    def final_output_validation(
        self,
        top_stocks: pd.DataFrame,
        excluded_keywords: List[str] = None,
        max_age_days: int = 30
    ) -> CheckResult:
        """
        最终输出验证 - GE版
        """
        # 在函数外部处理默认值，避免闭包变量问题
        keywords = excluded_keywords or ['退市', 'ST', '*ST']
        
        def _check():
            nonlocal keywords
            
            if len(top_stocks) == 0:
                return CheckResult(
                    checkpoint="final_output_validation",
                    status=CheckStatus.FAILED,
                    message="选股结果为空",
                    details={'output_count': 0}
                )

            # 转换为 pandas 以便使用 str 方法
            df = top_stocks
            if hasattr(df, 'to_pandas'):
                df = df.to_pandas()
            elif hasattr(df, 'toPandas'):
                df = df.toPandas()

            # 额外检查：排除问题股票 (先做这个检查，因为它不依赖GE)
            if 'name' in df.columns:
                for keyword in keywords:
                    escaped = re.escape(keyword)
                    if df['name'].astype(str).str.contains(escaped, na=False, regex=True).any():
                        affected = df[df['name'].astype(str).str.contains(escaped, na=False, regex=True)]
                        return CheckResult(
                            checkpoint="final_output_validation",
                            status=CheckStatus.FAILED,
                            message=f"输出包含{keyword}股票",
                            details={'keyword': keyword, 'affected_codes': affected['code'].tolist()}
                        )

            # GE验证
            expectations = [
                {'type': 'expect_table_row_count_to_be_between', 'min_value': 1, 'max_value': 100},
                {'type': 'expect_column_to_exist', 'column': 'code'},
                {'type': 'expect_column_to_exist', 'column': 'name'},
            ]

            # 使用唯一suite名称避免重复
            suite_name = f"final_output_suite_{uuid.uuid4().hex[:8]}"
            status, message, ge_details = self._create_suite_and_validate(
                df, expectations, suite_name
            )

            return CheckResult(
                checkpoint="final_output_validation",
                status=status,
                message=message,
                details={'output_count': len(top_stocks), **ge_details},
                ge_results=[ge_details]
            )

        return self._run_with_retry(_check)

    # ========== 工具方法 ==========

    def _check_data_freshness(self, data: pl.DataFrame, data_type: str) -> float:
        """检查数据新鲜度"""
        try:
            if data_type == "kline" and "trade_date" in data.columns:
                latest_date = data["trade_date"].max()
                if isinstance(latest_date, str):
                    latest_date = datetime.strptime(latest_date, "%Y-%m-%d")
                days_diff = (datetime.now() - latest_date).days
                return max(0, 100 - days_diff * 5)
            return 100
        except:
            return 50

    def get_all_results(self) -> List[Dict[str, Any]]:
        """获取所有检查结果"""
        return [r.to_dict() for r in self.results]

    def get_summary(self) -> Dict[str, Any]:
        """获取检查摘要"""
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        warnings = sum(1 for r in self.results if r.status == CheckStatus.WARNING)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        retries = sum(r.retry_count for r in self.results)

        return {
            'total_checks': len(self.results),
            'passed': passed,
            'warnings': warnings,
            'failed': failed,
            'total_retries': retries,
            'success_rate': passed / len(self.results) * 100 if self.results else 0
        }

    def clear_results(self):
        """清空结果"""
        self.results = []


# 便捷函数接口
def run_pre_collection_check(date: str, max_retries: int = 3, **kwargs) -> CheckResult:
    """运行采集前检查（带重试）"""
    config = GERetryConfig(max_retries=max_retries)
    validator = GECheckpointValidators(config)
    return validator.pre_collection_check(date, **kwargs)


def run_post_collection_validation(data: pl.DataFrame, max_retries: int = 3, **kwargs) -> CheckResult:
    """运行采集后验证（带重试）"""
    config = GERetryConfig(max_retries=max_retries)
    validator = GECheckpointValidators(config)
    return validator.post_collection_validation(data, **kwargs)


def run_pre_scoring_check(df: pl.DataFrame, code: str, max_retries: int = 0, **kwargs) -> CheckResult:
    """运行计算前检查（无重试，快速版）"""
    # 评分检查不启用重试，因为是快速检查
    config = GERetryConfig(max_retries=0)
    validator = GECheckpointValidators(config)
    return validator.pre_scoring_check(df, code, **kwargs)


def run_post_scoring_validation(scores_df: pl.DataFrame, max_retries: int = 2, **kwargs) -> CheckResult:
    """运行计算后验证（带重试）"""
    config = GERetryConfig(max_retries=max_retries)
    validator = GECheckpointValidators(config)
    return validator.post_scoring_validation(scores_df, **kwargs)


def run_pre_selection_check(stock_pool: pd.DataFrame, date: str, max_retries: int = 3, **kwargs) -> CheckResult:
    """运行选股前检查（带重试）"""
    config = GERetryConfig(max_retries=max_retries)
    validator = GECheckpointValidators(config)
    return validator.pre_selection_check(stock_pool, date, **kwargs)


def run_final_output_validation(top_stocks: pd.DataFrame, max_retries: int = 2, **kwargs) -> CheckResult:
    """运行最终输出验证（带重试）"""
    config = GERetryConfig(max_retries=max_retries)
    validator = GECheckpointValidators(config)
    return validator.final_output_validation(top_stocks, **kwargs)
