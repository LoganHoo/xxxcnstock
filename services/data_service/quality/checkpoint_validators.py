#!/usr/bin/env python3
"""
业务流数据质量检查点验证器

实现6个关键检查点:
1. 采集前检查 (Pre-Collection Check)
2. 采集后验证 (Post-Collection Validation) - 集成 GE
3. 计算前检查 (Pre-Scoring Check)
4. 计算后验证 (Post-Scoring Validation)
5. 选股前检查 (Pre-Selection Check)
6. 最终输出验证 (Final Output Validation)
"""
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
import pandas as pd
import great_expectations as ge
import great_expectations.expectations as gxe
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
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'checkpoint': self.checkpoint,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp
        }


class DataQualityCheckpoints:
    """数据质量检查点集合"""
    
    def __init__(self):
        self.logger = setup_logger("data_quality_checkpoints")
        self.results: List[CheckResult] = []
    
    # ========== 检查点1: 采集前检查 ==========
    
    def pre_collection_check(
        self,
        date: str,
        data_service_health_check: callable = None,
        min_storage_gb: float = 1.0
    ) -> CheckResult:
        """
        采集前检查
        
        Args:
            date: 采集日期
            data_service_health_check: 数据源健康检查函数
            min_storage_gb: 最小存储空间(GB)
        
        Returns:
            检查结果
        """
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
        
        result = CheckResult(
            checkpoint="pre_collection_check",
            status=CheckStatus.PASSED,
            message="采集前检查通过",
            details={'date': date}
        )
        self.results.append(result)
        return result
    
    # ========== 检查点2: 采集后验证 (集成 GE) ==========
    
    def post_collection_validation(
        self,
        data: pl.DataFrame,
        data_type: str = "kline",
        min_quality_score: float = 80.0
    ) -> CheckResult:
        """
        采集后验证 - 集成 Great Expectations
        
        Args:
            data: 采集的数据
            data_type: 数据类型 (kline, stock_list, financial)
            min_quality_score: 最低质量分数
        
        Returns:
            检查结果
        """
        self.logger.info(f"执行采集后验证: {data_type}, 行数={len(data)}")
        
        try:
            # 1. 基础质量检查
            if len(data) == 0:
                return CheckResult(
                    checkpoint="post_collection_validation",
                    status=CheckStatus.FAILED,
                    message="数据为空",
                    details={'data_type': data_type}
                )
            
            # 2. Great Expectations 验证
            context = ge.get_context(mode="ephemeral")
            
            # 根据数据类型创建对应的期望套件
            if data_type == "stock_list":
                suite = self._create_stock_list_suite(context)
            elif data_type == "kline":
                suite = self._create_kline_suite(context)
            else:
                suite = self._create_generic_suite(context, data_type)
            
            # 转换为 pandas 进行验证
            df_pd = data.to_pandas()
            
            # 创建 datasource 和 batch
            datasource = context.data_sources.add_pandas(f"{data_type}_datasource")
            data_asset = datasource.add_dataframe_asset(name=f"{data_type}_asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{data_type}_batch")
            batch = batch_definition.get_batch(batch_parameters={"dataframe": df_pd})
            
            # 运行验证
            validator = context.get_validator(
                batch_request=batch.batch_request,
                expectation_suite=suite
            )
            ge_result = validator.validate()
            
            # 计算质量分数
            success_rate = ge_result.statistics.get('success_percent', 0)
            
            # 3. 数据新鲜度检查
            freshness_score = self._check_data_freshness(data, data_type)
            
            # 4. 综合质量分数
            quality_score = (success_rate * 0.7) + (freshness_score * 0.3)
            
            details = {
                'data_type': data_type,
                'rows': len(data),
                'columns': len(data.columns),
                'ge_success_rate': success_rate,
                'freshness_score': freshness_score,
                'quality_score': quality_score,
                'evaluated_expectations': ge_result.statistics.get('evaluated_expectations', 0),
                'successful_expectations': ge_result.statistics.get('successful_expectations', 0)
            }
            
            if quality_score >= min_quality_score:
                status = CheckStatus.PASSED
                message = f"数据质量验证通过: {quality_score:.1f}分"
            elif quality_score >= min_quality_score * 0.8:
                status = CheckStatus.WARNING
                message = f"数据质量警告: {quality_score:.1f}分"
            else:
                status = CheckStatus.FAILED
                message = f"数据质量不达标: {quality_score:.1f}分 < {min_quality_score}分"
            
            result = CheckResult(
                checkpoint="post_collection_validation",
                status=status,
                message=message,
                details=details
            )
            
        except Exception as e:
            self.logger.error(f"采集后验证失败: {e}")
            result = CheckResult(
                checkpoint="post_collection_validation",
                status=CheckStatus.FAILED,
                message=f"验证过程异常: {str(e)}",
                details={'error': str(e)}
            )
        
        self.results.append(result)
        return result
    
    def _create_stock_list_suite(self, context) -> ge.ExpectationSuite:
        """创建股票列表期望套件"""
        suite = context.suites.add(ge.ExpectationSuite(name="stock_list_checkpoint_suite"))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1000, max_value=10000))
        suite.add_expectation(gxe.ExpectColumnToExist(column="code"))
        suite.add_expectation(gxe.ExpectColumnToExist(column="name"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="code"))
        suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="code", regex=r"^\d{6}$"))
        return suite
    
    def _create_kline_suite(self, context) -> ge.ExpectationSuite:
        """创建K线数据期望套件"""
        suite = context.suites.add(ge.ExpectationSuite(name="kline_checkpoint_suite"))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000))
        suite.add_expectation(gxe.ExpectColumnToExist(column="code"))
        suite.add_expectation(gxe.ExpectColumnToExist(column="trade_date"))
        suite.add_expectation(gxe.ExpectColumnToExist(column="close"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="trade_date"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="close"))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0.01, max_value=10000))
        return suite
    
    def _create_generic_suite(self, context, data_type: str) -> ge.ExpectationSuite:
        """创建通用期望套件"""
        suite = context.suites.add(ge.ExpectationSuite(name=f"{data_type}_checkpoint_suite"))
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=100000))
        return suite
    
    def _check_data_freshness(self, data: pl.DataFrame, data_type: str) -> float:
        """检查数据新鲜度，返回0-100分数"""
        try:
            if data_type == "kline" and "trade_date" in data.columns:
                latest_date = data["trade_date"].max()
                if isinstance(latest_date, str):
                    latest_date = datetime.strptime(latest_date, "%Y-%m-%d")
                days_diff = (datetime.now() - latest_date).days
                return max(0, 100 - days_diff * 5)  # 每天扣5分
            return 100
        except:
            return 50
    
    # ========== 检查点3: 计算前检查 ==========
    
    def pre_scoring_check(
        self,
        df: pl.DataFrame,
        code: str,
        min_rows: int = 20,
        max_missing_ratio: float = 0.1
    ) -> CheckResult:
        """
        计算前检查
        
        Args:
            df: K线数据
            code: 股票代码
            min_rows: 最小行数要求
            max_missing_ratio: 最大缺失值比例
        
        Returns:
            检查结果
        """
        # 1. 数据覆盖度检查
        if len(df) < min_rows:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 数据不足{min_rows}天",
                details={'code': code, 'rows': len(df), 'required': min_rows}
            )
        
        # 2. 必要列检查
        required_cols = ['close', 'volume', 'high', 'low']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 缺少必要列 {missing_cols}",
                details={'code': code, 'missing_cols': missing_cols}
            )
        
        # 3. 缺失值检查
        for col in required_cols:
            if col in df.columns:
                missing_ratio = df[col].null_count() / len(df)
                if missing_ratio > max_missing_ratio:
                    return CheckResult(
                        checkpoint="pre_scoring_check",
                        status=CheckStatus.FAILED,
                        message=f"{code}: {col} 缺失值过多 ({missing_ratio:.1%})",
                        details={'code': code, 'column': col, 'missing_ratio': missing_ratio}
                    )
        
        # 4. 数据合理性检查
        if df['close'].min() <= 0:
            return CheckResult(
                checkpoint="pre_scoring_check",
                status=CheckStatus.FAILED,
                message=f"{code}: 收盘价异常",
                details={'code': code, 'min_close': df['close'].min()}
            )
        
        result = CheckResult(
            checkpoint="pre_scoring_check",
            status=CheckStatus.PASSED,
            message=f"{code}: 计算前检查通过",
            details={'code': code, 'rows': len(df)}
        )
        self.results.append(result)
        return result
    
    # ========== 检查点4: 计算后验证 ==========
    
    def post_scoring_validation(
        self,
        scores_df: pl.DataFrame,
        min_stocks: int = 10,
        score_range: Tuple[float, float] = (0, 100),
        min_std: float = 1.0
    ) -> CheckResult:
        """
        计算后验证
        
        Args:
            scores_df: 评分结果
            min_stocks: 最小股票数量
            score_range: 评分有效范围
            min_std: 最小标准差(防止评分过于集中)
        
        Returns:
            检查结果
        """
        # 1. 结果完整性检查
        if len(scores_df) == 0:
            return CheckResult(
                checkpoint="post_scoring_validation",
                status=CheckStatus.FAILED,
                message="评分结果为空",
                details={'total_stocks': 0}
            )
        
        if len(scores_df) < min_stocks:
            return CheckResult(
                checkpoint="post_scoring_validation",
                status=CheckStatus.WARNING,
                message=f"评分股票数量较少: {len(scores_df)} < {min_stocks}",
                details={'total_stocks': len(scores_df), 'min_required': min_stocks}
            )
        
        # 2. 评分范围检查
        score_col = 'total_score' if 'total_score' in scores_df.columns else 'score'
        if score_col in scores_df.columns:
            min_score = scores_df[score_col].min()
            max_score = scores_df[score_col].max()
            
            if min_score < score_range[0] or max_score > score_range[1]:
                return CheckResult(
                    checkpoint="post_scoring_validation",
                    status=CheckStatus.FAILED,
                    message=f"评分超出有效范围: [{min_score}, {max_score}]",
                    details={'min_score': min_score, 'max_score': max_score, 'valid_range': score_range}
                )
            
            # 3. 评分分布检查
            score_std = scores_df[score_col].std()
            if score_std < min_std:
                return CheckResult(
                    checkpoint="post_scoring_validation",
                    status=CheckStatus.WARNING,
                    message=f"评分分布异常集中 (std={score_std:.2f} < {min_std})",
                    details={'std': score_std, 'min_std': min_std}
                )
        
        result = CheckResult(
            checkpoint="post_scoring_validation",
            status=CheckStatus.PASSED,
            message=f"评分验证通过: {len(scores_df)}只股票",
            details={'total_stocks': len(scores_df)}
        )
        self.results.append(result)
        return result
    
    # ========== 检查点5: 选股前检查 ==========
    
    def pre_selection_check(
        self,
        stock_pool: pd.DataFrame,
        date: str,
        min_pool_size: int = 100,
        required_cols: List[str] = None
    ) -> CheckResult:
        """
        选股前检查
        
        Args:
            stock_pool: 股票池
            date: 选股日期
            min_pool_size: 最小股票池大小
            required_cols: 必要列列表
        
        Returns:
            检查结果
        """
        required_cols = required_cols or ['code', 'name', 'close', 'volume']
        
        # 1. 股票池大小检查
        if len(stock_pool) < min_pool_size:
            return CheckResult(
                checkpoint="pre_selection_check",
                status=CheckStatus.FAILED,
                message=f"股票池太小: {len(stock_pool)} < {min_pool_size}",
                details={'pool_size': len(stock_pool), 'min_required': min_pool_size}
            )
        
        # 2. 必要列检查
        missing_cols = [col for col in required_cols if col not in stock_pool.columns]
        if missing_cols:
            return CheckResult(
                checkpoint="pre_selection_check",
                status=CheckStatus.FAILED,
                message=f"缺少必要列: {missing_cols}",
                details={'missing_cols': missing_cols}
            )
        
        # 3. 数据时效性检查
        if 'trade_date' in stock_pool.columns:
            latest_date = stock_pool['trade_date'].max()
            if str(latest_date) != str(date):
                return CheckResult(
                    checkpoint="pre_selection_check",
                    status=CheckStatus.WARNING,
                    message=f"数据日期不匹配: {latest_date} != {date}",
                    details={'data_date': latest_date, 'target_date': date}
                )
        
        result = CheckResult(
            checkpoint="pre_selection_check",
            status=CheckStatus.PASSED,
            message=f"选股前检查通过: {len(stock_pool)}只股票",
            details={'pool_size': len(stock_pool), 'date': date}
        )
        self.results.append(result)
        return result
    
    # ========== 检查点6: 最终输出验证 ==========
    
    def final_output_validation(
        self,
        top_stocks: pd.DataFrame,
        excluded_keywords: List[str] = None,
        max_age_days: int = 30
    ) -> CheckResult:
        """
        最终输出验证
        
        Args:
            top_stocks: 选股结果
            excluded_keywords: 排除的关键词列表 (如 '退市', 'ST')
            max_age_days: 最大数据年龄(天)
        
        Returns:
            检查结果
        """
        excluded_keywords = excluded_keywords or ['退市', 'ST', '*ST']
        
        # 1. 结果完整性检查
        if len(top_stocks) == 0:
            return CheckResult(
                checkpoint="final_output_validation",
                status=CheckStatus.FAILED,
                message="选股结果为空",
                details={'output_count': 0}
            )
        
        # 2. 排除问题股票
        if 'name' in top_stocks.columns:
            for keyword in excluded_keywords:
                # 转义正则表达式特殊字符
                import re
                escaped_keyword = re.escape(keyword)
                if top_stocks['name'].astype(str).str.contains(escaped_keyword, na=False, regex=True).any():
                    affected = top_stocks[top_stocks['name'].astype(str).str.contains(escaped_keyword, na=False, regex=True)]
                    return CheckResult(
                        checkpoint="final_output_validation",
                        status=CheckStatus.FAILED,
                        message=f"输出包含{keyword}股票: {affected['code'].tolist()}",
                        details={'keyword': keyword, 'affected_codes': affected['code'].tolist()}
                    )
        
        # 3. 数据新鲜度检查
        if 'latest_date' in top_stocks.columns:
            latest_dates = pd.to_datetime(top_stocks['latest_date'])
            days_old = (datetime.now() - latest_dates).dt.days.max()
            if days_old > max_age_days:
                return CheckResult(
                    checkpoint="final_output_validation",
                    status=CheckStatus.FAILED,
                    message=f"推荐股票数据过旧: {days_old}天 > {max_age_days}天",
                    details={'max_age_days': days_old, 'limit_days': max_age_days}
                )
        
        # 4. 重复检查
        if 'code' in top_stocks.columns:
            duplicates = top_stocks['code'].duplicated().sum()
            if duplicates > 0:
                return CheckResult(
                    checkpoint="final_output_validation",
                    status=CheckStatus.WARNING,
                    message=f"输出包含{duplicates}只重复股票",
                    details={'duplicate_count': duplicates}
                )
        
        result = CheckResult(
            checkpoint="final_output_validation",
            status=CheckStatus.PASSED,
            message=f"最终输出验证通过: {len(top_stocks)}只股票",
            details={'output_count': len(top_stocks)}
        )
        self.results.append(result)
        return result
    
    # ========== 工具方法 ==========
    
    def get_all_results(self) -> List[Dict[str, Any]]:
        """获取所有检查结果"""
        return [r.to_dict() for r in self.results]
    
    def get_summary(self) -> Dict[str, Any]:
        """获取检查摘要"""
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        warnings = sum(1 for r in self.results if r.status == CheckStatus.WARNING)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        
        return {
            'total_checks': len(self.results),
            'passed': passed,
            'warnings': warnings,
            'failed': failed,
            'success_rate': passed / len(self.results) * 100 if self.results else 0
        }
    
    def clear_results(self):
        """清空结果"""
        self.results = []


# 便捷函数接口
def run_pre_collection_check(date: str, **kwargs) -> CheckResult:
    """运行采集前检查"""
    validator = DataQualityCheckpoints()
    return validator.pre_collection_check(date, **kwargs)


def run_post_collection_validation(data: pl.DataFrame, **kwargs) -> CheckResult:
    """运行采集后验证"""
    validator = DataQualityCheckpoints()
    return validator.post_collection_validation(data, **kwargs)


def run_pre_scoring_check(df: pl.DataFrame, code: str, **kwargs) -> CheckResult:
    """运行计算前检查"""
    validator = DataQualityCheckpoints()
    return validator.pre_scoring_check(df, code, **kwargs)


def run_post_scoring_validation(scores_df: pl.DataFrame, **kwargs) -> CheckResult:
    """运行计算后验证"""
    validator = DataQualityCheckpoints()
    return validator.post_scoring_validation(scores_df, **kwargs)


def run_pre_selection_check(stock_pool: pd.DataFrame, date: str, **kwargs) -> CheckResult:
    """运行选股前检查"""
    validator = DataQualityCheckpoints()
    return validator.pre_selection_check(stock_pool, date, **kwargs)


def run_final_output_validation(top_stocks: pd.DataFrame, **kwargs) -> CheckResult:
    """运行最终输出验证"""
    validator = DataQualityCheckpoints()
    return validator.final_output_validation(top_stocks, **kwargs)
