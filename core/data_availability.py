"""
数据可用性检查模块

用于在报告生成前检查所需数据是否齐全
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from core.paths import ReportPaths

logger = logging.getLogger(__name__)


class DataStatus(Enum):
    """数据状态枚举"""
    AVAILABLE = "available"      # 数据可用
    MISSING = "missing"          # 数据缺失
    STALE = "stale"              # 数据过期
    CORRUPTED = "corrupted"      # 数据损坏


@dataclass
class DataCheckResult:
    """数据检查结果"""
    file_path: Path
    required: bool               # 是否必需
    status: DataStatus
    exists: bool
    size: int = 0
    modified_time: Optional[datetime] = None
    error_message: str = ""


@dataclass
class ReportDataCheck:
    """报告数据检查结果"""
    report_type: str
    all_available: bool
    required_available: bool
    optional_available: bool = True
    results: List[DataCheckResult] = field(default_factory=list)
    missing_required: List[str] = field(default_factory=list)
    missing_optional: List[str] = field(default_factory=list)


class DataAvailabilityChecker:
    """数据可用性检查器"""

    # 各报告类型所需的数据文件配置
    REQUIRED_DATA = {
        'morning_report': {
            'required': [
                ('foreign_index', lambda: ReportPaths.foreign_index()),
                ('market_analysis', lambda: ReportPaths.market_analysis(fallback_to_yesterday=True)),
                ('fund_behavior_result', lambda: ReportPaths.fund_behavior_result()),
            ],
            'optional': [
                ('daily_picks', lambda: ReportPaths.daily_picks(fallback_to_yesterday=True)),
                ('strategy_result', lambda: ReportPaths.strategy_result()),
            ]
        },
        'morning_shao': {
            'required': [
                ('foreign_index', lambda: ReportPaths.foreign_index()),
                ('market_analysis', lambda: ReportPaths.market_analysis(fallback_to_yesterday=True)),
                ('fund_behavior_result', lambda: ReportPaths.fund_behavior_result()),
            ],
            'optional': [
                ('macro_data', lambda: ReportPaths.macro_data()),
                ('oil_dollar_data', lambda: ReportPaths.oil_dollar_data()),
                ('commodities_data', lambda: ReportPaths.commodities_data()),
                ('sentiment_data', lambda: ReportPaths.sentiment_data()),
                ('news_data', lambda: ReportPaths.news_data()),
                ('daily_picks', lambda: ReportPaths.daily_picks(fallback_to_yesterday=True)),
            ]
        },
        'review_report': {
            'required': [
                ('dq_close', lambda: ReportPaths.dq_close()),
                ('market_review', lambda: ReportPaths.market_review()),
            ],
            'optional': [
                ('picks_review', lambda: ReportPaths.picks_review()),
                ('okr_data', lambda: ReportPaths.okr_data()),
                ('ai_review', lambda: ReportPaths.ai_review()),
                ('enhanced_scores', lambda: ReportPaths.enhanced_scores()),
                ('cvd_latest', lambda: ReportPaths.cvd_latest()),
            ]
        },
        'fund_behavior': {
            'required': [
                ('fund_behavior_result', lambda: ReportPaths.fund_behavior_result()),
            ],
            'optional': []
        }
    }

    def __init__(self, max_age_hours: int = 24):
        """
        初始化检查器
        
        Args:
            max_age_hours: 数据最大允许年龄（小时）
        """
        self.max_age_hours = max_age_hours

    def _check_file(self, file_path: Optional[Path], required: bool = True) -> DataCheckResult:
        """
        检查单个文件状态
        
        Args:
            file_path: 文件路径
            required: 是否必需
            
        Returns:
            DataCheckResult
        """
        if file_path is None:
            return DataCheckResult(
                file_path=Path("unknown"),
                required=required,
                status=DataStatus.MISSING,
                exists=False,
                error_message="文件路径为空"
            )

        if not file_path.exists():
            return DataCheckResult(
                file_path=file_path,
                required=required,
                status=DataStatus.MISSING,
                exists=False,
                error_message=f"文件不存在: {file_path}"
            )

        try:
            stat = file_path.stat()
            size = stat.st_size
            modified_time = datetime.fromtimestamp(stat.st_mtime)
            
            # 检查文件大小
            if size == 0:
                return DataCheckResult(
                    file_path=file_path,
                    required=required,
                    status=DataStatus.CORRUPTED,
                    exists=True,
                    size=size,
                    modified_time=modified_time,
                    error_message="文件为空"
                )

            # 检查文件年龄
            age = datetime.now() - modified_time
            if age > timedelta(hours=self.max_age_hours):
                return DataCheckResult(
                    file_path=file_path,
                    required=required,
                    status=DataStatus.STALE,
                    exists=True,
                    size=size,
                    modified_time=modified_time,
                    error_message=f"文件过期，年龄: {age}"
                )

            # 尝试解析JSON文件
            if file_path.suffix == '.json':
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        if not content:
                            return DataCheckResult(
                                file_path=file_path,
                                required=required,
                                status=DataStatus.CORRUPTED,
                                exists=True,
                                size=size,
                                modified_time=modified_time,
                                error_message="JSON文件内容为空"
                            )
                except json.JSONDecodeError as e:
                    return DataCheckResult(
                        file_path=file_path,
                        required=required,
                        status=DataStatus.CORRUPTED,
                        exists=True,
                        size=size,
                        modified_time=modified_time,
                        error_message=f"JSON解析失败: {e}"
                    )

            return DataCheckResult(
                file_path=file_path,
                required=required,
                status=DataStatus.AVAILABLE,
                exists=True,
                size=size,
                modified_time=modified_time
            )

        except Exception as e:
            return DataCheckResult(
                file_path=file_path,
                required=required,
                status=DataStatus.CORRUPTED,
                exists=True,
                error_message=f"检查文件时出错: {e}"
            )

    def check_report_data(self, report_type: str) -> ReportDataCheck:
        """
        检查报告所需数据
        
        Args:
            report_type: 报告类型
            
        Returns:
            ReportDataCheck
        """
        if report_type not in self.REQUIRED_DATA:
            logger.error(f"未知的报告类型: {report_type}")
            return ReportDataCheck(
                report_type=report_type,
                all_available=False,
                required_available=False,
                missing_required=["未知的报告类型"]
            )

        config = self.REQUIRED_DATA[report_type]
        results = []
        missing_required = []
        missing_optional = []

        # 检查必需文件
        for name, path_func in config.get('required', []):
            try:
                file_path = path_func()
                result = self._check_file(file_path, required=True)
                results.append(result)
                
                if result.status != DataStatus.AVAILABLE:
                    missing_required.append(f"{name} ({result.error_message})")
                    logger.warning(f"[必需] {name}: {result.status.value} - {result.error_message}")
                else:
                    logger.info(f"[必需] {name}: ✓ 可用")
            except Exception as e:
                logger.error(f"检查 {name} 时出错: {e}")
                missing_required.append(f"{name} (检查出错: {e})")

        # 检查可选文件
        for name, path_func in config.get('optional', []):
            try:
                file_path = path_func()
                result = self._check_file(file_path, required=False)
                results.append(result)
                
                if result.status != DataStatus.AVAILABLE:
                    missing_optional.append(f"{name} ({result.error_message})")
                    logger.info(f"[可选] {name}: {result.status.value} - {result.error_message}")
                else:
                    logger.info(f"[可选] {name}: ✓ 可用")
            except Exception as e:
                logger.warning(f"检查 {name} 时出错: {e}")
                missing_optional.append(f"{name} (检查出错: {e})")

        required_available = len(missing_required) == 0
        optional_available = len(missing_optional) == 0
        all_available = required_available and optional_available

        return ReportDataCheck(
            report_type=report_type,
            all_available=all_available,
            required_available=required_available,
            optional_available=optional_available,
            results=results,
            missing_required=missing_required,
            missing_optional=missing_optional
        )

    def can_generate_report(self, report_type: str, allow_missing_optional: bool = True) -> bool:
        """
        判断是否可以生成报告
        
        Args:
            report_type: 报告类型
            allow_missing_optional: 是否允许缺少可选数据
            
        Returns:
            bool
        """
        check = self.check_report_data(report_type)
        
        if not check.required_available:
            logger.error(f"无法生成 {report_type} 报告: 缺少必需数据")
            for missing in check.missing_required:
                logger.error(f"  - {missing}")
            return False

        if not allow_missing_optional and check.missing_optional:
            logger.error(f"无法生成 {report_type} 报告: 缺少可选数据")
            for missing in check.missing_optional:
                logger.error(f"  - {missing}")
            return False

        if check.missing_optional:
            logger.warning(f"生成 {report_type} 报告: 部分可选数据缺失，报告可能不完整")
            for missing in check.missing_optional:
                logger.warning(f"  - {missing}")

        return True

    def get_missing_data_summary(self, report_type: str) -> Dict[str, List[str]]:
        """
        获取缺失数据摘要
        
        Args:
            report_type: 报告类型
            
        Returns:
            Dict with 'required' and 'optional' keys
        """
        check = self.check_report_data(report_type)
        return {
            'required': check.missing_required,
            'optional': check.missing_optional
        }


# 全局检查器实例
def get_checker(max_age_hours: int = 24) -> DataAvailabilityChecker:
    """获取数据可用性检查器实例"""
    return DataAvailabilityChecker(max_age_hours=max_age_hours)


def check_before_report(report_type: str, max_age_hours: int = 24) -> Tuple[bool, List[str]]:
    """
    报告生成前的快速检查
    
    Args:
        report_type: 报告类型
        max_age_hours: 数据最大允许年龄
        
    Returns:
        (是否可以生成, 缺失数据列表)
    """
    checker = get_checker(max_age_hours)
    can_generate = checker.can_generate_report(report_type, allow_missing_optional=True)
    summary = checker.get_missing_data_summary(report_type)
    
    missing = summary['required'] + summary['optional']
    return can_generate, missing
