#!/usr/bin/env python3
"""
统一报告基类模块

提供标准化的报告生成和发送流程，所有报告脚本应继承此类
"""
import os
import sys
import json
import hashlib
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from core.paths import ReportPaths
from core.data_availability import check_before_report
from core.report_validator import check_report_quality, get_quality_checker


class ReportStatus(Enum):
    """报告状态"""
    PENDING = "pending"           # 待执行
    RUNNING = "running"           # 执行中
    SUCCESS = "success"           # 成功
    FAILED = "failed"             # 失败
    VALIDATION_FAILED = "validation_failed"  # 验证失败
    DATA_MISSING = "data_missing" # 数据缺失
    SEND_FAILED = "send_failed"   # 发送失败


@dataclass
class ReportResult:
    """报告执行结果"""
    report_type: str
    status: ReportStatus
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    content_hash: Optional[str] = None
    error_message: Optional[str] = None
    data_sources: Dict[str, Any] = field(default_factory=dict)
    validation_issues: List[str] = field(default_factory=list)
    execution_time_ms: int = 0


class BaseReporter(ABC):
    """
    报告基类
    
    所有报告脚本应继承此类，实现抽象方法
    
    使用示例:
        class MorningReporter(BaseReporter):
            @property
            def report_type(self) -> str:
                return "morning_report"
            
            def load_data(self) -> Dict:
                return {
                    'foreign': self._load_json(ReportPaths.foreign_index()),
                    'market': self._load_json(ReportPaths.market_analysis()),
                }
            
            def generate(self, data: Dict) -> str:
                template = get_template('morning_report')
                return template.generate(**data)
    """

    def __init__(self):
        self.logger = self._setup_logger()
        self.quality_checker = get_quality_checker()
        self._result = None
        self._start_time = None

    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    @property
    @abstractmethod
    def report_type(self) -> str:
        """报告类型标识"""
        pass

    @property
    def required_data_sources(self) -> List[str]:
        """必需的数据源列表"""
        return []

    @property
    def optional_data_sources(self) -> List[str]:
        """可选的数据源列表"""
        return []

    @property
    def max_age_hours(self) -> int:
        """数据最大有效期（小时）"""
        return 24

    @abstractmethod
    def load_data(self) -> Dict[str, Any]:
        """
        加载报告所需数据
        
        Returns:
            Dict: 数据字典，key为数据源名称
        """
        pass

    @abstractmethod
    def generate(self, data: Dict[str, Any]) -> str:
        """
        生成报告内容
        
        Args:
            data: load_data() 返回的数据
            
        Returns:
            str: 报告内容
        """
        pass

    def _load_json_file(self, file_path: Optional[Path], default: Any = None) -> Any:
        """统一加载JSON文件"""
        if file_path is None or not file_path.exists():
            return default
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"加载文件失败 {file_path}: {e}")
            return default

    def _check_prerequisites(self) -> Tuple[bool, List[str]]:
        """检查前置条件"""
        return check_before_report(
            self.report_type,
            max_age_hours=self.max_age_hours
        )

    def _validate_data(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证数据质量"""
        quality_check = check_report_quality(
            self.report_type,
            **data
        )

        # 生成质量报告
        quality_report = self.quality_checker.generate_quality_report(quality_check)
        self.logger.info(f"数据质量检查:\n{quality_report}")

        issues = []
        if quality_check['critical_issues']:
            issues.extend(quality_check['critical_issues'])

        # 检查必需数据源
        for source in self.required_data_sources:
            if source not in data or data[source] is None:
                issues.append(f"缺少必需数据源: {source}")

        is_valid = len(issues) == 0
        return is_valid, issues

    def _validate_content(self, content: str) -> Tuple[bool, List[str]]:
        """验证报告内容"""
        issues = []

        if not content or len(content.strip()) == 0:
            issues.append("报告内容为空")
            return False, issues

        # 检查异常标记
        abnormal_markers = ['数据暂不可用', 'N/A', 'None', 'null', 'NULL']
        for marker in abnormal_markers:
            if marker in content:
                issues.append(f"内容包含异常标记: {marker}")

        # 检查内容长度
        if len(content) < 100:
            issues.append(f"报告内容过短: {len(content)} 字符")

        is_valid = len(issues) == 0 or all("异常标记" in i for i in issues)
        return is_valid, issues

    def _compute_content_hash(self, content: str) -> str:
        """计算内容哈希"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()[:16]

    def _send(self, content: str) -> bool:
        """发送报告"""
        from services.email_sender import EmailService

        try:
            email_service = EmailService()

            recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
            recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

            if not recipients:
                self.logger.error("未配置收件人")
                return False

            today = datetime.now().strftime('%Y-%m-%d')
            subject = f"【{self.report_type}】{today}"

            success = email_service.send(
                to=recipients,
                subject=subject,
                content=content
            )

            if success:
                self.logger.info(f"报告发送成功: {recipients}")
            else:
                self.logger.error("邮件发送失败")

            return success

        except Exception as e:
            self.logger.error(f"发送失败: {e}")
            return False

    def _record_result(self, status: ReportStatus, content_hash: str = None,
                       error: str = None, data_sources: Dict = None,
                       validation_issues: List[str] = None):
        """记录执行结果"""
        execution_time = 0
        if self._start_time:
            execution_time = int((datetime.now() - self._start_time).total_seconds() * 1000)

        self._result = ReportResult(
            report_type=self.report_type,
            status=status,
            content_hash=content_hash,
            error_message=error,
            data_sources=data_sources or {},
            validation_issues=validation_issues or [],
            execution_time_ms=execution_time
        )

        # 保存到数据库
        try:
            from services.report_tracking_service import get_tracking_service
            service = get_tracking_service()
            service.record_result(self._result)
        except Exception as e:
            self.logger.warning(f"记录结果失败: {e}")

    def run(self) -> bool:
        """
        执行完整报告流程
        
        Returns:
            bool: 是否成功
        """
        self._start_time = datetime.now()
        self.logger.info(f"开始执行报告: {self.report_type}")

        # 1. 检查前置条件
        can_generate, missing = self._check_prerequisites()
        if not can_generate:
            error_msg = f"前置条件检查失败: {missing}"
            self.logger.error(error_msg)
            self._record_result(
                ReportStatus.DATA_MISSING,
                error=error_msg,
                validation_issues=missing
            )
            return False

        # 2. 加载数据
        self.logger.info("加载数据...")
        try:
            data = self.load_data()
        except Exception as e:
            error_msg = f"数据加载失败: {e}"
            self.logger.error(error_msg)
            self._record_result(ReportStatus.DATA_MISSING, error=error_msg)
            return False

        # 3. 验证数据
        self.logger.info("验证数据...")
        is_valid, issues = self._validate_data(data)
        if not is_valid:
            error_msg = f"数据验证失败: {issues}"
            self.logger.error(error_msg)
            self._record_result(
                ReportStatus.VALIDATION_FAILED,
                error=error_msg,
                data_sources={k: v is not None for k, v in data.items()},
                validation_issues=issues
            )
            return False

        # 4. 生成报告
        self.logger.info("生成报告...")
        try:
            content = self.generate(data)
        except Exception as e:
            error_msg = f"报告生成失败: {e}"
            self.logger.error(error_msg)
            import traceback
            self.logger.error(traceback.format_exc())
            self._record_result(
                ReportStatus.FAILED,
                error=error_msg,
                data_sources={k: v is not None for k, v in data.items()}
            )
            return False

        # 5. 验证内容
        self.logger.info("验证报告内容...")
        is_valid, issues = self._validate_content(content)
        if not is_valid:
            error_msg = f"内容验证失败: {issues}"
            self.logger.error(error_msg)
            self._record_result(
                ReportStatus.VALIDATION_FAILED,
                error=error_msg,
                data_sources={k: v is not None for k, v in data.items()},
                validation_issues=issues
            )
            return False

        # 6. 计算内容哈希
        content_hash = self._compute_content_hash(content)

        # 7. 发送报告
        self.logger.info("发送报告...")
        success = self._send(content)

        if success:
            self._record_result(
                ReportStatus.SUCCESS,
                content_hash=content_hash,
                data_sources={k: v is not None for k, v in data.items()}
            )
            self.logger.info(f"报告执行完成: {self.report_type}")
        else:
            self._record_result(
                ReportStatus.SEND_FAILED,
                content_hash=content_hash,
                error="发送失败",
                data_sources={k: v is not None for k, v in data.items()}
            )

        return success

    def get_last_result(self) -> Optional[ReportResult]:
        """获取上次执行结果"""
        return self._result
