"""
报告生成监控服务

用于追踪报告生成状态、记录日志、发送告警
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class ReportStatus(Enum):
    """报告状态枚举"""
    PENDING = "pending"           # 等待执行
    RUNNING = "running"           # 执行中
    SUCCESS = "success"           # 成功
    PARTIAL = "partial"           # 部分成功（缺少可选数据）
    FAILED = "failed"             # 失败
    TIMEOUT = "timeout"           # 超时
    CANCELLED = "cancelled"       # 取消


@dataclass
class ReportExecutionRecord:
    """报告执行记录"""
    report_type: str
    status: ReportStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    missing_required: List[str] = field(default_factory=list)
    missing_optional: List[str] = field(default_factory=list)
    error_message: str = ""
    stack_trace: str = ""
    output_files: List[str] = field(default_factory=list)
    email_sent: bool = False
    db_saved: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'report_type': self.report_type,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'missing_required': self.missing_required,
            'missing_optional': self.missing_optional,
            'error_message': self.error_message,
            'stack_trace': self.stack_trace,
            'output_files': self.output_files,
            'email_sent': self.email_sent,
            'db_saved': self.db_saved,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ReportExecutionRecord':
        """从字典创建"""
        return cls(
            report_type=data.get('report_type', ''),
            status=ReportStatus(data.get('status', 'pending')),
            start_time=datetime.fromisoformat(data['start_time']) if data.get('start_time') else None,
            end_time=datetime.fromisoformat(data['end_time']) if data.get('end_time') else None,
            duration_seconds=data.get('duration_seconds', 0.0),
            missing_required=data.get('missing_required', []),
            missing_optional=data.get('missing_optional', []),
            error_message=data.get('error_message', ''),
            stack_trace=data.get('stack_trace', ''),
            output_files=data.get('output_files', []),
            email_sent=data.get('email_sent', False),
            db_saved=data.get('db_saved', False),
            metadata=data.get('metadata', {})
        )


class ReportMonitoringService:
    """报告监控服务"""

    def __init__(self, status_dir: Optional[Path] = None):
        """
        初始化监控服务

        Args:
            status_dir: 状态文件保存目录
        """
        if status_dir is None:
            status_dir = Path(__file__).parent.parent / "data" / "monitoring"
        self.status_dir = status_dir
        self.status_dir.mkdir(parents=True, exist_ok=True)

        self._current_executions: Dict[str, ReportExecutionRecord] = {}
        self._history: List[ReportExecutionRecord] = []

    def _get_status_file(self, report_type: str, date: Optional[str] = None) -> Path:
        """获取状态文件路径"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        return self.status_dir / f"{report_type}_{date}.json"

    def start_execution(self, report_type: str, metadata: Optional[Dict] = None) -> ReportExecutionRecord:
        """
        开始执行报告

        Args:
            report_type: 报告类型
            metadata: 元数据

        Returns:
            ReportExecutionRecord
        """
        record = ReportExecutionRecord(
            report_type=report_type,
            status=ReportStatus.RUNNING,
            start_time=datetime.now(),
            metadata=metadata or {}
        )
        self._current_executions[report_type] = record

        logger.info(f"[{report_type}] 开始执行报告生成")
        return record

    def end_execution(self, report_type: str,
                     status: ReportStatus,
                     missing_required: Optional[List[str]] = None,
                     missing_optional: Optional[List[str]] = None,
                     error_message: str = "",
                     stack_trace: str = "",
                     output_files: Optional[List[str]] = None,
                     email_sent: bool = False,
                     db_saved: bool = False):
        """
        结束执行报告

        Args:
            report_type: 报告类型
            status: 执行状态
            missing_required: 缺失的必需数据
            missing_optional: 缺失的可选数据
            error_message: 错误消息
            stack_trace: 堆栈跟踪
            output_files: 输出文件列表
            email_sent: 是否发送邮件
            db_saved: 是否保存到数据库
        """
        record = self._current_executions.get(report_type)
        if record is None:
            logger.warning(f"[{report_type}] 未找到执行记录")
            return

        record.end_time = datetime.now()
        record.duration_seconds = (record.end_time - record.start_time).total_seconds()
        record.status = status
        record.missing_required = missing_required or []
        record.missing_optional = missing_optional or []
        record.error_message = error_message
        record.stack_trace = stack_trace
        record.output_files = output_files or []
        record.email_sent = email_sent
        record.db_saved = db_saved

        # 保存到历史
        self._history.append(record)

        # 保存到文件
        self._save_record(record)

        # 移除当前执行记录
        del self._current_executions[report_type]

        # 记录日志
        if status == ReportStatus.SUCCESS:
            logger.info(f"[{report_type}] 报告生成成功，耗时 {record.duration_seconds:.2f}秒")
        elif status == ReportStatus.PARTIAL:
            logger.warning(f"[{report_type}] 报告生成部分成功，耗时 {record.duration_seconds:.2f}秒")
            logger.warning(f"  缺失可选数据: {record.missing_optional}")
        elif status == ReportStatus.FAILED:
            logger.error(f"[{report_type}] 报告生成失败，耗时 {record.duration_seconds:.2f}秒")
            logger.error(f"  错误: {error_message}")
        elif status == ReportStatus.TIMEOUT:
            logger.error(f"[{report_type}] 报告生成超时")

    def _save_record(self, record: ReportExecutionRecord):
        """保存记录到文件"""
        try:
            status_file = self._get_status_file(record.report_type)
            with open(status_file, 'w', encoding='utf-8') as f:
                json.dump(record.to_dict(), f, ensure_ascii=False, indent=2)
            logger.debug(f"执行记录已保存: {status_file}")
        except Exception as e:
            logger.error(f"保存执行记录失败: {e}")

    def load_record(self, report_type: str, date: Optional[str] = None) -> Optional[ReportExecutionRecord]:
        """
        加载执行记录

        Args:
            report_type: 报告类型
            date: 日期，格式 YYYYMMDD

        Returns:
            ReportExecutionRecord or None
        """
        status_file = self._get_status_file(report_type, date)
        if not status_file.exists():
            return None

        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return ReportExecutionRecord.from_dict(data)
        except Exception as e:
            logger.error(f"加载执行记录失败: {e}")
            return None

    def get_execution_summary(self, report_type: Optional[str] = None,
                             days: int = 7) -> Dict[str, Any]:
        """
        获取执行摘要

        Args:
            report_type: 报告类型，None表示所有
            days: 最近天数

        Returns:
            Dict
        """
        records = []

        # 从文件加载历史记录
        for status_file in self.status_dir.glob("*.json"):
            try:
                with open(status_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                record = ReportExecutionRecord.from_dict(data)

                # 过滤报告类型
                if report_type and record.report_type != report_type:
                    continue

                # 过滤日期
                if record.start_time:
                    days_diff = (datetime.now() - record.start_time).days
                    if days_diff > days:
                        continue

                records.append(record)
            except Exception as e:
                logger.warning(f"加载记录失败 {status_file}: {e}")

        # 统计
        total = len(records)
        success = sum(1 for r in records if r.status == ReportStatus.SUCCESS)
        partial = sum(1 for r in records if r.status == ReportStatus.PARTIAL)
        failed = sum(1 for r in records if r.status == ReportStatus.FAILED)
        timeout = sum(1 for r in records if r.status == ReportStatus.TIMEOUT)

        avg_duration = sum(r.duration_seconds for r in records) / total if total > 0 else 0

        return {
            'total': total,
            'success': success,
            'partial': partial,
            'failed': failed,
            'timeout': timeout,
            'success_rate': (success / total * 100) if total > 0 else 0,
            'avg_duration_seconds': avg_duration,
            'records': [r.to_dict() for r in records]
        }

    def check_health(self) -> Dict[str, Any]:
        """
        健康检查

        Returns:
            Dict
        """
        issues = []

        # 检查最近执行
        summary = self.get_execution_summary(days=1)

        if summary['failed'] > 0:
            issues.append(f"最近24小时有 {summary['failed']} 个报告生成失败")

        if summary['timeout'] > 0:
            issues.append(f"最近24小时有 {summary['timeout']} 个报告生成超时")

        # 检查当前执行
        running = list(self._current_executions.keys())
        if running:
            issues.append(f"当前有 {len(running)} 个报告正在执行: {running}")

        return {
            'healthy': len(issues) == 0,
            'issues': issues,
            'summary': summary
        }

    def generate_daily_report(self) -> str:
        """
        生成每日监控报告

        Returns:
            str
        """
        summary = self.get_execution_summary(days=1)

        lines = [
            "=" * 70,
            "【报告生成监控日报】",
            f"日期: {datetime.now().strftime('%Y-%m-%d')}",
            "=" * 70,
            "",
            "📊 今日统计:",
            f"  ● 总执行次数: {summary['total']}",
            f"  ● 成功: {summary['success']}",
            f"  ● 部分成功: {summary['partial']}",
            f"  ● 失败: {summary['failed']}",
            f"  ● 超时: {summary['timeout']}",
            f"  ● 成功率: {summary['success_rate']:.1f}%",
            f"  ● 平均耗时: {summary['avg_duration_seconds']:.2f}秒",
            "",
        ]

        # 失败的报告详情
        failed_records = [r for r in summary['records'] if r['status'] == 'failed']
        if failed_records:
            lines.append("❌ 失败报告:")
            for record in failed_records:
                lines.append(f"  ● {record['report_type']}: {record['error_message'][:50]}")
            lines.append("")

        # 部分成功的报告
        partial_records = [r for r in summary['records'] if r['status'] == 'partial']
        if partial_records:
            lines.append("⚠️ 部分成功报告:")
            for record in partial_records:
                missing = record.get('missing_optional', [])
                lines.append(f"  ● {record['report_type']}: 缺失 {len(missing)} 个可选数据")
            lines.append("")

        lines.append("=" * 70)

        return "\n".join(lines)


# 全局监控服务实例
_monitoring_service: Optional[ReportMonitoringService] = None


def get_monitoring_service(status_dir: Optional[Path] = None) -> ReportMonitoringService:
    """获取监控服务实例（单例）"""
    global _monitoring_service
    if _monitoring_service is None:
        _monitoring_service = ReportMonitoringService(status_dir)
    return _monitoring_service


# 便捷函数
def start_report_execution(report_type: str, metadata: Optional[Dict] = None) -> ReportExecutionRecord:
    """开始报告执行"""
    return get_monitoring_service().start_execution(report_type, metadata)


def end_report_execution(report_type: str, status: ReportStatus, **kwargs):
    """结束报告执行"""
    get_monitoring_service().end_execution(report_type, status, **kwargs)


def get_report_health() -> Dict[str, Any]:
    """获取报告健康状态"""
    return get_monitoring_service().check_health()
