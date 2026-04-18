#!/usr/bin/env python3
"""
报告发送状态追踪服务

用于记录和查询报告发送历史，防止重复发送，追踪发送状态
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from threading import Lock

from core.base_reporter import ReportResult, ReportStatus

logger = logging.getLogger(__name__)


@dataclass
class SendRecord:
    """发送记录"""
    report_type: str
    timestamp: str
    status: str
    content_hash: Optional[str] = None
    error_message: Optional[str] = None
    recipients: List[str] = None
    subject: Optional[str] = None


class ReportTrackingService:
    """报告追踪服务"""

    def __init__(self, storage_path: Optional[Path] = None):
        """
        初始化追踪服务

        Args:
            storage_path: 存储路径，默认 logs/report_tracking.json
        """
        if storage_path is None:
            storage_path = Path(__file__).parent.parent / "logs" / "report_tracking.json"

        self.storage_path = storage_path
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[str, List[Dict]] = {}
        self._lock = Lock()
        self._load()

    def _load(self):
        """从文件加载数据"""
        if self.storage_path.exists():
            try:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载追踪数据失败: {e}")
                self._cache = {}
        else:
            self._cache = {}

    def _save(self):
        """保存数据到文件"""
        try:
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger.error(f"保存追踪数据失败: {e}")

    def record_result(self, result: ReportResult):
        """
        记录报告执行结果

        Args:
            result: 报告结果对象
        """
        with self._lock:
            record = {
                'timestamp': result.timestamp,
                'status': result.status.value,
                'content_hash': result.content_hash,
                'error_message': result.error_message,
                'data_sources': result.data_sources,
                'validation_issues': result.validation_issues,
                'execution_time_ms': result.execution_time_ms
            }

            if result.report_type not in self._cache:
                self._cache[result.report_type] = []

            self._cache[result.report_type].append(record)

            # 只保留最近30天的记录
            self._cleanup_old_records(result.report_type, days=30)

            self._save()

            logger.info(f"记录报告结果: {result.report_type} - {result.status.value}")

    def record_send_attempt(self, report_type: str, status: str,
                           content_hash: str = None, error: str = None,
                           recipients: List[str] = None, subject: str = None):
        """
        记录发送尝试

        Args:
            report_type: 报告类型
            status: 状态
            content_hash: 内容哈希
            error: 错误信息
            recipients: 收件人列表
            subject: 邮件主题
        """
        with self._lock:
            record = {
                'timestamp': datetime.now().isoformat(),
                'status': status,
                'content_hash': content_hash,
                'error_message': error,
                'recipients': recipients,
                'subject': subject
            }

            key = f"{report_type}_send"
            if key not in self._cache:
                self._cache[key] = []

            self._cache[key].append(record)
            self._cleanup_old_records(key, days=30)
            self._save()

    def is_duplicate(self, report_type: str, content_hash: str,
                     window_hours: int = 24) -> bool:
        """
        检查是否重复发送

        Args:
            report_type: 报告类型
            content_hash: 内容哈希
            window_hours: 检查时间窗口（小时）

        Returns:
            bool: 是否重复
        """
        with self._lock:
            key = f"{report_type}_send"
            if key not in self._cache:
                return False

            cutoff_time = datetime.now() - timedelta(hours=window_hours)

            for record in self._cache[key]:
                record_time = datetime.fromisoformat(record['timestamp'])
                if record_time < cutoff_time:
                    continue

                if record.get('content_hash') == content_hash:
                    if record.get('status') == 'success':
                        return True

            return False

    def get_send_status(self, report_type: str, date: str = None) -> Optional[Dict]:
        """
        获取指定日期的发送状态

        Args:
            report_type: 报告类型
            date: 日期字符串 YYYY-MM-DD，默认今天

        Returns:
            Dict: 状态信息
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        with self._lock:
            if report_type not in self._cache:
                return None

            for record in reversed(self._cache[report_type]):
                if record['timestamp'].startswith(date):
                    return {
                        'timestamp': record['timestamp'],
                        'status': record['status'],
                        'content_hash': record.get('content_hash'),
                        'error_message': record.get('error_message'),
                        'validation_issues': record.get('validation_issues', [])
                    }

            return None

    def get_recent_records(self, report_type: str, hours: int = 24) -> List[Dict]:
        """
        获取最近的发送记录

        Args:
            report_type: 报告类型
            hours: 最近几小时

        Returns:
            List[Dict]: 记录列表
        """
        with self._lock:
            if report_type not in self._cache:
                return []

            cutoff_time = datetime.now() - timedelta(hours=hours)
            records = []

            for record in self._cache[report_type]:
                record_time = datetime.fromisoformat(record['timestamp'])
                if record_time >= cutoff_time:
                    records.append(record)

            return records

    def get_success_rate(self, report_type: str, days: int = 7) -> Dict:
        """
        获取成功率统计

        Args:
            report_type: 报告类型
            days: 统计天数

        Returns:
            Dict: 统计信息
        """
        with self._lock:
            if report_type not in self._cache:
                return {'total': 0, 'success': 0, 'rate': 0.0}

            cutoff_time = datetime.now() - timedelta(days=days)
            total = 0
            success = 0

            for record in self._cache[report_type]:
                record_time = datetime.fromisoformat(record['timestamp'])
                if record_time >= cutoff_time:
                    total += 1
                    if record['status'] == ReportStatus.SUCCESS.value:
                        success += 1

            rate = (success / total * 100) if total > 0 else 0.0

            return {
                'total': total,
                'success': success,
                'failed': total - success,
                'rate': round(rate, 2)
            }

    def _cleanup_old_records(self, key: str, days: int = 30):
        """清理旧记录"""
        if key not in self._cache:
            return

        cutoff_time = datetime.now() - timedelta(days=days)
        self._cache[key] = [
            r for r in self._cache[key]
            if datetime.fromisoformat(r['timestamp']) >= cutoff_time
        ]

    def get_all_report_types(self) -> List[str]:
        """获取所有报告类型"""
        with self._lock:
            return [k for k in self._cache.keys() if not k.endswith('_send')]

    def generate_daily_report(self, date: str = None) -> str:
        """
        生成每日报告发送汇总

        Args:
            date: 日期 YYYY-MM-DD，默认今天

        Returns:
            str: 汇总报告
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        lines = [
            "=" * 70,
            f"【报告发送日报】{date}",
            "=" * 70,
            ""
        ]

        report_types = self.get_all_report_types()

        for report_type in report_types:
            status = self.get_send_status(report_type, date)
            if status:
                status_icon = "✅" if status['status'] == 'success' else "❌"
                lines.append(f"{status_icon} {report_type}")
                lines.append(f"   时间: {status['timestamp']}")
                lines.append(f"   状态: {status['status']}")
                if status.get('error_message'):
                    lines.append(f"   错误: {status['error_message']}")
                if status.get('validation_issues'):
                    lines.append(f"   问题: {len(status['validation_issues'])} 个")
                lines.append("")
            else:
                lines.append(f"⏳ {report_type} - 未发送")
                lines.append("")

        # 添加成功率统计
        lines.append("-" * 70)
        lines.append("【近7天成功率统计】")
        lines.append("-" * 70)

        for report_type in report_types:
            stats = self.get_success_rate(report_type, days=7)
            lines.append(f"{report_type}: {stats['success']}/{stats['total']} "
                        f"({stats['rate']}%)")

        lines.append("=" * 70)

        return "\n".join(lines)


# 全局服务实例
_tracking_service: Optional[ReportTrackingService] = None


def get_tracking_service() -> ReportTrackingService:
    """获取追踪服务实例"""
    global _tracking_service
    if _tracking_service is None:
        _tracking_service = ReportTrackingService()
    return _tracking_service
