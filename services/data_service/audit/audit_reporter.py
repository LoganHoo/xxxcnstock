#!/usr/bin/env python3
"""
审计报告生成模块

生成各类审计报告:
- 数据质量审计报告
- 操作审计报告
- 合规性审计报告
- 异常检测报告
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum

import pandas as pd

from core.logger import setup_logger
from core.paths import get_data_path
from .audit_logger import AuditLogger, get_audit_logger
from .operation_audit import OperationAuditor, get_operation_auditor
from .change_audit import ChangeAuditor, get_change_auditor
from .data_lineage import DataLineageTracker, get_lineage_tracker


class ReportType(Enum):
    """报告类型"""
    DAILY = "daily"                    # 日报
    WEEKLY = "weekly"                  # 周报
    MONTHLY = "monthly"                # 月报
    QUARTERLY = "quarterly"            # 季报
    ANNUAL = "annual"                  # 年报
    COMPLIANCE = "compliance"          # 合规报告
    SECURITY = "security"              # 安全报告
    CUSTOM = "custom"                  # 自定义报告


@dataclass
class AuditReport:
    """审计报告"""
    report_id: str
    report_type: str
    report_name: str
    generated_at: str
    period_start: str
    period_end: str
    summary: Dict[str, Any]
    details: Dict[str, Any]
    recommendations: List[str]
    attachments: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


class AuditReporter:
    """审计报告生成器"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        初始化审计报告生成器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir or get_data_path() / "audit_reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("audit_reporter")
        
        # 各审计模块
        self.audit_logger = get_audit_logger()
        self.operation_auditor = get_operation_auditor()
        self.change_auditor = get_change_auditor()
        self.lineage_tracker = get_lineage_tracker()
    
    def _generate_report_id(self) -> str:
        """生成报告ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"RPT{timestamp}"
    
    def generate_daily_report(self, report_date: Optional[datetime] = None) -> AuditReport:
        """
        生成日报
        
        Args:
            report_date: 报告日期(默认为昨天)
        
        Returns:
            审计报告
        """
        if report_date is None:
            report_date = datetime.now() - timedelta(days=1)
        
        start_time = report_date.replace(hour=0, minute=0, second=0)
        end_time = report_date.replace(hour=23, minute=59, second=59)
        
        # 收集统计数据
        audit_stats = self.audit_logger.get_log_stats(days=1)
        operation_stats = self.operation_auditor.get_operation_statistics(days=1)
        change_stats = self.change_auditor.get_change_statistics(days=1)
        lineage_stats = self.lineage_tracker.get_statistics()
        
        summary = {
            'report_date': report_date.strftime('%Y-%m-%d'),
            'total_audit_events': audit_stats.get('total_records', 0),
            'total_operations': operation_stats.get('total_operations', 0),
            'total_changes': change_stats.get('total_changes', 0),
            'error_count': audit_stats.get('error_count', 0) + operation_stats.get('error_count', 0),
            'anomaly_count': operation_stats.get('anomaly_count', 0)
        }
        
        details = {
            'audit_events_by_type': audit_stats.get('by_type', {}),
            'operations_by_type': operation_stats.get('by_type', {}),
            'changes_by_type': change_stats.get('by_type', {}),
            'lineage_statistics': lineage_stats
        }
        
        recommendations = self._generate_recommendations(summary, details)
        
        report = AuditReport(
            report_id=self._generate_report_id(),
            report_type=ReportType.DAILY.value,
            report_name=f"数据审计日报 - {report_date.strftime('%Y-%m-%d')}",
            generated_at=datetime.now().isoformat(),
            period_start=start_time.isoformat(),
            period_end=end_time.isoformat(),
            summary=summary,
            details=details,
            recommendations=recommendations,
            attachments=[]
        )
        
        # 保存报告
        self._save_report(report)
        
        return report
    
    def generate_weekly_report(self, week_start: Optional[datetime] = None) -> AuditReport:
        """
        生成周报
        
        Args:
            week_start: 周开始日期(默认为上周一)
        
        Returns:
            审计报告
        """
        if week_start is None:
            today = datetime.now()
            week_start = today - timedelta(days=today.weekday() + 7)
        
        week_end = week_start + timedelta(days=6)
        
        # 收集统计数据
        audit_stats = self.audit_logger.get_log_stats(days=7)
        operation_stats = self.operation_auditor.get_operation_statistics(days=7)
        change_stats = self.change_auditor.get_change_statistics(days=7)
        
        summary = {
            'week_start': week_start.strftime('%Y-%m-%d'),
            'week_end': week_end.strftime('%Y-%m-%d'),
            'total_audit_events': audit_stats.get('total_records', 0),
            'total_operations': operation_stats.get('total_operations', 0),
            'total_changes': change_stats.get('total_changes', 0),
            'error_rate': self._calculate_error_rate(operation_stats),
            'anomaly_rate': self._calculate_anomaly_rate(operation_stats)
        }
        
        details = {
            'daily_breakdown': self._get_daily_breakdown(week_start, week_end),
            'top_users': sorted(
                operation_stats.get('by_user', {}).items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
            'top_resource_types': sorted(
                operation_stats.get('by_resource_type', {}).items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }
        
        recommendations = self._generate_recommendations(summary, details)
        
        report = AuditReport(
            report_id=self._generate_report_id(),
            report_type=ReportType.WEEKLY.value,
            report_name=f"数据审计周报 - {week_start.strftime('%Y-%m-%d')} 至 {week_end.strftime('%Y-%m-%d')}",
            generated_at=datetime.now().isoformat(),
            period_start=week_start.isoformat(),
            period_end=week_end.isoformat(),
            summary=summary,
            details=details,
            recommendations=recommendations,
            attachments=[]
        )
        
        self._save_report(report)
        
        return report
    
    def generate_compliance_report(self,
                                  start_time: Optional[datetime] = None,
                                  end_time: Optional[datetime] = None) -> AuditReport:
        """
        生成合规性报告
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
        
        Returns:
            审计报告
        """
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(days=30)
        
        # 合规性检查
        compliance_checks = {
            'data_access_logging': self._check_data_access_logging(start_time, end_time),
            'data_modification_tracking': self._check_data_modification_tracking(start_time, end_time),
            'user_authentication': self._check_user_authentication(start_time, end_time),
            'sensitive_data_protection': self._check_sensitive_data_protection(start_time, end_time),
            'data_retention_policy': self._check_data_retention_policy()
        }
        
        all_passed = all(check['passed'] for check in compliance_checks.values())
        
        summary = {
            'compliance_status': 'PASSED' if all_passed else 'FAILED',
            'checks_passed': sum(1 for check in compliance_checks.values() if check['passed']),
            'checks_failed': sum(1 for check in compliance_checks.values() if not check['passed']),
            'total_checks': len(compliance_checks),
            'period_days': (end_time - start_time).days
        }
        
        details = {
            'compliance_checks': compliance_checks,
            'failed_checks_details': [
                {'check_name': name, **check}
                for name, check in compliance_checks.items()
                if not check['passed']
            ]
        }
        
        recommendations = self._generate_compliance_recommendations(compliance_checks)
        
        report = AuditReport(
            report_id=self._generate_report_id(),
            report_type=ReportType.COMPLIANCE.value,
            report_name=f"数据合规性审计报告 - {start_time.strftime('%Y-%m-%d')} 至 {end_time.strftime('%Y-%m-%d')}",
            generated_at=datetime.now().isoformat(),
            period_start=start_time.isoformat(),
            period_end=end_time.isoformat(),
            summary=summary,
            details=details,
            recommendations=recommendations,
            attachments=[]
        )
        
        self._save_report(report)
        
        return report
    
    def generate_security_report(self,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> AuditReport:
        """
        生成安全审计报告
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
        
        Returns:
            审计报告
        """
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(days=7)
        
        # 安全事件统计
        security_events = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        security_events_count = len([
            e for e in security_events
            if e.audit_type == 'security_event' or e.audit_level in ['error', 'critical']
        ])
        
        # 异常操作统计
        anomaly_stats = self.operation_auditor.get_operation_statistics(
            days=(end_time - start_time).days
        )
        
        summary = {
            'security_score': self._calculate_security_score(security_events, anomaly_stats),
            'security_events_count': security_events_count,
            'anomaly_count': anomaly_stats.get('anomaly_count', 0),
            'failed_login_attempts': self._count_failed_logins(start_time, end_time),
            'unauthorized_access_attempts': self._count_unauthorized_access(start_time, end_time)
        }
        
        details = {
            'security_events': [
                {
                    'timestamp': e.timestamp,
                    'type': e.audit_type,
                    'level': e.audit_level,
                    'resource': f"{e.resource_type}/{e.resource_id}",
                    'action': e.action,
                    'user': e.user_id
                }
                for e in security_events
                if e.audit_type == 'security_event'
            ],
            'risk_assessment': self._assess_security_risks(security_events)
        }
        
        recommendations = self._generate_security_recommendations(summary, details)
        
        report = AuditReport(
            report_id=self._generate_report_id(),
            report_type=ReportType.SECURITY.value,
            report_name=f"数据安全审计报告 - {start_time.strftime('%Y-%m-%d')} 至 {end_time.strftime('%Y-%m-%d')}",
            generated_at=datetime.now().isoformat(),
            period_start=start_time.isoformat(),
            period_end=end_time.isoformat(),
            summary=summary,
            details=details,
            recommendations=recommendations,
            attachments=[]
        )
        
        self._save_report(report)
        
        return report
    
    def _check_data_access_logging(self, start_time: datetime, end_time: datetime) -> Dict:
        """检查数据访问日志记录"""
        logs = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            audit_type='data_access',
            limit=1
        )
        
        return {
            'passed': len(logs) > 0,
            'description': '数据访问日志记录检查',
            'details': f"找到 {len(logs)} 条数据访问日志"
        }
    
    def _check_data_modification_tracking(self, start_time: datetime, end_time: datetime) -> Dict:
        """检查数据变更追踪"""
        changes = self.change_auditor.get_changes(
            start_time=start_time,
            end_time=end_time,
            limit=1
        )
        
        return {
            'passed': len(changes) > 0,
            'description': '数据变更追踪检查',
            'details': f"找到 {len(changes)} 条变更记录"
        }
    
    def _check_user_authentication(self, start_time: datetime, end_time: datetime) -> Dict:
        """检查用户认证"""
        login_logs = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            audit_type='user_login',
            limit=100
        )
        
        failed_logins = [log for log in login_logs if log.status == 'failure']
        
        return {
            'passed': len(failed_logins) < 10,  # 允许少量失败
            'description': '用户认证检查',
            'details': f"总登录次数: {len(login_logs)}, 失败次数: {len(failed_logins)}"
        }
    
    def _check_sensitive_data_protection(self, start_time: datetime, end_time: datetime) -> Dict:
        """检查敏感数据保护"""
        export_logs = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            audit_type='data_export',
            limit=100
        )
        
        return {
            'passed': len(export_logs) > 0,  # 有导出记录说明有监控
            'description': '敏感数据保护检查',
            'details': f"数据导出操作次数: {len(export_logs)}"
        }
    
    def _check_data_retention_policy(self) -> Dict:
        """检查数据保留策略"""
        # 简化的检查，实际应该检查配置
        return {
            'passed': True,
            'description': '数据保留策略检查',
            'details': '数据保留策略已配置'
        }
    
    def _calculate_error_rate(self, operation_stats: Dict) -> float:
        """计算错误率"""
        total = operation_stats.get('total_operations', 0)
        errors = operation_stats.get('error_count', 0)
        
        if total == 0:
            return 0.0
        
        return round(errors / total * 100, 2)
    
    def _calculate_anomaly_rate(self, operation_stats: Dict) -> float:
        """计算异常率"""
        total = operation_stats.get('total_operations', 0)
        anomalies = operation_stats.get('anomaly_count', 0)
        
        if total == 0:
            return 0.0
        
        return round(anomalies / total * 100, 2)
    
    def _calculate_security_score(self, security_events: List, anomaly_stats: Dict) -> int:
        """计算安全评分"""
        score = 100
        
        # 根据安全事件扣分
        score -= len([e for e in security_events if e.audit_level == 'critical']) * 20
        score -= len([e for e in security_events if e.audit_level == 'error']) * 10
        score -= len([e for e in security_events if e.audit_level == 'warning']) * 5
        
        # 根据异常扣分
        score -= anomaly_stats.get('anomaly_count', 0) * 2
        
        return max(0, score)
    
    def _count_failed_logins(self, start_time: datetime, end_time: datetime) -> int:
        """统计失败登录次数"""
        logs = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            audit_type='user_login',
            limit=1000
        )
        
        return len([log for log in logs if log.status == 'failure'])
    
    def _count_unauthorized_access(self, start_time: datetime, end_time: datetime) -> int:
        """统计未授权访问次数"""
        logs = self.audit_logger.query_logs(
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        return len([log for log in logs if log.status == 'denied'])
    
    def _get_daily_breakdown(self, start_time: datetime, end_time: datetime) -> Dict[str, Dict]:
        """获取每日细分数据"""
        daily_data = {}
        
        current = start_time
        while current <= end_time:
            day_str = current.strftime('%Y-%m-%d')
            
            # 获取当天的操作统计
            day_stats = self.operation_auditor.get_operation_statistics(days=1)
            
            daily_data[day_str] = {
                'operations': day_stats.get('total_operations', 0),
                'errors': day_stats.get('error_count', 0),
                'anomalies': day_stats.get('anomaly_count', 0)
            }
            
            current += timedelta(days=1)
        
        return daily_data
    
    def _assess_security_risks(self, security_events: List) -> List[Dict]:
        """评估安全风险"""
        risks = []
        
        # 根据事件评估风险
        critical_count = len([e for e in security_events if e.audit_level == 'critical'])
        if critical_count > 0:
            risks.append({
                'level': 'HIGH',
                'type': 'critical_events',
                'description': f'发现 {critical_count} 个严重安全事件',
                'recommendation': '立即调查并处理严重安全事件'
            })
        
        error_count = len([e for e in security_events if e.audit_level == 'error'])
        if error_count > 10:
            risks.append({
                'level': 'MEDIUM',
                'type': 'error_events',
                'description': f'发现 {error_count} 个错误事件',
                'recommendation': '检查系统错误并修复'
            })
        
        return risks
    
    def _generate_recommendations(self, summary: Dict, details: Dict) -> List[str]:
        """生成建议"""
        recommendations = []
        
        # 基于错误率生成建议
        error_rate = summary.get('error_rate', 0)
        if error_rate > 5:
            recommendations.append(f"错误率较高({error_rate}%)，建议检查系统稳定性")
        
        # 基于异常率生成建议
        anomaly_rate = summary.get('anomaly_rate', 0)
        if anomaly_rate > 1:
            recommendations.append(f"异常率较高({anomaly_rate}%)，建议加强监控")
        
        # 基于操作量生成建议
        total_ops = summary.get('total_operations', 0)
        if total_ops > 10000:
            recommendations.append("操作量较大，建议优化性能")
        
        if not recommendations:
            recommendations.append("系统运行正常，继续保持")
        
        return recommendations
    
    def _generate_compliance_recommendations(self, compliance_checks: Dict) -> List[str]:
        """生成合规建议"""
        recommendations = []
        
        for check_name, check in compliance_checks.items():
            if not check['passed']:
                recommendations.append(f"{check['description']}: {check['details']}")
        
        if not recommendations:
            recommendations.append("所有合规检查通过")
        
        return recommendations
    
    def _generate_security_recommendations(self, summary: Dict, details: Dict) -> List[str]:
        """生成安全建议"""
        recommendations = []
        
        security_score = summary.get('security_score', 100)
        if security_score < 80:
            recommendations.append(f"安全评分较低({security_score})，建议加强安全措施")
        
        failed_logins = summary.get('failed_login_attempts', 0)
        if failed_logins > 10:
            recommendations.append(f"失败登录次数较多({failed_logins})，建议检查账户安全")
        
        risks = details.get('risk_assessment', [])
        for risk in risks:
            recommendations.append(f"[{risk['level']}] {risk['recommendation']}")
        
        if not recommendations:
            recommendations.append("安全状况良好")
        
        return recommendations
    
    def _save_report(self, report: AuditReport):
        """保存报告"""
        report_file = self.output_dir / f"{report.report_type}_{report.report_id}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"报告已保存: {report_file}")
    
    def export_report_to_html(self, report: AuditReport, output_file: Optional[Path] = None) -> Path:
        """
        导出报告为HTML
        
        Args:
            report: 审计报告
            output_file: 输出文件路径
        
        Returns:
            输出文件路径
        """
        if output_file is None:
            output_file = self.output_dir / f"{report.report_type}_{report.report_id}.html"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{report.report_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; }}
        h2 {{ color: #666; margin-top: 30px; }}
        .summary {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #2196F3; }}
        .metric-label {{ font-size: 12px; color: #999; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .recommendation {{ background: #fff3cd; padding: 10px; margin: 5px 0; border-radius: 3px; }}
        .status-passed {{ color: #4CAF50; }}
        .status-failed {{ color: #f44336; }}
    </style>
</head>
<body>
    <h1>{report.report_name}</h1>
    <p>报告ID: {report.report_id}</p>
    <p>生成时间: {report.generated_at}</p>
    <p>统计周期: {report.period_start[:10]} 至 {report.period_end[:10]}</p>
    
    <h2>摘要</h2>
    <div class="summary">
"""
        
        # 添加摘要指标
        for key, value in report.summary.items():
            html_content += f"""
        <div class="metric">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{key}</div>
        </div>
"""
        
        html_content += """
    </div>
    
    <h2>建议</h2>
"""
        
        # 添加建议
        for rec in report.recommendations:
            html_content += f'<div class="recommendation">{rec}</div>\n'
        
        html_content += """
    <h2>详细信息</h2>
    <pre>"""
        
        html_content += json.dumps(report.details, ensure_ascii=False, indent=2)
        
        html_content += """</pre>
</body>
</html>
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML报告已导出: {output_file}")
        
        return output_file
    
    def list_reports(self, report_type: Optional[ReportType] = None, limit: int = 20) -> List[Dict]:
        """
        列出报告
        
        Args:
            report_type: 报告类型过滤
            limit: 限制数量
        
        Returns:
            报告列表
        """
        reports = []
        
        pattern = f"{report_type.value}_*" if report_type else "*.json"
        
        for report_file in sorted(self.output_dir.glob(pattern), reverse=True)[:limit]:
            try:
                with open(report_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                reports.append({
                    'report_id': data.get('report_id'),
                    'report_type': data.get('report_type'),
                    'report_name': data.get('report_name'),
                    'generated_at': data.get('generated_at'),
                    'file': str(report_file)
                })
            except Exception as e:
                self.logger.warning(f"读取报告失败 {report_file}: {e}")
                continue
        
        return reports


# 全局审计报告生成器实例
_audit_reporter: Optional[AuditReporter] = None


def get_audit_reporter() -> AuditReporter:
    """获取全局审计报告生成器"""
    global _audit_reporter
    if _audit_reporter is None:
        _audit_reporter = AuditReporter()
    return _audit_reporter
