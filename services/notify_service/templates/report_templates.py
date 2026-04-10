"""
报告模板管理器
统一管理所有邮件报告模板和配置
"""
from typing import Dict, Optional, List

from .report_config import ReportConfig, BaseReportTemplate
from .morning_report_template import MorningReportTemplate
from .review_report_template import ReviewReportTemplate
from .tracking_report_template import TrackingReportTemplate
from .night_report_template import NightReportTemplate


def get_template(report_name: str) -> Optional[BaseReportTemplate]:
    """获取报告模板实例"""
    templates = {
        'morning_report': MorningReportTemplate,
        'review_report': ReviewReportTemplate,
        'tracking_report': TrackingReportTemplate,
        'night_report': NightReportTemplate,
    }
    template_class = templates.get(report_name)
    if template_class:
        return template_class()
    return None


def list_report_configs() -> List[str]:
    """列出所有报告配置"""
    return ReportConfig.get_all_configs()


__all__ = [
    'ReportConfig',
    'BaseReportTemplate',
    'MorningReportTemplate',
    'ReviewReportTemplate',
    'TrackingReportTemplate',
    'NightReportTemplate',
    'get_template',
    'list_report_configs',
]