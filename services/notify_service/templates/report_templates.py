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
from .morning_shao_report import A股量化战略内参Template, MorningShaoReportTemplate
from .fund_behavior_report_template import FundBehaviorHTMLReport
from .weekly_monthly_report_template import WeeklyMonthlyHTMLReport


def get_template(report_name: str) -> Optional[BaseReportTemplate]:
    """获取报告模板实例"""
    templates = {
        'morning_report': MorningReportTemplate,
        'review_report': ReviewReportTemplate,
        'tracking_report': TrackingReportTemplate,
        'night_report': NightReportTemplate,
        'morning_shao_report': A股量化战略内参Template,
    }
    template_class = templates.get(report_name)
    if template_class:
        return template_class()
    return None


def get_html_template(report_name: str):
    """获取HTML报告模板实例"""
    html_templates = {
        'fund_behavior': FundBehaviorHTMLReport,
        'weekly_monthly': WeeklyMonthlyHTMLReport,
    }
    template_class = html_templates.get(report_name)
    if template_class:
        return template_class()
    return None


def list_report_configs() -> List[str]:
    """列出所有报告配置"""
    return ReportConfig.get_all_configs()


def list_all_templates() -> List[str]:
    """列出所有可用文本模板"""
    return ['morning_report', 'review_report', 'tracking_report', 'night_report', 'morning_shao_report']


__all__ = [
    'ReportConfig',
    'BaseReportTemplate',
    'MorningReportTemplate',
    'ReviewReportTemplate',
    'TrackingReportTemplate',
    'NightReportTemplate',
    'A股量化战略内参Template',
    'MorningShaoReportTemplate',
    'FundBehaviorHTMLReport',
    'WeeklyMonthlyHTMLReport',
    'get_template',
    'get_html_template',
    'list_report_configs',
    'list_all_templates',
]