"""
报告模板管理器
"""
from .report_templates import (
    BaseReportTemplate,
    ReportConfig,
    MorningReportTemplate,
    ReviewReportTemplate,
    TrackingReportTemplate,
    NightReportTemplate,
    A股量化战略内参Template,
    MorningShaoReportTemplate,
    FundBehaviorHTMLReport,
    WeeklyMonthlyHTMLReport,
    get_template,
    get_html_template,
    list_report_configs,
    list_all_templates,
)

__all__ = [
    'BaseReportTemplate',
    'ReportConfig',
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
    'morning_shao_report',
]

from . import morning_shao_report