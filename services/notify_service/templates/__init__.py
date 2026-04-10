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
    get_template,
    list_report_configs
)

__all__ = [
    'BaseReportTemplate',
    'ReportConfig',
    'MorningReportTemplate',
    'ReviewReportTemplate',
    'TrackingReportTemplate',
    'NightReportTemplate',
    'get_template',
    'list_report_configs',
    'morning_shao_report',
    'FundBehaviorHTMLReport',
    'WeeklyMonthlyHTMLReport',
]

from . import morning_shao_report
from .fund_behavior_report_template import FundBehaviorHTMLReport, generate_fund_behavior_html
from .weekly_monthly_report_template import WeeklyMonthlyHTMLReport, generate_weekly_html, generate_monthly_html