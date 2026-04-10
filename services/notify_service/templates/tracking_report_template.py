"""
跟踪报告模板
"""
from typing import Dict, List
from .report_config import BaseReportTemplate


class TrackingReportTemplate(BaseReportTemplate):
    """跟踪报告模板"""

    def __init__(self):
        super().__init__('tracking_report')

    def generate(self, tracking_data: Dict = None, **kwargs) -> str:
        """生成跟踪报告内容"""
        lines = []
        lines.append("【推荐跟踪日报】")
        lines.append("=" * 50)

        if tracking_data:
            stats = tracking_data.get('stats', {})
            lines.append(f"  总跟踪: {stats.get('total', 0)} 只")
            lines.append(f"  上涨: {stats.get('rising', 0)} 只")
            lines.append(f"  下跌: {stats.get('falling', 0)} 只")
            avg_profit = stats.get('avg_profit_pct', 0)
            lines.append(f"  平均收益: {avg_profit:+.2f}%")

        lines.append("")
        lines.append("=" * 50)

        return "\n".join(lines)