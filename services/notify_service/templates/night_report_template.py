"""
夜间报告模板
"""
from typing import Dict, List
from .report_config import BaseReportTemplate


class NightReportTemplate(BaseReportTemplate):
    """夜间报告模板"""

    def __init__(self):
        super().__init__('night_report')

    def generate(
        self,
        picks_data: Dict = None,
        market_data: Dict = None,
        **kwargs
    ) -> str:
        """生成夜间报告内容"""
        lines = []
        lines.append("【夜间选股报告】")
        lines.append("=" * 50)

        if picks_data and self.is_section_enabled('picks_data'):
            filters = picks_data.get('filters', {})
            for grade_key, grade_name in [
                ('s_grade', 'S级'),
                ('a_grade', 'A级'),
            ]:
                stocks = filters.get(grade_key, {}).get('stocks', [])
                if stocks:
                    lines.append("")
                    lines.append(f"【{grade_name}】({len(stocks)}只)")
                    for s in stocks[:5]:
                        lines.append(f"  {s.get('code', '')} {s.get('name', '')} 评分:{s.get('score', 0)}")

        lines.append("")
        lines.append("=" * 50)

        return "\n".join(lines)