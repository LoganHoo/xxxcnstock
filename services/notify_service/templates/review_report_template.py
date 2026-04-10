"""
复盘报告模板
"""
from typing import Dict, List
from .report_config import BaseReportTemplate


class ReviewReportTemplate(BaseReportTemplate):
    """复盘报告模板"""

    def __init__(self):
        super().__init__('review_report')

    def generate(
        self,
        market_data: Dict = None,
        limit_data: Dict = None,
        picks_review_data: Dict = None,
        dq_report: Dict = None,
        okr_data: Dict = None,
        ai_review_data: Dict = None,
        **kwargs
    ) -> str:
        """生成完整复盘报告"""
        lines = []
        lines.append("=" * 70)
        lines.append("【完整复盘报告】A股市场日终总结")
        lines.append(f"生成时间: {self._get_datetime_str()}")
        lines.append("=" * 70)

        if dq_report and self.is_section_enabled('dq_report'):
            lines.extend(self._format_dq_report(dq_report))

        if market_data:
            if self.is_section_enabled('market_overview'):
                lines.extend(self._format_market_overview(market_data))
            if self.is_section_enabled('limit_analysis'):
                lines.extend(self._format_limit_analysis(market_data))
            if self.is_section_enabled('cvd_analysis'):
                lines.extend(self._format_cvd_analysis(market_data))
            if self.is_section_enabled('hot_sectors'):
                lines.extend(self._format_hot_sectors(market_data))

        if picks_review_data and self.is_section_enabled('picks_tracking'):
            lines.extend(self._format_picks_tracking(picks_review_data))

        if okr_data and self.is_section_enabled('okr'):
            lines.extend(self._format_okr(okr_data))

        if ai_review_data and self.is_section_enabled('ai_review'):
            lines.extend(self._format_ai_review(ai_review_data))

        lines.append("\n" + "=" * 70)
        lines.append("【风险提示】本报告仅供参考，不构成投资建议。")
        lines.append("=" * 70)

        return "\n".join(lines)

    def _get_datetime_str(self) -> str:
        """获取日期时间字符串"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M')

    def _format_dq_report(self, dq_report: Dict) -> List[str]:
        lines = []
        completeness = dq_report.get('completeness', {})
        completeness_rate = completeness.get('completeness_rate', 0) * 100 if completeness.get('completeness_rate') else 0
        lines.append("\n【一、数据质量】")
        lines.append("-" * 56)
        lines.append(f"  ● 采集完整度: {completeness_rate:.1f}%")
        lines.append(f"  ● 有效数据: {completeness.get('valid_stocks', 'N/A')}只")
        lines.append("")
        return lines

    def _format_market_overview(self, market_data: Dict) -> List[str]:
        lines = []
        summary = market_data.get('summary', {})
        indices = market_data.get('indices', [])[:2]

        lines.append("\n【二、大盘概况】")
        lines.append("-" * 56)

        has_valid_index = False
        for idx in indices:
            name = idx.get('name', '')
            levels = idx.get('levels', {})
            close = levels.get('close')
            if close and close > 0:
                has_valid_index = True
                lines.append(f"  ● {name}: {close:.2f}")
                lines.append(f"    MA5:{levels.get('ma5', 0):.2f} MA20:{levels.get('ma20', 0):.2f} MA60:{levels.get('ma60', 0):.2f}")

        rising = summary.get('rising_count') or 0
        falling = summary.get('falling_count') or 0
        if rising > 0 or falling > 0:
            lines.append(f"  ● 上涨: {rising}只 | 下跌: {falling}只")
        elif not has_valid_index:
            lines.append("  ⚠️ 大盘数据暂不可用")
        lines.append("")
        return lines

    def _format_limit_analysis(self, market_data: Dict) -> List[str]:
        lines = []
        summary = market_data.get('summary', {})

        lines.append("\n【三、涨跌停】")
        lines.append("-" * 56)
        limit_up = summary.get('limit_up_count') or 0
        limit_down = summary.get('limit_down_count') or 0
        lines.append(f"  ● 涨停: {limit_up}只")
        lines.append(f"  ● 跌停: {limit_down}只")
        if limit_up == 0 and limit_down == 0:
            lines.append("  ⚠️ 涨跌停数据暂不可用")
        lines.append("")
        return lines

    def _format_cvd_analysis(self, market_data: Dict) -> List[str]:
        lines = []
        cvd_data = market_data.get('cvd', {})
        signal_map = {'buy_dominant': '主力净流入', 'sell_dominant': '主力净流出', 'neutral': '多空平衡'}

        lines.append("\n【四、资金流向(CVD)】")
        lines.append("-" * 56)
        signal = cvd_data.get('signal', 'neutral')
        cvd_cumsum = cvd_data.get('cvd_cumsum')
        cvd_trend = cvd_data.get('cvd_trend', 'N/A')

        lines.append(f"  ● 信号: {signal_map.get(signal, 'N/A')}")
        if cvd_cumsum is not None:
            lines.append(f"  ● 累计: {cvd_cumsum:,.0f}")
        else:
            lines.append("  ● 累计: N/A")
        lines.append(f"  ● 趋势: {cvd_trend}")
        if cvd_cumsum == 0 and signal == 'neutral':
            lines.append("  ⚠️ CVD数据暂不可用")
        lines.append("")
        return lines

    def _format_hot_sectors(self, market_data: Dict) -> List[str]:
        lines = []
        sectors = market_data.get('top_sectors', [])[:5]

        if not sectors:
            return lines

        lines.append("\n【五、热点板块】")
        lines.append("-" * 56)
        for i, sector in enumerate(sectors, 1):
            lines.append(f"  {i}. {sector.get('name', 'N/A')}: {sector.get('change', 0):+.2f}%")
        lines.append("")
        return lines

    def _format_picks_tracking(self, picks_review: Dict) -> List[str]:
        lines = []
        summary = picks_review.get('summary', {})
        stocks = picks_review.get('stocks', [])[:10]

        lines.append("\n【六、昨日推荐追踪】")
        lines.append("-" * 56)
        lines.append(f"  推荐总数: {summary.get('total_picks', 0)}只")
        lines.append(f"  上涨: {summary.get('win_count', 0)}只 | 下跌: {summary.get('loss_count', 0)}只")
        lines.append(f"  平均收益: {summary.get('avg_profit_pct', 0):+.2f}%")
        lines.append("")
        lines.append("  股票明细:")
        lines.append("  " + "-" * 52)
        lines.append("  代码      名称       推荐日收盘  今日收盘  涨跌幅")
        lines.append("  " + "-" * 52)

        for stock in stocks:
            code = stock.get('code', '')
            name = stock.get('name', '')[:6]
            prev_close = stock.get('prev_close', 0)
            current = stock.get('current_price', 0)
            change = stock.get('change_pct', 0)
            lines.append(f"  {code:8} {name:8} {prev_close:8.2f} {current:8.2f} {change:+6.2f}%")

        lines.append("")
        return lines

    def _format_okr(self, okr_data: Dict) -> List[str]:
        lines = []

        lines.append("\n【七、OKR目标完成情况】")
        lines.append("-" * 56)

        objectives = okr_data.get('objectives', [])
        for obj in objectives:
            lines.append(f"  ● {obj.get('title', 'N/A')}")
            for kr in obj.get('key_results', []):
                lines.append(f"    - {kr.get('name', 'N/A')}: {kr.get('current', 0)}/{kr.get('target', 0)}")

        lines.append("")
        return lines

    def _format_ai_review(self, ai_review: Dict) -> List[str]:
        lines = []

        lines.append("\n【八、AI复盘与人工修正】")
        lines.append("-" * 56)

        review_text = ai_review.get('review', '')
        correction = ai_review.get('correction', '')

        if review_text:
            lines.append("  AI分析:")
            for line in review_text.split('\n')[:5]:
                lines.append(f"    {line}")

        if correction:
            lines.append("  人工修正:")
            for line in correction.split('\n')[:3]:
                lines.append(f"    {line}")

        lines.append("")
        return lines