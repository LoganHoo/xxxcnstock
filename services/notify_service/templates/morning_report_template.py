"""
晨间报告模板
"""
from typing import Dict, List
from .report_config import BaseReportTemplate


class MorningReportTemplate(BaseReportTemplate):
    """晨间报告模板"""

    def __init__(self):
        super().__init__('morning_report')

    def generate(
        self,
        market_data: Dict = None,
        picks_data: Dict = None,
        foreign_data: Dict = None,
        strategy_data: Dict = None,
        fb_result: Dict = None,
        **kwargs
    ) -> str:
        """生成晨间报告内容"""
        lines = []
        lines.append(f"【量化决策报告 {self._get_date_str()}】")
        lines.append("=" * 56)
        lines.append("")

        # 1. 外盘动态 - 始终显示
        if self.is_section_enabled('foreign_data'):
            if foreign_data:
                lines.extend(self._format_foreign_data(foreign_data))
            else:
                lines.extend(self._format_placeholder("1. 外盘动态", "数据暂不可用"))

        # 2. 资金行为学决策
        if self.is_section_enabled('fund_behavior'):
            if fb_result:
                lines.extend(self._format_fund_behavior(fb_result))
            else:
                lines.extend(self._format_placeholder("2. 资金行为学决策", "数据暂不可用"))

        # 3. 大盘关键位
        if self.is_section_enabled('market_levels'):
            if market_data:
                lines.extend(self._format_market_levels(market_data))
            else:
                lines.extend(self._format_placeholder("3. 大盘关键位", "数据暂不可用"))

        # 4-6. 选股相关
        if picks_data:
            if self.is_section_enabled('s_grade'):
                s_lines = self._format_s_grade(picks_data)
                if s_lines:
                    lines.extend(s_lines)
                else:
                    lines.extend(self._format_placeholder("4. S级打板候选", "今日无S级推荐"))
            if self.is_section_enabled('a_grade'):
                a_lines = self._format_a_grade(picks_data)
                if a_lines:
                    lines.extend(a_lines)
                else:
                    lines.extend(self._format_placeholder("5. A级波段候选", "今日无A级推荐"))
            if self.is_section_enabled('limit_analysis'):
                lines.extend(self._format_limit_analysis(picks_data))
        else:
            if self.is_section_enabled('s_grade'):
                lines.extend(self._format_placeholder("4. S级打板候选", "数据暂不可用"))
            if self.is_section_enabled('a_grade'):
                lines.extend(self._format_placeholder("5. A级波段候选", "数据暂不可用"))

        # 7. 策略综合
        if self.is_section_enabled('strategy_summary'):
            if strategy_data:
                lines.extend(self._format_strategy_summary(strategy_data))
            else:
                lines.extend(self._format_placeholder("6. 策略综合", "数据暂不可用"))

        lines.append("=" * 56)
        lines.append("【风险提示】本报告仅供参考，不构成投资建议。")
        lines.append("=" * 56)

        return "\n".join(lines)

    def _format_placeholder(self, title: str, message: str) -> List[str]:
        """格式化占位符章节"""
        lines = []
        lines.append(f"【{title}】")
        lines.append("-" * 56)
        lines.append(f"  ⚠️ {message}")
        lines.append("")
        return lines

    def _format_foreign_data(self, foreign_data: Dict) -> List[str]:
        """格式化外盘数据"""
        lines = []
        lines.append("【1. 外盘动态】")
        lines.append("-" * 56)

        us = foreign_data.get('us_index', {}).get('data', {})
        asia = foreign_data.get('asia_index', {}).get('data', {})

        has_us = False
        if us:
            for name, data in us.items():
                price = data.get('price')
                if price and price > 0:
                    if not has_us:
                        lines.append("  ● 美股:")
                        has_us = True
                    name_map = {'nasdaq': '纳斯达克', 'sp500': '标普500', 'dow': '道琼斯'}
                    display_name = name_map.get(name, name)
                    lines.append(f"    {display_name}: {price} ({data.get('change_pct', 0):+.2f}%)")

        has_asia = False
        if asia:
            for name, data in asia.items():
                price = data.get('price')
                if price and price > 0:
                    if not has_asia:
                        lines.append("  ● 亚洲:")
                        has_asia = True
                    name_map = {'hang_seng': '恒生', 'nikkei': '日经', 'kospi': '韩综'}
                    display_name = name_map.get(name, name)
                    lines.append(f"    {display_name}: {price} ({data.get('change_pct', 0):+.2f}%)")

        if not has_us and not has_asia:
            lines.append("  ⚠️ 外盘数据暂不可用")
        lines.append("")
        return lines

    def _format_market_levels(self, market_data: Dict) -> List[str]:
        """格式化大盘关键位"""
        lines = []
        lines.append("【2. 大盘关键位】")
        lines.append("-" * 56)

        indices = market_data.get('indices', [])
        has_valid = False
        for idx in indices[:2]:
            name = idx.get('name', '')
            levels = idx.get('levels', {})
            cvd = idx.get('cvd', {})
            analysis = idx.get('analysis', {})

            close = levels.get('close')
            if close and close > 0:
                has_valid = True
                lines.append(f"  ● {name}:")
                lines.append(f"    收盘: {close}")
                ma5 = levels.get('ma5') or 0
                ma20 = levels.get('ma20') or 0
                ma60 = levels.get('ma60') or 0
                lines.append(f"    MA5: {ma5:.2f} | MA20: {ma20:.2f} | MA60: {ma60:.2f}")
                high_60 = levels.get('high_60') or 0
                low_60 = levels.get('low_60') or 0
                lines.append(f"    60日高: {high_60:.2f} | 60日低: {low_60:.2f}")
                res_1 = levels.get('resistance_1') or 0
                sup_1 = levels.get('support_1') or 0
                lines.append(f"    压力位: {res_1:.2f} | 支撑位: {sup_1:.2f}")
                lines.append(f"    CVD信号: {cvd.get('signal', 'neutral')} ({cvd.get('cvd_trend', '')})")
                lines.append(f"    结论: {analysis.get('conclusion', '')} → {analysis.get('action', '')}")
                lines.append("")

        if not has_valid:
            lines.append("  ⚠️ 大盘关键位数据暂不可用")
            lines.append("")
        return lines

    def _format_s_grade(self, picks_data: Dict) -> List[str]:
        """格式化S级打板股票"""
        lines = []
        s_grade = picks_data.get('filters', {}).get('s_grade', {})
        stocks = s_grade.get('stocks', [])[:5]

        if not stocks:
            return lines

        lines.append("【3. S级打板候选】")
        lines.append("-" * 56)

        for stock in stocks:
            lines.append(f"  ● {stock.get('code', '')} {stock.get('name', '')}:")
            lines.append(f"    现价: {stock.get('price', 0)} | 涨停: {stock.get('change_pct', 0):.1f}%")
            lines.append(f"    题材: {stock.get('reasons', '')}")
            lines.append(f"    支撑: {stock.get('support_5d', 0):.2f} | 压力: {stock.get('resistance_5d', 0):.2f}")
            lines.append(f"    RSI: {stock.get('rsi', 0):.1f} | 评分: {stock.get('enhanced_score', 0)}")
            lines.append("")

        return lines

    def _format_a_grade(self, picks_data: Dict) -> List[str]:
        """格式化A级波段股票"""
        lines = []
        a_grade = picks_data.get('filters', {}).get('a_grade', {})
        stocks = a_grade.get('stocks', [])[:5]

        if not stocks:
            return lines

        lines.append("【4. A级波段候选】")
        lines.append("-" * 56)

        for stock in stocks:
            lines.append(f"  ● {stock.get('code', '')} {stock.get('name', '')}:")
            lines.append(f"    现价: {stock.get('price', 0)} | 涨幅: {stock.get('change_pct', 0):.1f}%")
            lines.append(f"    题材: {stock.get('reasons', '')}")
            lines.append(f"    20日高: {stock.get('high_20', 0):.2f} | 20日低: {stock.get('low_20', 0):.2f}")
            lines.append(f"    RSI: {stock.get('rsi', 0):.1f} | 评分: {stock.get('enhanced_score', 0)}")
            lines.append("")

        return lines

    def _format_limit_analysis(self, picks_data: Dict) -> List[str]:
        """格式化涨停分析"""
        lines = []
        filters = picks_data.get('filters', {})

        limit_up_count = 0
        for grade_key in ['s_grade', 'a_grade', 'bullish']:
            stocks = filters.get(grade_key, {}).get('stocks', [])
            limit_up_count += len([s for s in stocks if s.get('change_pct', 0) >= 9.5])

        if limit_up_count > 0:
            lines.append("【5. 涨停情绪】")
            lines.append("-" * 56)
            lines.append(f"  ● 强势股数量: {limit_up_count}只")
            lines.append("")

        return lines

    def _format_strategy_summary(self, strategy_data: Dict) -> List[str]:
        """格式化策略综合"""
        lines = []

        if not strategy_data:
            return lines

        lines.append("【6. 策略综合】")
        lines.append("-" * 56)

        best = strategy_data.get('best_strategy', {})
        if best:
            lines.append(f"  ● 最优策略: {best.get('name', '')}")
            lines.append(f"    收益率: {best.get('return_rate', 0):.2f}%")
            lines.append(f"    胜率: {best.get('win_rate', 0):.1f}%")

        lines.append("")
        return lines

    def _format_fund_behavior(self, fb_result: Dict) -> List[str]:
        """格式化资金行为学策略结果"""
        lines = []

        if not fb_result:
            return lines

        lines.append("【2. 资金行为学决策】")
        lines.append("-" * 56)

        market_state = fb_result.get('market_state', [])
        if market_state:
            state_counts = {}
            for s in market_state:
                state_counts[s] = state_counts.get(s, 0) + 1
            dominant_state = max(state_counts, key=state_counts.get)
            lines.append(f"  ● 周期定位: {dominant_state.upper()}")

        v_total = fb_result.get('v_total', 0)
        sentiment_temp = fb_result.get('sentiment_temperature', 0)
        lines.append(f"  ● 量能: {v_total/10000:.2f}万亿")
        lines.append(f"  ● 情绪温度: {sentiment_temp:.1f}°")

        cvd_signal = fb_result.get('cvd_signal', 'neutral')
        lines.append(f"  ● CVD信号: {cvd_signal}")

        position = fb_result.get('position', {})
        if position:
            lines.append(f"  ● 仓位建议:")
            lines.append(f"    波段: {position.get('trend', 0):.0%}")
            lines.append(f"    短线: {position.get('short_term', 0):.0%}")
            lines.append(f"    现金: {position.get('cash', 0):.0%}")

        upward_pivot = fb_result.get('upward_pivot', False)
        if upward_pivot:
            lines.append("  ● 10点变盘: ✓ 向上变盘信号触发")
        else:
            lines.append("  ● 10点变盘: ✗ 未触发")

        lines.append("")
        return lines