"""
A股量化战略内参模板 - 盘前报告
4大模块：环球大局观 + 国内市场基调 + 情绪引擎 + AI综合建议
"""
from typing import Dict, List, Optional
from datetime import datetime


class A股量化战略内参Template:
    """A股量化战略内参模板"""

    def __init__(self):
        self.name = "morning_shao_report"
        self.display_name = "A股量化战略内参"

    def generate(self, data: Dict = None) -> str:
        """生成完整报告"""
        if data is None:
            data = {}

        lines = []
        lines.append("=" * 64)
        lines.append(f"【A股量化战略内参】{self._get_date_str()} 盘前")
        lines.append("=" * 64)
        lines.append("")

        lines.extend(self._format_global_alpha(data.get('global_alpha', {})))
        lines.extend(self._format_domestic_core(data.get('domestic_core', {})))
        lines.extend(self._format_sentiment_engine(data.get('sentiment_engine', {})))
        lines.extend(self._format_ai_strategy(data.get('ai_strategy', {})))

        lines.append("")
        lines.append("=" * 64)
        lines.append("【风险提示】本报告仅供机构投资者参考，不构成投资建议")
        lines.append("=" * 64)

        return "\n".join(lines)

    def _get_date_str(self) -> str:
        return datetime.now().strftime('%Y-%m-%d')

    def _format_global_alpha(self, ga: Dict) -> List[str]:
        """格式化环球大局观"""
        lines = []
        lines.append("一、环球大局观 (Global Alpha)")
        lines.append("-" * 56)

        foreign = ga.get('foreign', {})
        macro = ga.get('macro', {})
        oil_dollar = ga.get('oil_dollar', {})
        commodities = ga.get('commodities', {})

        us = foreign.get('us', {})
        if us:
            lines.append("  ● 外盘动态:")
            for name, val in us.items():
                change = val.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    {name}: {val.get('price', 0)} {arrow}{abs(change):.2f}%")

        a50 = foreign.get('a50', {})
        if a50:
            change = a50.get('change_pct', 0)
            arrow = self._arrow(change)
            lines.append(f"    A50期指: {a50.get('price', 0)} {arrow}{abs(change):.2f}%")

        cnc = foreign.get('china_concept', {})
        if cnc:
            change = cnc.get('change_pct', 0)
            arrow = self._arrow(change)
            lines.append(f"    中概金龙: {cnc.get('price', 0)} {arrow}{abs(change):.2f}%")

        if macro:
            lines.append("  ● 宏观指标:")
            dxy = macro.get('dxy', {})
            if dxy:
                change = dxy.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    美元指数: {dxy.get('value', 0)} {arrow}{abs(change):.2f}%")

            us10y = macro.get('us10y', {})
            if us10y:
                lines.append(f"    美债10Y: {us10y.get('value', 0):.3f}%")

            cny = macro.get('cny', {})
            if cny:
                lines.append(f"    离岸人民币: {cny.get('value', 0)}")

        if oil_dollar:
            lines.append("  ● 石油美元:")
            oil = oil_dollar.get('oil', {})
            if oil:
                brent = oil.get('brent', {})
                if brent:
                    change = brent.get('change_pct', 0)
                    arrow = self._arrow(change)
                    lines.append(f"    布伦特原油: ${brent.get('price', 0)} {arrow}{abs(change):.2f}%")

            wti = oil.get('wti', {})
            if wti:
                change = wti.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    WTI原油: ${wti.get('price', 0)} {arrow}{abs(change):.2f}%")

            notes = oil_dollar.get('notes', [])
            for note in notes[:2]:
                lines.append(f"    📌 {note}")

        if commodities:
            lines.append("  ● 期货大宗:")
            gold = commodities.get('gold', {})
            if gold:
                change = gold.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    黄金: ${gold.get('price', 0)} {arrow}{abs(change):.2f}%")

            copper = commodities.get('copper', {})
            if copper:
                change = copper.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    LME铜: ${copper.get('price', 0)} {arrow}{abs(change):.2f}%")

            lithium = commodities.get('lithium', {})
            if lithium:
                change = lithium.get('change_pct', 0)
                arrow = self._arrow(change)
                lines.append(f"    碳酸锂: ¥{lithium.get('price', 0)} {arrow}{abs(change):.2f}%")

        lines.append("")
        return lines

    def _format_domestic_core(self, dc: Dict) -> List[str]:
        """格式化国内市场基调"""
        lines = []
        lines.append("二、国内市场基调 (Domestic Core)")
        lines.append("-" * 56)

        yesterday = dc.get('yesterday', {})
        if yesterday:
            lines.append("  ● 昨日回顾:")
            indices = yesterday.get('indices', [])
            for idx in indices[:2]:
                name = idx.get('name', '')
                close = idx.get('close') or 0
                change = idx.get('change_pct') or 0
                if close <= 0:
                    continue
                arrow = self._arrow(change)
                lines.append(f"    {name}: {close:.2f} {arrow}{abs(change):.2f}%")

            summary = yesterday.get('summary', {})
            if summary:
                rising = summary.get('rising') or 0
                falling = summary.get('falling') or 0
                limit_up = summary.get('limit_up') or 0
                limit_down = summary.get('limit_down') or 0
                volume = summary.get('volume', 0) or 0
                if rising > 0 or falling > 0:
                    lines.append(f"    上涨: {rising} | 下跌: {falling}")
                    lines.append(f"    涨停: {limit_up} | 跌停: {limit_down}")
                if volume > 10000:
                    lines.append(f"    成交额: {volume/10000:.0f}万亿")
                elif volume > 0:
                    lines.append(f"    成交额: {volume:.0f}亿")
                elif not (rising > 0 or falling > 0):
                    lines.append(f"    ⚠️ 昨日数据暂不可用")

        key_levels = dc.get('key_levels', {})
        if key_levels:
            lines.append("  ● 关键位预测:")
            for name, levels in key_levels.items():
                lines.append(f"    {name}:")
                resistance = levels.get('resistance')
                support = levels.get('support')
                prediction = levels.get('prediction', 'N/A')
                lines.append(f"      压力位: {resistance if resistance and resistance != 'N/A' and resistance != 0 else 'N/A'}")
                lines.append(f"      支撑位: {support if support and support != 'N/A' and support != 0 else 'N/A'}")
                lines.append(f"      预判: {prediction}")

        news = dc.get('news', [])
        if news:
            lines.append("  ● 重大事件:")
            for item in news[:4]:
                lines.append(f"    📰 {item}")

        lines.append("")
        return lines

    def _format_sentiment_engine(self, se: Dict) -> List[str]:
        """格式化情绪引擎"""
        lines = []
        lines.append("三、情绪与打板环境 (Sentiment Engine)")
        lines.append("-" * 56)

        board = se.get('board', {})
        if board:
            lines.append("  ● 打板梯队:")
            highest = board.get('highest', 0)
            lines.append(f"    最高板高度: {highest}连板")
            trend = board.get('trend', 'N/A')
            lines.append(f"    板型趋势: {trend}")

            themes = board.get('themes', [])
            if themes:
                lines.append("    核心题材:")
                for theme in themes[:3]:
                    lines.append(f"      🔥 {theme}")

        bomb = se.get('bomb_rate', {})
        if bomb:
            rate = bomb.get('rate')
            premium = bomb.get('premium')
            if rate is not None and premium is not None:
                lines.append("  ● 炸板分析:")
                lines.append(f"    炸板率: {rate:.1f}%")
                lines.append(f"    溢价率: {premium:.1f}%")

                if rate > 40:
                    lines.append("    ⚠️ 亏钱效应放大，谨慎打板")
                elif rate < 20:
                    lines.append("    ✅ 赚钱效应良好")

        sentiment_data = se.get('sentiment', {})
        fear_greed = sentiment_data.get('fear_greed', {}) if isinstance(sentiment_data, dict) else {}
        if fear_greed:
            value = fear_greed.get('value', 50)
            level = fear_greed.get('level', 'neutral')
            level_cn = {
                'extreme_fear': '极度恐惧',
                'fear': '恐惧',
                'neutral': '中性',
                'greed': '贪婪',
                'extreme_greed': '极度贪婪'
            }.get(level, level)
            lines.append(f"  ● 恐慌贪婪: {value} ({level_cn})")

        vix = sentiment_data.get('vix', {}) if isinstance(sentiment_data, dict) else {}
        if vix:
            lines.append(f"    VIX恐慌指数: {vix.get('value', 'N/A')}")

        lines.append("")
        return lines

    def _format_ai_strategy(self, ai: Dict) -> List[str]:
        """格式化AI综合判断"""
        lines = []
        lines.append("四、AI综合判断与建议 (AI Strategy)")
        lines.append("-" * 56)

        themes = ai.get('focus_themes', [])
        if themes:
            lines.append("  ● 重点布局赛道:")
            for theme in themes:
                lines.append(f"    📈 {theme}")

        stocks = ai.get('stocks', {})
        s_grade = stocks.get('s_grade', [])
        a_grade = stocks.get('a_grade', [])

        if s_grade:
            lines.append("  ● S级打板核心:")
            for stock in s_grade[:3]:
                lines.append(f"    {stock.get('code', '')} {stock.get('name', '')}")
                lines.append(f"      题材: {stock.get('theme', 'N/A')}")

        if a_grade:
            lines.append("  ● A级趋势低吸:")
            for stock in a_grade[:3]:
                lines.append(f"    {stock.get('code', '')} {stock.get('name', '')}")
                lines.append(f"      条件: {stock.get('condition', 'N/A')}")

        reverse = ai.get('reverse_logic', [])
        if reverse:
            lines.append("  ● ⚠️ AI反向逻辑提醒:")
            for item in reverse:
                lines.append(f"    【{item.get('type', '风险')}】{item.get('content', '')}")

        warnings = ai.get('warnings', [])
        if warnings:
            lines.append("  ● 风险警示:")
            for warning in warnings:
                lines.append(f"    ⚠️ {warning}")

        macro_factor = ai.get('macro_factor', {})
        if macro_factor:
            lines.append("  ● 宏观调节因子:")
            m_macro = macro_factor.get('m_macro', 1.0)
            m_sentiment = macro_factor.get('m_sentiment', 1.0)
            lines.append(f"    M_macro: {m_macro:.2f}")
            lines.append(f"    M_sentiment: {m_sentiment:.2f}")

        lines.append("")
        return lines

    def _arrow(self, change: float) -> str:
        """返回箭头符号"""
        if change > 0:
            return "▲"
        elif change < 0:
            return "▼"
        return "─"


def get_量化战略内参_template() -> A股量化战略内参Template:
    """获取模板实例"""
    return A股量化战略内参Template()


MorningShaoReportTemplate = A股量化战略内参Template
