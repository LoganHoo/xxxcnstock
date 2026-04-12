"""
资金行为学策略HTML报告模板
"""
from typing import Dict, List, Optional
from datetime import datetime
import json


class FundBehaviorHTMLReport:
    """资金行为学策略HTML报告生成器"""

    def __init__(self):
        self.report_date = datetime.now().strftime('%Y-%m-%d')

    def generate(self, result: Dict, config: Dict = None) -> str:
        """生成完整的HTML报告"""
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化决策报告 - {self.report_date}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        :root {{
            --bg-primary: #0a0e27;
            --bg-secondary: #111633;
            --bg-card: #1a1f4e;
            --text-primary: #e8eaed;
            --text-secondary: #9aa0a6;
            --accent-blue: #4facfe;
            --accent-cyan: #00f2fe;
            --accent-green: #00e676;
            --accent-red: #ff5252;
            --accent-orange: #ffab40;
            --accent-purple: #b388ff;
            --border-color: #2d3562;
        }}

        body {{
            font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
            background-image:
                radial-gradient(ellipse at top, #1a1f4e 0%, transparent 50%),
                radial-gradient(ellipse at bottom, #0d1229 0%, transparent 50%);
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        .header {{
            text-align: center;
            padding: 40px 20px;
            margin-bottom: 30px;
            background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-card) 100%);
            border-radius: 20px;
            border: 1px solid var(--border-color);
            position: relative;
            overflow: hidden;
        }}

        .header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-cyan), var(--accent-purple));
        }}

        .header h1 {{
            font-size: 2.5em;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-cyan));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 10px;
        }}

        .header .subtitle {{
            color: var(--text-secondary);
            font-size: 1.1em;
        }}

        .market-overview {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}

        .card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 32px rgba(79, 172, 254, 0.15);
        }}

        .card-header {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 20px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border-color);
        }}

        .card-icon {{
            width: 40px;
            height: 40px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.4em;
        }}

        .card-title {{
            font-size: 1.1em;
            font-weight: 600;
            color: var(--text-primary);
        }}

        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }}

        .metric {{
            text-align: center;
            padding: 16px;
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
        }}

        .metric-value {{
            font-size: 1.8em;
            font-weight: 700;
            margin-bottom: 4px;
        }}

        .metric-label {{
            font-size: 0.85em;
            color: var(--text-secondary);
        }}

        .metric-value.positive {{ color: var(--accent-green); }}
        .metric-value.negative {{ color: var(--accent-red); }}
        .metric-value.neutral {{ color: var(--accent-blue); }}
        .metric-value.warning {{ color: var(--accent-orange); }}

        .signal-badge {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.95em;
        }}

        .signal-bullish {{
            background: rgba(0, 230, 118, 0.15);
            color: var(--accent-green);
            border: 1px solid rgba(0, 230, 118, 0.3);
        }}

        .signal-bearish {{
            background: rgba(255, 82, 82, 0.15);
            color: var(--accent-red);
            border: 1px solid rgba(255, 82, 82, 0.3);
        }}

        .signal-neutral {{
            background: rgba(79, 172, 254, 0.15);
            color: var(--accent-blue);
            border: 1px solid rgba(79, 172, 254, 0.3);
        }}

        .position-section {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
            margin-bottom: 30px;
        }}

        .position-bar {{
            display: flex;
            height: 32px;
            border-radius: 16px;
            overflow: hidden;
            margin: 20px 0;
        }}

        .position-segment {{
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9em;
            transition: flex 0.5s ease;
        }}

        .position-trend {{
            background: linear-gradient(90deg, #00c6ff, #0072ff);
        }}

        .position-short {{
            background: linear-gradient(90deg, #f093fb, #f5576c);
        }}

        .position-cash {{
            background: linear-gradient(90deg, #4a4a4a, #2a2a2a);
        }}

        .stock-list {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 12px;
            margin-top: 20px;
        }}

        .stock-item {{
            background: rgba(255,255,255,0.03);
            border-radius: 10px;
            padding: 12px 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border: 1px solid transparent;
            transition: all 0.2s ease;
        }}

        .stock-item:hover {{
            background: rgba(79, 172, 254, 0.1);
            border-color: rgba(79, 172, 254, 0.3);
        }}

        .stock-code {{
            font-weight: 600;
            color: var(--accent-cyan);
        }}

        .stock-tag {{
            font-size: 0.75em;
            padding: 2px 8px;
            border-radius: 4px;
            background: rgba(79, 172, 254, 0.2);
            color: var(--accent-blue);
        }}

        .warning-section {{
            background: linear-gradient(135deg, rgba(255, 82, 82, 0.1), rgba(255, 171, 64, 0.1));
            border: 1px solid rgba(255, 82, 82, 0.3);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 30px;
        }}

        .warning-item {{
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255, 82, 82, 0.1);
        }}

        .warning-item:last-child {{
            border-bottom: none;
        }}

        .warning-icon {{
            font-size: 1.4em;
        }}

        .footer {{
            text-align: center;
            padding: 30px;
            color: var(--text-secondary);
            font-size: 0.85em;
        }}

        .section-title {{
            font-size: 1.3em;
            font-weight: 600;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .section-title::before {{
            content: '';
            width: 4px;
            height: 24px;
            background: linear-gradient(180deg, var(--accent-blue), var(--accent-cyan));
            border-radius: 2px;
        }}

        .index-row {{
            display: flex;
            gap: 20px;
            margin-bottom: 16px;
        }}

        .index-item {{
            flex: 1;
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
            padding: 16px;
            text-align: center;
        }}

        .index-name {{
            color: var(--text-secondary);
            font-size: 0.9em;
            margin-bottom: 8px;
        }}

        .index-value {{
            font-size: 1.5em;
            font-weight: 700;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.6; }}
        }}

        .live-indicator {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: var(--accent-green);
            font-size: 0.85em;
        }}

        .live-indicator::before {{
            content: '';
            width: 8px;
            height: 8px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: pulse 1.5s infinite;
        }}
    </style>
</head>
<body>
    <div class="container">
        {self._generate_header(result)}
        {self._generate_market_overview(result)}
        {self._generate_position_section(result)}
        {self._generate_warnings(result)}
        {self._generate_footer()}
    </div>
</body>
</html>"""

    def _generate_header(self, result: Dict) -> str:
        market_state = result.get('market_state', ['N/A'])[0] if result.get('market_state') else 'N/A'
        state_colors = {
            'STRONG': '#00e676',
            'OSCILLATING': '#ffab40',
            'WEAK': '#ff5252'
        }
        state_color = state_colors.get(market_state, '#9aa0a6')

        # 防守信号 - 最重要的动作指令
        defense = result.get('defense_signals', {})
        defense_action = defense.get('action', 'BUY')
        
        if defense_action == 'DEFENSE':
            market_tone = "⛔ 防守"
            tone_class = "signal-bearish"
            tone_icon = "🛡️"
        elif defense_action == 'CAUTION':
            market_tone = "⚠️ 谨慎"
            tone_class = "signal-neutral"
            tone_icon = "⚡"
        else:
            is_strong = result.get('is_strong_region', False)
            upward_pivot = result.get('upward_pivot', False)
            if is_strong and upward_pivot:
                market_tone = "🚀 强势做多"
                tone_class = "signal-bullish"
                tone_icon = "🚀"
            else:
                market_tone = "⚡ 震荡上行"
                tone_class = "signal-neutral"
                tone_icon = "⚡"

        return f"""
        <div class="header">
            <h1>📊 量化决策报告</h1>
            <p class="subtitle">{self.report_date} | 资金行为学策略</p>
            <div style="margin-top: 20px; display: flex; gap: 16px; justify-content: center;">
                <span class="signal-badge {tone_class}">
                    {tone_icon} {market_tone}
                </span>
                <span class="signal-badge" style="background: rgba(79,172,254,0.15); color: {state_color}; border: 1px solid {state_color}30;">
                    周期定位: {market_state}
                </span>
            </div>
        </div>"""

    def _generate_market_overview(self, result: Dict) -> str:
        v_total = result.get('v_total', 0)
        v_total_display = v_total / 10000 if v_total > 100 else v_total
        v_unit = "万亿" if v_total_display >= 1 else "亿"

        sentiment_temp = result.get('sentiment_temperature', 0)
        hedge_effect = result.get('hedge_effect', False)

        sentiment_class = "positive" if sentiment_temp >= 50 else "warning" if sentiment_temp >= 30 else "negative"

        hedge_class = "positive" if hedge_effect else "negative"
        hedge_text = "量能充沛" if hedge_effect else "量能不足"

        cost_peak = result.get('cost_peak', 0)
        current_price = result.get('current_price', 0)

        price_vs_cost = "高于" if current_price > cost_peak else "低于"

        return f"""
        <div class="market-overview">
            <div class="card">
                <div class="card-header">
                    <div class="card-icon" style="background: linear-gradient(135deg, #667eea, #764ba2);">
                        💰
                    </div>
                    <div class="card-title">量能环境</div>
                </div>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="metric-value neutral">{v_total_display:.2f}</div>
                        <div class="metric-label">总成交额 ({v_unit})</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value {hedge_class}">{hedge_text}</div>
                        <div class="metric-label">对冲效果</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <div class="card-icon" style="background: linear-gradient(135deg, #f093fb, #f5576c);">
                        🌡️
                    </div>
                    <div class="card-title">情绪温度</div>
                </div>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="metric-value {sentiment_class}">{sentiment_temp:.1f}°</div>
                        <div class="metric-label">当前温度</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value neutral">{result.get('delta_temperature', 0):+.1f}°</div>
                        <div class="metric-label">温差惯性</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <div class="card-icon" style="background: linear-gradient(135deg, #4facfe, #00f2fe);">
                        🎯
                    </div>
                    <div class="card-title">筹码分布</div>
                </div>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="metric-value neutral">{cost_peak:.2f}</div>
                        <div class="metric-label">核心筹码位</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value {'positive' if current_price > cost_peak else 'negative'}">{price_vs_cost}</div>
                        <div class="metric-label">当前价 vs 筹码位</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <div class="card-icon" style="background: linear-gradient(135deg, #fa709a, #fee140);">
                        ⚡
                    </div>
                    <div class="card-title">10点变盘</div>
                </div>
                <div style="text-align: center; padding: 20px;">
                    <div class="signal-badge {'signal-bullish' if result.get('upward_pivot') else 'signal-bearish'}">
                        {'✅ 向上变盘' if result.get('upward_pivot') else '❌ 变盘未触发'}
                    </div>
                    <p style="margin-top: 12px; color: var(--text-secondary); font-size: 0.9em;">
                        {'大胆做多，顺势而为' if result.get('upward_pivot') else '日内以防守为主'}
                    </p>
                </div>
            </div>
        </div>"""

    def _generate_position_section(self, result: Dict) -> str:
        position = result.get('position_size', {})
        total_capital = sum(position.values()) if position else 0

        trend_pos = position.get('trend', 0)
        short_pos = position.get('short_term', 0)
        cash_pos = position.get('cash', 0)

        trend_pct = (trend_pos / total_capital * 100) if total_capital > 0 else 0
        short_pct = (short_pos / total_capital * 100) if total_capital > 0 else 0
        cash_pct = (cash_pos / total_capital * 100) if total_capital > 0 else 0

        trend_stocks = result.get('trend_stocks', [])
        short_stocks = result.get('short_term_stocks', [])

        return f"""
        <div class="position-section">
            <div class="section-title">仓位与板块</div>

            <div class="position-bar">
                <div class="position-segment position-trend" style="flex: {trend_pct};">
                    {'波段 ' + str(int(trend_pct)) + '%' if trend_pct > 5 else ''}
                </div>
                <div class="position-segment position-short" style="flex: {short_pct};">
                    {'短线 ' + str(int(short_pct)) + '%' if short_pct > 5 else ''}
                </div>
                <div class="position-segment position-cash" style="flex: {cash_pct};">
                    {'现金 ' + str(int(cash_pct)) + '%' if cash_pct > 5 else ''}
                </div>
            </div>

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin-bottom: 20px;">
                <div style="text-align: center; padding: 16px; background: rgba(0,114,255,0.1); border-radius: 12px;">
                    <div style="font-size: 1.5em; font-weight: 700; color: #00c6ff;">{trend_pos/10000:.0f}万</div>
                    <div style="color: var(--text-secondary); font-size: 0.85em;">波段仓位</div>
                </div>
                <div style="text-align: center; padding: 16px; background: rgba(245,87,108,0.1); border-radius: 12px;">
                    <div style="font-size: 1.5em; font-weight: 700; color: #f5576c;">{short_pos/10000:.0f}万</div>
                    <div style="color: var(--text-secondary); font-size: 0.85em;">短线仓位</div>
                </div>
                <div style="text-align: center; padding: 16px; background: rgba(74,74,74,0.3); border-radius: 12px;">
                    <div style="font-size: 1.5em; font-weight: 700; color: #9aa0a6;">{cash_pos/10000:.0f}万</div>
                    <div style="color: var(--text-secondary); font-size: 0.85em;">现金储备</div>
                </div>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div>
                    <h4 style="color: var(--accent-cyan); margin-bottom: 12px;">📈 波段模式 ({len(trend_stocks)}只)</h4>
                    <div class="stock-list">
                        {''.join([f'<div class="stock-item"><span class="stock-code">{code}</span><span class="stock-tag">MA5</span></div>' for code in trend_stocks[:6]])}
                    </div>
                </div>
                <div>
                    <h4 style="color: var(--accent-purple); margin-bottom: 12px;">⚡ 短线/打板 ({len(short_stocks)}只)</h4>
                    <div class="stock-list">
                        {''.join([f'<div class="stock-item"><span class="stock-code">{code}</span><span class="stock-tag">打板</span></div>' for code in short_stocks[:6]])}
                    </div>
                </div>
            </div>
        </div>"""

    def _generate_warnings(self, result: Dict) -> str:
        warnings = []
        
        # 防守信号
        defense = result.get('defense_signals', {})
        defense_action = defense.get('action', 'BUY')
        
        if defense_action == 'DEFENSE':
            warnings.append(("⛔ 防守指令", "成交量萎缩或跌破支撑位，禁止买入"))
            for reason in defense.get('reasons', []):
                warnings.append(("⚠️ 防守原因", reason))
        elif defense_action == 'CAUTION':
            warnings.append(("⚠️ 谨慎操作", "市场存在风险，需谨慎"))
            for reason in defense.get('reasons', []):
                warnings.append(("⚠️ 风险提示", reason))
        else:
            # 正常买入状态，检查其他风险
            if not result.get('hedge_effect', False):
                warnings.append(("⚠️ 量能预警", "量能不足，对冲效果有限"))
            if result.get('sentiment_temperature', 0) > 80:
                warnings.append(("🔥 情绪预警", "情绪温度进入过热区域"))
            if result.get('delta_temperature', 0) < -20:
                warnings.append(("❄️ 惯性预警", "降温预判次日有惯性杀跌风险"))
            if result.get('current_price', 0) < result.get('cost_peak', 0) and result.get('cost_peak', 0) > 0:
                warnings.append(("📍 筹码预警", "股价跌破筹码峰位"))
        
        # 关键位信息
        details = defense.get('details', {})
        if details.get('near_support'):
            warnings.append(("⚡ 支撑位附近", "接近支撑位，关注反弹机会"))
        if details.get('near_resistance'):
            warnings.append(("⚠️ 阻力位附近", "接近阻力位，注意回落风险"))

        if not warnings:
            warnings.append(("✅ 无风险预警", "市场情绪稳定，可正常执行策略"))

        warnings_html = ""
        for icon_title, desc in warnings:
            is_safe = "✅" in icon_title
            bg_color = "rgba(0,230,118,0.1)" if is_safe else "rgba(255,82,82,0.1)"
            border_color = "rgba(0,230,118,0.3)" if is_safe else "rgba(255,82,82,0.3)"
            warnings_html += f"""
                <div class="warning-item" style="background: {bg_color}; border-color: {border_color}; border-radius: 10px; padding: 16px; margin-bottom: 8px;">
                    <span class="warning-icon">{'✅' if is_safe else '⚠️'}</span>
                    <div>
                        <div style="font-weight: 600;">{icon_title.replace('✅ ', '').replace('⚠️ ', '')}</div>
                        <div style="color: var(--text-secondary); font-size: 0.9em;">{desc}</div>
                    </div>
                </div>"""

        return f"""
        <div class="warning-section">
            <div class="section-title">风险预警</div>
            {warnings_html}
        </div>"""

    def _generate_footer(self) -> str:
        return f"""
        <div class="footer">
            <p>本报告由资金行为学量化策略系统生成</p>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="margin-top: 10px; color: var(--accent-red);">⚠️ 风险提示: 本报告仅供参考，不构成投资建议</p>
        </div>"""


def generate_fund_behavior_html(result: Dict, config: Dict = None) -> str:
    """生成HTML报告的便捷函数"""
    generator = FundBehaviorHTMLReport()
    return generator.generate(result, config)


if __name__ == "__main__":
    sample_result = {
        'market_state': ['STRONG'],
        'is_strong_region': True,
        'upward_pivot': True,
        'v_total': 18500,
        'sentiment_temperature': 65.0,
        'delta_temperature': 15.0,
        'hedge_effect': True,
        'cost_peak': 4067.5,
        'current_price': 4085.2,
        'position_size': {'trend': 500000, 'short_term': 400000, 'cash': 100000},
        'trend_stocks': ['000001', '600519', '000002', '600036', '601318', '000858'],
        'short_term_stocks': ['300999', '301000', '688xxx'],
    }
    html = generate_fund_behavior_html(sample_result)
    with open('/tmp/fund_behavior_report.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("HTML报告已生成: /tmp/fund_behavior_report.html")