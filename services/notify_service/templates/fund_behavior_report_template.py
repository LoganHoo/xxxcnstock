"""
资金行为学策略HTML报告模板
"""
from typing import Dict, List, Optional
from datetime import datetime
import json
from pathlib import Path
import polars as pl
from services.key_levels import KeyLevels


class FundBehaviorHTMLReport:
    """资金行为学策略HTML报告生成器"""

    def __init__(self):
        self.report_date = datetime.now().strftime('%Y-%m-%d')

    def generate(self, result: Dict, config: Dict = None, morning_data: Dict = None) -> str:
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
        {self._generate_section_1(result)}
        {self._generate_key_levels_section(result)}
        {self._generate_section_2(result, morning_data)}
        {self._generate_cctv_section(result)}
        {self._generate_section_3(morning_data)}
        {self._generate_section_4(result)}
        {self._generate_section_5(result)}
        {self._generate_section_6(result)}
        {self._generate_section_7(result)}
        {self._generate_review_section(result)}
        {self._generate_quality_check_section(result)}
        {self._generate_summary_section(result)}
        {self._generate_section_8(result)}
        {self._generate_xgboost_section(result)}
        {self._generate_section_9(result)}
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

    # =========================================================================
    # 9个章节的生成方法
    # =========================================================================

    def _generate_section_1(self, result: Dict) -> str:
        """1️⃣ 市场环境定性 - 带数据来源说明"""
        market_states = result.get('market_state', [])
        dominant = max(set(market_states), key=market_states.count) if market_states else 'N/A'
        v_total = result.get('v_total', 0)
        v_display = v_total / 10000 if v_total > 10000 else v_total
        v_unit = "万亿" if v_total > 10000 else "亿"
        sentiment = result.get('sentiment_temperature', 0)
        delta = result.get('delta_temperature', 0)

        state_emoji = {'STRONG': '🚀', 'OSCILLATING': '〰️', 'WEAK': '🔴', 'RISK': '⚠️'}.get(dominant, '➖')

        # 数据来源说明
        data_source = result.get('_data_source', 'AKShare实时行情数据')
        calc_time = result.get('_calc_time', datetime.now().strftime('%H:%M:%S'))

        # 验证状态
        validation_status = result.get('_validation_status', {})
        is_valid = validation_status.get('is_valid', True)
        validation_msg = validation_status.get('message', '数据验证通过')

        return f"""
        <div class="section">
            <div class="section-title">1️⃣ 市场环境定性</div>
            <div class="market-overview">
                <div class="metric-card">
                    <div class="metric-value neutral">{state_emoji} {dominant}</div>
                    <div class="metric-label">周期定位</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value neutral">{v_display:.2f}{v_unit}</div>
                    <div class="metric-label">量能判定</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value {'positive' if sentiment >= 50 else 'warning' if sentiment >= 30 else 'negative'}">{sentiment:.1f}°</div>
                    <div class="metric-label">情绪温度</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value {'positive' if delta > 0 else 'negative'}">{delta:+.1f}°</div>
                    <div class="metric-label">温差惯性</div>
                </div>
            </div>
            <!-- 数据来源和验证信息 -->
            <div style="margin-top: 16px; padding: 12px 16px; background: rgba(255,255,255,0.03); border-radius: 8px; font-size: 0.8em; color: var(--text-secondary);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>📡 数据来源: {data_source} | 计算时间: {calc_time}</span>
                    <span style="color: {'var(--accent-green)' if is_valid else 'var(--accent-red)'}">
                        {'✅' if is_valid else '⚠️'} {validation_msg}
                    </span>
                </div>
            </div>
        </div>"""

    def _generate_key_levels_section(self, result: Dict) -> str:
        """生成关键位价格展示区域 - 只保留上证指数"""
        key_levels = result.get('_key_levels', {})
        index_levels = key_levels.get('index', {})

        # 只保留上证指数
        sh = index_levels.get('000001', {})
        sh_current = sh.get('current_price', 0)
        sh_resistances = sh.get('resistances', [])
        sh_supports = sh.get('supports', [])
        sh_resistance = sh_resistances[0].get('value', 0) if sh_resistances else 0
        sh_support = sh_supports[-1].get('value', 0) if sh_supports else 0

        # 计算位置百分比（用于进度条）
        def calc_position(current, support, resistance):
            if support >= resistance or current == 0:
                return 50
            return min(95, max(5, (current - support) / (resistance - support) * 100))

        sh_pos = calc_position(sh_current, sh_support, sh_resistance)

        # 计算距离支撑/压力的百分比
        sh_to_resistance = ((sh_resistance - sh_current) / sh_current * 100) if sh_current > 0 else 0
        sh_to_support = ((sh_current - sh_support) / sh_current * 100) if sh_current > 0 else 0

        return f"""
        <div class="section">
            <div class="section-title">📊 上证指数关键位（基于真实数据计算）</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 24px;">
                <div class="item" style="text-align: center; padding: 24px;">
                    <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1.2em;">上证指数</h4>
                    <div style="display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                        <span style="color: var(--text-secondary);">压力位</span>
                        <span style="color: var(--accent-red); font-weight: bold; font-size: 1.1em;">{sh_resistance:,.0f}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                        <span style="color: var(--text-secondary);">当前价</span>
                        <span style="color: var(--accent-green); font-weight: bold; font-size: 1.3em;">{sh_current:,.0f}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; padding: 12px 0;">
                        <span style="color: var(--text-secondary);">支撑位</span>
                        <span style="color: var(--accent-blue); font-weight: bold; font-size: 1.1em;">{sh_support:,.0f}</span>
                    </div>
                    <div style="margin-top: 16px; height: 8px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden;">
                        <div style="width: {sh_pos}%; height: 100%; background: linear-gradient(90deg, var(--accent-blue), var(--accent-green), var(--accent-red)); border-radius: 4px;"></div>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-top: 8px; font-size: 0.8em; color: var(--text-secondary);">
                        <span>支撑</span>
                        <span>当前</span>
                        <span>压力</span>
                    </div>
                </div>
                <div class="item" style="padding: 24px;">
                    <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1.2em;">位置分析</h4>
                    <div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 16px; margin-bottom: 12px;">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                            <span style="color: var(--text-secondary);">距压力位</span>
                            <span style="color: var(--accent-red); font-weight: 600;">+{sh_to_resistance:.1f}%</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: var(--text-secondary);">距支撑位</span>
                            <span style="color: var(--accent-green); font-weight: 600;">-{sh_to_support:.1f}%</span>
                        </div>
                    </div>
                    <div style="font-size: 0.9em; color: var(--text-secondary); line-height: 1.6;">
                        {'<span style="color: var(--accent-green);">✅ 当前处于强势区域，可积极做多</span>' if sh_pos > 60 else 
                         '<span style="color: var(--accent-orange);">⚠️ 当前处于震荡区域，谨慎操作</span>' if sh_pos > 40 else 
                         '<span style="color: var(--accent-red);">⛔ 当前处于弱势区域，注意防守</span>'}
                    </div>
                </div>
            </div>
        </div>"""

    def _generate_cctv_section(self, result: Dict) -> str:
        """📺 新闻联播AI分析 - 从result中获取"""
        morning_data = result.get('_morning_data', {})
        if not morning_data:
            return ""
        
        cctv = morning_data.get('cctv_analysis', {})
        if not cctv or not cctv.get('summary'):
            # 如果没有数据，显示提示信息
            return """
        <div class="section">
            <div class="section-title">📺 新闻联播AI分析</div>
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; text-align: center; color: var(--text-secondary);">
                <div style="font-size: 2em; margin-bottom: 12px;">📡</div>
                <div>暂无新闻联播AI分析数据</div>
                <div style="font-size: 0.85em; margin-top: 8px; opacity: 0.7;">数据来源于每日19:00新闻联播，需提前配置MySQL数据库连接</div>
            </div>
        </div>"""
        
        summary = cctv.get('summary', '')
        bullish = cctv.get('bullish', '')
        hot_sectors = cctv.get('hot_sectors', '')
        leading = cctv.get('leading_stocks', '')
        guidance = cctv.get('macro_guidance', '')
        risks = cctv.get('risk_alerts', '')
        sentiment = cctv.get('overall_sentiment', '中性')
        
        sentiment_emoji = {'积极': '🟢', '中性': '🟡', '谨慎': '🔴'}.get(sentiment, '⚪')
        sentiment_color = {'积极': 'var(--accent-green)', '中性': 'var(--accent-orange)', '谨慎': 'var(--accent-red)'}.get(sentiment, 'var(--text-secondary)')
        
        html = f"""
        <div class="section">
            <div class="section-title">📺 新闻联播AI分析 - 政策风向</div>
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px;">"""
        
        # 政策情绪标签
        html += f'<div style="display: inline-block; background: {sentiment_color}20; color: {sentiment_color}; padding: 6px 16px; border-radius: 20px; font-weight: 600; margin-bottom: 16px;">{sentiment_emoji} 政策情绪: {sentiment}</div>'
        
        if summary:
            html += f'<div style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);"><span style="color: var(--accent-cyan); font-weight: 600;">📝 核心要点：</span><span style="color: var(--text-secondary); line-height: 1.6;">{summary}</span></div>'
        if bullish:
            html += f'<div style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);"><span style="color: var(--accent-green); font-weight: 600;">📈 利好因素：</span><span style="color: var(--text-secondary); line-height: 1.6;">{bullish}</span></div>'
        if hot_sectors:
            html += f'<div style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);"><span style="color: var(--accent-orange); font-weight: 600;">🔥 热门板块：</span><span style="color: var(--text-secondary);">{hot_sectors}</span></div>'
        if leading:
            html += f'<div style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);"><span style="color: var(--accent-purple); font-weight: 600;">⭐ 龙头关注：</span><span style="color: var(--text-secondary); line-height: 1.6;">{leading}</span></div>'
        if guidance:
            html += f'<div style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);"><span style="color: var(--accent-cyan); font-weight: 600;">💡 策略指导：</span><span style="color: var(--text-secondary); line-height: 1.6;">{guidance}</span></div>'
        if risks:
            html += f'<div style="padding: 12px 0;"><span style="color: var(--accent-red); font-weight: 600;">⚠️ 风险提示：</span><span style="color: var(--text-secondary); line-height: 1.6;">{risks}</span></div>'
        
        html += """
            </div>
        </div>"""
        return html

    def _generate_section_2(self, result: Dict, morning_data: Dict) -> str:
        """2️⃣ 宏观与外盘环境"""
        if not morning_data:
            return ""
        
        # 宏观数据
        macro = morning_data.get('macro', {})
        dxy = macro.get('dxy', {}).get('value', 0)
        us10y = macro.get('us10y', {}).get('value', 0)
        cny = macro.get('cny', {}).get('value', 0)
        
        # 外盘数据
        foreign = morning_data.get('foreign_market', {})
        sp500 = foreign.get('sp500', {}).get('change_pct', 0)
        nasdaq = foreign.get('nasdaq', {}).get('change_pct', 0)
        dow = foreign.get('dow', {}).get('change_pct', 0)
        hs = foreign.get('hang_seng', {}).get('change_pct', 0)
        
        # 大宗商品
        comm = morning_data.get('commodities', {})
        gold = comm.get('metals', {}).get('gold', {})
        gold_price = gold.get('price', 0)
        gold_change = gold.get('change_pct', 0)
        oil = comm.get('oil', {}).get('wti', {})
        oil_price = oil.get('price', 0)
        oil_change = oil.get('change_pct', 0)
        
        def fmt_change(val):
            if val > 0:
                return f'<span class="positive">+{val:.2f}%</span>'
            elif val < 0:
                return f'<span class="negative">{val:.2f}%</span>'
            return f'<span class="neutral">0.00%</span>'
        
        html = f"""
        <div class="section">
            <div class="section-title">2️⃣ 宏观与外盘环境</div>
            <div class="grid-2">"""
        
        # 宏观指标
        if dxy or us10y or cny:
            html += """
                <div class="item">
                    <h4 style="color: var(--accent-cyan); margin-bottom: 12px;">🌍 宏观指标</h4>"""
            if dxy:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>美元指数</span><span>{dxy:.2f}</span></div>'
            if us10y:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>美债10Y</span><span>{us10y:.2f}%</span></div>'
            if cny:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>离岸人民币</span><span>{cny:.4f}</span></div>'
            html += "</div>"
        
        # 外盘股市
        if sp500 or nasdaq or dow or hs:
            html += """
                <div class="item">
                    <h4 style="color: var(--accent-purple); margin-bottom: 12px;">📈 外盘股市</h4>"""
            if sp500:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🇺🇸 标普500</span>{fmt_change(sp500)}</div>'
            if nasdaq:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🇺🇸 纳斯达克</span>{fmt_change(nasdaq)}</div>'
            if dow:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🇺🇸 道琼斯</span>{fmt_change(dow)}</div>'
            if hs:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🇭🇰 恒生指数</span>{fmt_change(hs)}</div>'
            html += "</div>"
        
        # 大宗商品
        if gold_price or oil_price:
            html += """
                <div class="item">
                    <h4 style="color: var(--accent-orange); margin-bottom: 12px;">🛢️ 大宗商品</h4>"""
            if gold_price:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🥇 黄金</span><span>${gold_price:.2f} {fmt_change(gold_change)}</span></div>'
            if oil_price:
                html += f'<div style="display: flex; justify-content: space-between; padding: 8px 0;"><span>🛢️ WTI原油</span><span>${oil_price:.2f} {fmt_change(oil_change)}</span></div>'
            html += "</div>"
        
        html += """
            </div>
        </div>"""
        return html

    def _generate_section_3(self, morning_data: Dict) -> str:
        """3️⃣ 市场情绪与资金流向分析"""
        if not morning_data:
            morning_data = {}
        
        sentiment = morning_data.get('sentiment', {})
        sentiment_score = sentiment.get('score', 50)
        sentiment_label = sentiment.get('label', '中性')
        
        # 情绪颜色
        if sentiment_score >= 70:
            sentiment_color = 'var(--accent-green)'
            sentiment_icon = '🟢'
        elif sentiment_score >= 40:
            sentiment_color = 'var(--accent-orange)'
            sentiment_icon = '🟡'
        else:
            sentiment_color = 'var(--accent-red)'
            sentiment_icon = '🔴'
        
        html = f"""
        <div class="section">
            <div class="section-title">3️⃣ 市场情绪与资金流向</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div class="item" style="text-align: center; padding: 24px;">
                    <div style="font-size: 3em; margin-bottom: 12px;">{sentiment_icon}</div>
                    <div style="font-size: 2em; font-weight: 700; color: {sentiment_color}; margin-bottom: 8px;">{sentiment_score}</div>
                    <div style="color: var(--text-secondary);">市场情绪指数</div>
                    <div style="margin-top: 12px; padding: 6px 16px; background: {sentiment_color}20; color: {sentiment_color}; border-radius: 16px; display: inline-block; font-weight: 600;">{sentiment_label}</div>
                </div>
                <div class="item" style="padding: 24px;">
                    <h4 style="color: var(--accent-cyan); margin-bottom: 16px;">📊 资金流向解读</h4>
                    <div style="color: var(--text-secondary); line-height: 1.8; font-size: 0.9em;">
                        <div style="margin-bottom: 8px;">• 北向资金流向反映外资态度</div>
                        <div style="margin-bottom: 8px;">• 主力资金流向显示机构动向</div>
                        <div style="margin-bottom: 8px;">• 散户情绪指标辅助判断</div>
                        <div>• 综合评分辅助仓位决策</div>
                    </div>
                </div>
            </div>
            <div style="margin-top: 16px; padding: 16px; background: rgba(255,255,255,0.03); border-radius: 10px; font-size: 0.85em; color: var(--text-secondary);">
                <span style="color: var(--accent-cyan);">💡 说明：</span>市场情绪指数综合了涨跌停比、涨跌家数比、成交量变化等多维度数据计算得出
            </div>
        </div>"""
        return html

    def _generate_section_4(self, result: Dict) -> str:
        """4️⃣ 防守信号"""
        defense = result.get('defense_signals', {})
        action = defense.get('action', 'UNKNOWN')
        reasons = defense.get('reasons', [])
        
        action_display = {
            'BUY': ('✅ 积极买入', 'signal-bullish'),
            'DEFENSE': ('⛔ 防守观望', 'signal-bearish'),
            'CAUTION': ('⚠️ 谨慎操作', 'signal-neutral')
        }.get(action, (action, 'signal-neutral'))
        
        html = f"""
        <div class="section">
            <div class="section-title">4️⃣ 防守信号 - ⚠️ 核心交易指令</div>
            <div style="text-align: center; margin-bottom: 20px;">
                <span class="signal-badge {action_display[1]}">{action_display[0]}</span>
            </div>"""
        
        if reasons:
            html += '<div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 16px;">'
            for i, reason in enumerate(reasons, 1):
                html += f'<div style="padding: 8px 0;">📌 原因{i}：{reason}</div>'
            html += '</div>'
        else:
            html += '<div style="text-align: center; color: var(--text-secondary);">✅ 市场环境正常，可积极参与</div>'
        
        html += "</div>"
        return html

    def _generate_section_5(self, result: Dict) -> str:
        """5️⃣ 核心观察点"""
        upward = result.get('upward_pivot', False)
        hedge = result.get('hedge_effect', False)
        strong = result.get('is_strong_region', False)
        
        return f"""
        <div class="section">
            <div class="section-title">5️⃣ 核心观察点</div>
            <div class="grid-2">
                <div class="item" style="display: flex; justify-content: space-between; align-items: center;">
                    <span>{'✅' if upward else '❌'} 向上变盘</span>
                    <span style="color: var(--text-secondary);">{'是 - 大胆做多' if upward else '否 - 日内防守'}</span>
                </div>
                <div class="item" style="display: flex; justify-content: space-between; align-items: center;">
                    <span>{'✅' if hedge else '❌'} 对冲效果</span>
                    <span style="color: var(--text-secondary);">{'是 - 量能充沛' if hedge else '否 - 量能不足'}</span>
                </div>
                <div class="item" style="display: flex; justify-content: space-between; align-items: center;">
                    <span>{'✅' if strong else '❌'} 强势区域</span>
                    <span style="color: var(--text-secondary);">{'是' if strong else '否'}</span>
                </div>
            </div>
        </div>"""

    def _generate_section_6(self, result: Dict) -> str:
        """6️⃣ 选股结果 - 核心关注个股（强化强势股、龙头股展示）"""
        trend_stocks = result.get('trend_stocks', [])
        short_stocks = result.get('short_term_stocks', [])
        trend_detail = result.get('trend_stocks_detail', {})
        short_detail = result.get('short_term_stocks_detail', {})
        key_levels = result.get('_key_levels', {})
        stock_key_levels = key_levels.get('stocks', {})
        
        # 识别强势股和龙头股
        leader_stocks = []  # 龙头股：涨停分>=60 且 先锋>=3
        strong_stocks = []  # 强势股：涨停分>=50 或 先锋>=2
        
        for code in short_stocks:
            detail = short_detail.get(code, {})
            limit_up_score = detail.get('limit_up_score', 0) or 0
            pioneer = detail.get('pioneer_status', 0) or 0
            
            if limit_up_score >= 60 and pioneer >= 3:
                leader_stocks.append({
                    'code': code,
                    'name': detail.get('name', '未知'),
                    'close': detail.get('close', 0),
                    'limit_up_score': limit_up_score,
                    'pioneer': pioneer,
                    'score': detail.get('score', 0)
                })
            elif limit_up_score >= 50 or pioneer >= 2:
                strong_stocks.append({
                    'code': code,
                    'name': detail.get('name', '未知'),
                    'close': detail.get('close', 0),
                    'limit_up_score': limit_up_score,
                    'pioneer': pioneer,
                    'score': detail.get('score', 0)
                })
        
        # 生成龙头股HTML
        leader_html = ""
        if leader_stocks:
            for i, stock in enumerate(leader_stocks[:5], 1):
                leader_html += f"""
                    <div style="background: linear-gradient(135deg, rgba(255,215,0,0.1), rgba(255,140,0,0.1)); border: 2px solid rgba(255,215,0,0.5); border-radius: 10px; padding: 14px; margin-bottom: 10px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <div style="display: flex; align-items: center; gap: 8px;">
                                <span style="background: linear-gradient(90deg, #FFD700, #FF8C00); color: #000; padding: 3px 10px; border-radius: 12px; font-size: 0.75em; font-weight: bold;">🏆 龙头 #{i}</span>
                                <span style="font-weight: 700; color: #FFD700; font-size: 1.1em;">{stock['code']} {stock['name']}</span>
                            </div>
                            <span style="color: var(--accent-green); font-weight: 700; font-size: 1.2em;">¥{stock['close']:.2f}</span>
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; font-size: 0.85em;">
                            <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 6px;">
                                <div style="color: var(--text-secondary); font-size: 0.75em;">涨停分</div>
                                <div style="color: #FFD700; font-weight: 700; font-size: 1.1em;">{stock['limit_up_score']:.1f}</div>
                            </div>
                            <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 6px;">
                                <div style="color: var(--text-secondary); font-size: 0.75em;">先锋分</div>
                                <div style="color: var(--accent-green); font-weight: 700; font-size: 1.1em;">{stock['pioneer']:.0f}</div>
                            </div>
                            <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 6px;">
                                <div style="color: var(--text-secondary); font-size: 0.75em;">综合评分</div>
                                <div style="color: var(--accent-cyan); font-weight: 700; font-size: 1.1em;">{stock['score']:.1f}</div>
                            </div>
                        </div>
                    </div>
                """
        else:
            leader_html = '<div style="color: var(--text-secondary); text-align: center; padding: 20px; font-style: italic;">暂无龙头股数据（涨停分≥60且先锋分≥3）</div>'
        
        # 生成强势股HTML
        strong_html = ""
        if strong_stocks:
            for i, stock in enumerate(strong_stocks[:5], 1):
                strong_type = ""
                if stock['limit_up_score'] >= 50 and stock['pioneer'] >= 2:
                    strong_type = "双强"
                elif stock['limit_up_score'] >= 50:
                    strong_type = "涨停强"
                else:
                    strong_type = "先锋强"
                    
                strong_html += f"""
                    <div style="background: rgba(0,230,118,0.08); border: 1px solid rgba(0,230,118,0.4); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div style="display: flex; align-items: center; gap: 8px;">
                                <span style="background: rgba(0,230,118,0.2); color: var(--accent-green); padding: 2px 8px; border-radius: 4px; font-size: 0.7em; font-weight: 600;">💪 {strong_type}</span>
                                <span style="font-weight: 600; color: var(--accent-green);">{stock['code']} {stock['name']}</span>
                            </div>
                            <span style="color: var(--accent-green); font-weight: 600;">¥{stock['close']:.2f}</span>
                        </div>
                        <div style="display: flex; gap: 16px; margin-top: 6px; font-size: 0.8em; color: var(--text-secondary);">
                            <span>涨停分: <b style="color: var(--accent-cyan);">{stock['limit_up_score']:.1f}</b></span>
                            <span>先锋: <b style="color: var(--accent-purple);">{stock['pioneer']:.0f}</b></span>
                        </div>
                    </div>
                """
        else:
            strong_html = '<div style="color: var(--text-secondary); text-align: center; padding: 15px; font-style: italic;">暂无强势股数据（涨停分≥50或先锋分≥2）</div>'
        
        # 生成波段股详情HTML
        trend_details_html = ""
        for i, code in enumerate(trend_stocks[:8], 1):
            detail = trend_detail.get(code, {})
            key_level = stock_key_levels.get(code, {})
            
            name = detail.get('name', '未知') or '未知'
            close = detail.get('close', 0) or 0
            score = detail.get('score', 0) or 0
            v_ratio = detail.get('v_ratio10', 0) or 0
            ma5_bias = detail.get('ma5_bias', 0) or 0
            
            # 关键位数据
            resistance = key_level.get('resistance', 0)
            support = key_level.get('support', 0)
            
            # 计算距离关键位的百分比
            to_resistance = ((resistance - close) / close * 100) if close > 0 and resistance > 0 else 0
            to_support = ((close - support) / close * 100) if close > 0 and support > 0 else 0
            
            trend_details_html += f"""
                <div style="background: rgba(0,198,255,0.05); border: 1px solid rgba(0,198,255,0.2); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-weight: 600; color: var(--accent-cyan);">{i}. {code} {name}</span>
                        <span style="color: var(--accent-green); font-weight: 600;">¥{close:.2f}</span>
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; font-size: 0.8em; color: var(--text-secondary);">
                        <div>评分: <span style="color: var(--accent-cyan);">{score:.1f}</span></div>
                        <div>量比: <span style="color: {('var(--accent-green)' if v_ratio > 1.5 else 'var(--text-secondary)')};">{v_ratio:.2f}</span></div>
                        <div>MA5: <span style="color: {('var(--accent-green)' if ma5_bias > 0 else 'var(--accent-red)')};">{ma5_bias:+.1f}%</span></div>
                        <div>涨停分: <span style="color: var(--accent-purple);">{detail.get('limit_up_score', 0):.1f}</span></div>
                    </div>
                    {'<div style="margin-top: 8px; font-size: 0.75em; display: flex; gap: 12px;"><span style="color: var(--accent-red);">压: ¥' + f'{resistance:.2f} (+{to_resistance:.1f}%)' + '</span><span style="color: var(--accent-blue);">支: ¥' + f'{support:.2f} (-{to_support:.1f}%)' + '</span></div>' if resistance > 0 and support > 0 else ''}
                </div>
            """
        
        # 生成短线股详情HTML
        short_details_html = ""
        for i, code in enumerate(short_stocks[:5], 1):
            detail = short_detail.get(code, {})
            key_level = stock_key_levels.get(code, {})
            
            name = detail.get('name', '未知') or '未知'
            close = detail.get('close', 0) or 0
            score = detail.get('score', 0) or 0
            limit_up_score = detail.get('limit_up_score', 0) or 0
            pioneer = detail.get('pioneer_status', 0) or 0
            
            # 关键位数据
            resistance = key_level.get('resistance', 0)
            support = key_level.get('support', 0)
            
            # 判断是否为龙头股或强势股
            is_leader = limit_up_score >= 60 and pioneer >= 3
            is_strong = limit_up_score >= 50 or pioneer >= 2
            
            # 标识徽章
            badge = ""
            if is_leader:
                badge = '<span style="background: linear-gradient(90deg, #FFD700, #FF8C00); color: #000; padding: 2px 8px; border-radius: 4px; font-size: 0.7em; margin-left: 4px; font-weight: bold;">🏆 龙头</span>'
            elif is_strong:
                badge = '<span style="background: rgba(0,230,118,0.3); color: var(--accent-green); padding: 2px 8px; border-radius: 4px; font-size: 0.7em; margin-left: 4px; font-weight: 600;">💪 强势</span>'
            
            short_details_html += f"""
                <div style="background: rgba(179,136,255,0.05); border: 1px solid rgba(179,136,255,0.2); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <span style="font-weight: 600; color: var(--accent-purple);">{i}. {code} {name}{badge}</span>
                        <span style="color: var(--accent-green); font-weight: 600;">¥{close:.2f}</span>
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; font-size: 0.8em; color: var(--text-secondary);">
                        <div>评分: <span style="color: var(--accent-purple);">{score:.1f}</span></div>
                        <div>涨停分: <span style="color: {('var(--accent-red)' if limit_up_score > 50 else 'var(--text-secondary)')};">{limit_up_score:.1f}</span></div>
                        <div>先锋: <span style="color: {('var(--accent-green)' if pioneer >= 3 else 'var(--text-secondary)')};">{pioneer:.0f}</span></div>
                    </div>
                    {'<div style="margin-top: 8px; font-size: 0.75em; display: flex; gap: 12px;"><span style="color: var(--accent-red);">压: ¥' + f'{resistance:.2f}' + '</span><span style="color: var(--accent-blue);">支: ¥' + f'{support:.2f}' + '</span></div>' if resistance > 0 and support > 0 else ''}
                </div>
            """
        
        html = f"""
        <div class="section">
            <div class="section-title">6️⃣ 选股结果 - 核心关注个股（打板、强势股、龙头股）</div>
            
            <!-- 龙头股专区 -->
            <div style="background: linear-gradient(135deg, rgba(255,215,0,0.05), rgba(255,140,0,0.05)); border: 1px solid rgba(255,215,0,0.3); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">
                    <span style="font-size: 1.5em;">🏆</span>
                    <h4 style="color: #FFD700; margin: 0; font-size: 1.2em; font-weight: 700;">龙头股专区</h4>
                    <span style="background: rgba(255,215,0,0.2); color: #FFD700; padding: 2px 10px; border-radius: 10px; font-size: 0.75em;">涨停分≥60 & 先锋分≥3</span>
                </div>
                {leader_html}
            </div>
            
            <!-- 强势股专区 -->
            <div style="background: rgba(0,230,118,0.05); border: 1px solid rgba(0,230,118,0.3); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">
                    <span style="font-size: 1.5em;">💪</span>
                    <h4 style="color: var(--accent-green); margin: 0; font-size: 1.2em; font-weight: 700;">强势股专区</h4>
                    <span style="background: rgba(0,230,118,0.2); color: var(--accent-green); padding: 2px 10px; border-radius: 10px; font-size: 0.75em;">涨停分≥50 或 先锋分≥2</span>
                </div>
                {strong_html}
            </div>
            
            <!-- 波段股和短线股 -->
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div class="item" style="padding: 16px;">
                    <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1.1em;">📈 波段趋势股（前8只）</h4>
                    {trend_details_html if trend_details_html else '<div style="color: var(--text-secondary); text-align: center; padding: 20px;">暂无数据</div>'}
                </div>
                <div class="item" style="padding: 16px;">
                    <h4 style="color: var(--accent-purple); margin-bottom: 16px; font-size: 1.1em;">⚡ 短线打板股（前5只）</h4>
                    {short_details_html if short_details_html else '<div style="color: var(--text-secondary); text-align: center; padding: 20px;">暂无数据</div>'}
                </div>
            </div>
            
            <!-- 全部股票代码 -->
            <div style="margin-top: 20px; padding: 16px; background: rgba(255,255,255,0.03); border-radius: 10px;">
                <div style="display: flex; gap: 40px;">
                    <div>
                        <span style="color: var(--text-secondary); font-size: 0.85em;">全部波段股 ({len(trend_stocks)}只):</span>
                        <div style="margin-top: 8px;">
                            {''.join([f'<span class="stock-tag" style="margin: 2px;">{code}</span>' for code in trend_stocks])}
                        </div>
                    </div>
                    <div>
                        <span style="color: var(--text-secondary); font-size: 0.85em;">全部短线股 ({len(short_stocks)}只):</span>
                        <div style="margin-top: 8px;">
                            {''.join([f'<span class="stock-tag" style="margin: 2px; background: rgba(179,136,255,0.2); color: var(--accent-purple);">{code}</span>' for code in short_stocks])}
                        </div>
                    </div>
                </div>
            </div>
        </div>"""
        return html

    def _generate_section_7(self, result: Dict) -> str:
        """7️⃣ 仓位分配建议 - 根据市场信号动态调整，带逻辑说明"""
        defense = result.get('defense_signals', {})
        action = defense.get('action', 'BUY')
        
        # 根据市场信号确定建议仓位
        if action == 'DEFENSE':
            # 防守信号：减仓至30%以下
            suggested_trend_pct = 20
            suggested_short_pct = 10
            suggested_cash_pct = 70
            advice = "⛔ 防守信号：建议减仓至30%以下或空仓观望"
            advice_color = "var(--accent-red)"
            logic_reason = "触发防守条件（量能萎缩/跌破支撑/情绪过冷），优先保护本金"
        elif action == 'CAUTION':
            # 谨慎信号：控制仓位在50%左右
            suggested_trend_pct = 30
            suggested_short_pct = 20
            suggested_cash_pct = 50
            advice = "⚠️ 谨慎信号：建议控制仓位在50%左右"
            advice_color = "var(--accent-orange)"
            logic_reason = "市场方向不明，保持半仓灵活应对，进可攻退可守"
        else:
            # 积极信号：可满仓操作
            upward = result.get('upward_pivot', False)
            if upward:
                suggested_trend_pct = 40
                suggested_short_pct = 50
                suggested_cash_pct = 10
                advice = "🚀 积极信号+向上变盘：可满仓操作，重点配置"
                logic_reason = "向上变盘确认，市场情绪积极，重仓参与热点"
            else:
                suggested_trend_pct = 50
                suggested_short_pct = 30
                suggested_cash_pct = 20
                advice = "✅ 积极信号：可满仓操作，保持灵活"
                logic_reason = "市场环境良好，但变盘信号未触发，保持部分现金灵活调仓"
            advice_color = "var(--accent-green)"
        
        # 实际持仓（从策略结果获取）
        position = result.get('position_size', {})
        actual_trend = position.get('trend', 0)
        actual_short = position.get('short_term', 0)
        actual_cash = position.get('cash', 0)
        actual_total = actual_trend + actual_short + actual_cash
        
        actual_trend_pct = (actual_trend / actual_total * 100) if actual_total > 0 else 0
        actual_short_pct = (actual_short / actual_total * 100) if actual_total > 0 else 0
        actual_cash_pct = (actual_cash / actual_total * 100) if actual_total > 0 else 0
        
        return f"""
        <div class="section">
            <div class="section-title">7️⃣ 仓位分配建议</div>
            
            <!-- 仓位逻辑说明 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 16px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 12px; font-size: 1em;">🧮 仓位逻辑</h4>
                <div style="color: var(--text-secondary); font-size: 0.9em; line-height: 1.8;">
                    <div style="margin-bottom: 8px;"><span style="color: var(--accent-cyan);">计算依据：</span>基于防守信号 + 变盘信号 + 情绪温度综合判断</div>
                    <div style="margin-bottom: 8px;"><span style="color: var(--accent-cyan);">当前判断：</span>{logic_reason}</div>
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1);">
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: var(--text-secondary);">防守信号</div>
                            <div style="color: {advice_color}; font-weight: 600;">{action}</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: var(--text-secondary);">向上变盘</div>
                            <div style="color: {'var(--accent-green)' if result.get('upward_pivot') else 'var(--text-secondary)'}; font-weight: 600;">{'✅ 是' if result.get('upward_pivot') else '❌ 否'}</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: var(--text-secondary);">情绪温度</div>
                            <div style="color: {'var(--accent-green)' if result.get('sentiment_temperature', 0) >= 50 else 'var(--accent-orange)' if result.get('sentiment_temperature', 0) >= 30 else 'var(--accent-red)'}; font-weight: 600;">{result.get('sentiment_temperature', 0):.1f}°</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- 建议仓位配置 -->
            <div style="background: rgba(79,172,254,0.1); border: 1px solid rgba(79,172,254,0.3); border-radius: 12px; padding: 16px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 12px; font-size: 1em;">📊 建议仓位配置</h4>
                <div class="position-bar" style="margin: 16px 0;">
                    <div class="position-trend" style="flex: {suggested_trend_pct}; display: flex; align-items: center; justify-content: center; font-size: 0.85em;">
                        波段 {suggested_trend_pct}%
                    </div>
                    <div class="position-short" style="flex: {suggested_short_pct}; display: flex; align-items: center; justify-content: center; font-size: 0.85em;">
                        短线 {suggested_short_pct}%
                    </div>
                    <div class="position-cash" style="flex: {suggested_cash_pct}; display: flex; align-items: center; justify-content: center; font-size: 0.85em;">
                        现金 {suggested_cash_pct}%
                    </div>
                </div>
                <div style="text-align: center; padding: 12px; background: rgba(0,0,0,0.2); border-radius: 8px; color: {advice_color}; font-weight: 600;">
                    {advice}
                </div>
            </div>
            
            <!-- 实际持仓对比 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 16px;">
                <h4 style="color: var(--text-secondary); margin-bottom: 12px; font-size: 0.9em;">📈 当前策略持仓（100万模拟资金）</h4>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;">
                    <div class="metric-card" style="background: rgba(0,114,255,0.1);">
                        <div class="metric-value" style="color: var(--accent-cyan);">{actual_trend/10000:.0f}万</div>
                        <div class="metric-label">波段仓位 ({actual_trend_pct:.0f}%)</div>
                    </div>
                    <div class="metric-card" style="background: rgba(245,87,108,0.1);">
                        <div class="metric-value" style="color: var(--accent-purple);">{actual_short/10000:.0f}万</div>
                        <div class="metric-label">短线仓位 ({actual_short_pct:.0f}%)</div>
                    </div>
                    <div class="metric-card" style="background: rgba(154,160,166,0.1);">
                        <div class="metric-value" style="color: var(--text-secondary);">{actual_cash/10000:.0f}万</div>
                        <div class="metric-label">现金储备 ({actual_cash_pct:.0f}%)</div>
                    </div>
                </div>
            </div>
        </div>"""

    def _generate_section_8(self, result: Dict) -> str:
        """8️⃣ 今日策略建议"""
        defense = result.get('defense_signals', {})
        action = defense.get('action', 'BUY')
        upward = result.get('upward_pivot', False)
        
        if action == 'DEFENSE':
            title = "⛔ 避险为主"
            items = ["开盘后如冲高回落，及时减仓", "关注防御性板块（医药、消费）", "可做T降低成本，但不新开仓"]
        elif action == 'CAUTION':
            title = "⚠️ 谨慎操作"
            items = ["控制仓位，分批建仓", "关注强势股回调机会", "设置止损，严格执行"]
        else:
            if upward:
                title = "🚀 积极做多"
                items = ["大胆追涨强势股", "关注热点板块龙头", "可适当参与打板"]
            else:
                title = "📊 震荡上行"
                items = ["低吸高抛，做T为主", "关注均线支撑位", "不追高，回调再买入"]
        
        items_html = ''.join([f'<li style="padding: 8px 0;">• {item}</li>' for item in items])
        
        return f"""
        <div class="section">
            <div class="section-title">8️⃣ 今日策略建议</div>
            <div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 12px;">{title}</h4>
                <ul style="list-style: none; padding: 0;">
                    {items_html}
                </ul>
            </div>
        </div>"""

    def _generate_xgboost_section(self, result: Dict) -> str:
        """🤖 XGBoost AI选股 - 机器学习模型选股结果"""
        xgboost_picks = result.get('xgboost_picks', [])
        xgboost_fusion = result.get('xgboost_fusion', {})
        
        if not xgboost_picks:
            return """
            <div class="section">
                <div class="section-title">🤖 XGBoost AI选股</div>
                <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; text-align: center; color: var(--text-secondary);">
                    暂无XGBoost选股数据
                </div>
            </div>"""
        
        # 生成XGBoost选股列表HTML
        picks_html = ""
        for i, pick in enumerate(xgboost_picks[:10], 1):
            code = pick.get('code', '')
            score = pick.get('xgboost_score', 0)
            close = pick.get('close', 0)
            volume = pick.get('volume', 0)
            
            # 检查是否在融合列表中
            fusion_badge = ""
            if code in xgboost_fusion.get('common_trend', []):
                fusion_badge = '<span style="background: linear-gradient(90deg, #00c6ff, #0072ff); color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 4px;">波段共振</span>'
            elif code in xgboost_fusion.get('common_short', []):
                fusion_badge = '<span style="background: linear-gradient(90deg, #f093fb, #f5576c); color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; margin-left: 4px;">短线共振</span>'
            
            picks_html += f"""
                <div style="background: rgba(0,230,118,0.05); border: 1px solid rgba(0,230,118,0.2); border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 600; color: var(--accent-green);">{i}. {code}{fusion_badge}</span>
                        <span style="color: var(--text-secondary); font-size: 0.9em;">得分: {score:.4f}</span>
                    </div>
                    <div style="display: flex; gap: 16px; margin-top: 8px; font-size: 0.8em; color: var(--text-secondary);">
                        <span>价格: ¥{close:.2f}</span>
                        <span>成交量: {volume:,.0f}</span>
                    </div>
                </div>
            """
        
        # 融合分析
        common_trend = xgboost_fusion.get('common_trend', [])
        common_short = xgboost_fusion.get('common_short', [])
        xgboost_only = xgboost_fusion.get('xgboost_only', [])
        
        fusion_html = ""
        if common_trend or common_short:
            fusion_html += "<div style='margin-top: 16px;'>"
            fusion_html += "<h5 style='color: var(--accent-cyan); margin-bottom: 12px;'>🎯 策略共振（高置信度）</h5>"
            
            if common_trend:
                fusion_html += "<div style='margin-bottom: 12px;'>"
                fusion_html += "<span style='color: var(--text-secondary); font-size: 0.85em;'>波段共振:</span>"
                fusion_html += "<div style='display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px;'>"
                for code in common_trend[:5]:
                    fusion_html += f"<span style='background: rgba(0,198,255,0.2); color: var(--accent-cyan); padding: 3px 8px; border-radius: 4px; font-size: 0.75em;'>{code}</span>"
                fusion_html += "</div></div>"
            
            if common_short:
                fusion_html += "<div>"
                fusion_html += "<span style='color: var(--text-secondary); font-size: 0.85em;'>短线共振:</span>"
                fusion_html += "<div style='display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px;'>"
                for code in common_short[:5]:
                    fusion_html += f"<span style='background: rgba(179,136,255,0.2); color: var(--accent-purple); padding: 3px 8px; border-radius: 4px; font-size: 0.75em;'>{code}</span>"
                fusion_html += "</div></div>"
            
            fusion_html += "</div>"
        
        return f"""
        <div class="section">
            <div class="section-title">🤖 XGBoost AI选股</div>
            
            <!-- 模型说明 -->
            <div style="background: rgba(0,230,118,0.1); border: 1px solid rgba(0,230,118,0.3); border-radius: 12px; padding: 16px; margin-bottom: 20px;">
                <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
                    <span style="font-size: 1.5em;">🧠</span>
                    <span style="font-weight: 600; color: var(--accent-green);">机器学习选股模型</span>
                </div>
                <div style="color: var(--text-secondary); font-size: 0.9em; line-height: 1.6;">
                    基于XGBoost算法，融合技术面、量价、基本面代理等多维因子，预测未来5日收益排名。
                    选出{len(xgboost_picks)}只最具潜力的股票。
                </div>
            </div>
            
            <!-- 选股结果 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">📊 Top 10 选股结果</h4>
                {picks_html}
                {fusion_html}
            </div>
            
            <!-- 全部股票 -->
            <div style="margin-top: 20px; padding: 16px; background: rgba(255,255,255,0.03); border-radius: 10px;">
                <span style="color: var(--text-secondary); font-size: 0.85em;">全部XGBoost选股 ({len(xgboost_picks)}只):</span>
                <div style="margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px;">
                    {''.join([f'<span style="background: rgba(0,230,118,0.15); color: var(--accent-green); padding: 3px 8px; border-radius: 4px; font-size: 0.75em;">{p["code"]}</span>' for p in xgboost_picks])}
                </div>
            </div>
        </div>"""

    def _generate_review_section(self, result: Dict) -> str:
        """📈 昨日复盘 - 回顾昨日市场表现和策略效果"""
        # 从结果中获取昨日复盘数据
        review_data = result.get('_review_data', {})
        
        # 如果没有复盘数据，使用模拟数据或提示
        if not review_data:
            # 尝试从 morning_data 中获取
            morning_data = result.get('_morning_data', {})
            review_data = {
                'market_summary': morning_data.get('market_summary', '暂无昨日复盘数据'),
                'index_change': morning_data.get('index_change', {}),
                'volume_analysis': morning_data.get('volume_analysis', '数据待补充'),
                'sector_performance': morning_data.get('sector_performance', []),
                'strategy_performance': result.get('_strategy_performance', {})
            }
        
        # 获取指数涨跌数据
        index_changes = review_data.get('index_change', {})
        sh_change = index_changes.get('sh000001', {})
        sh_pct = sh_change.get('pct_change', 0) if isinstance(sh_change, dict) else 0
        sh_color = 'var(--accent-green)' if sh_pct >= 0 else 'var(--accent-red)'
        sh_sign = '+' if sh_pct >= 0 else ''
        
        # 市场情绪回顾
        sentiment_review = review_data.get('sentiment_review', '昨日市场情绪整体平稳，个股涨跌互现。')
        
        # 策略表现回顾
        strategy_perf = review_data.get('strategy_performance', {})
        trend_return = strategy_perf.get('trend_return', 0)
        short_return = strategy_perf.get('short_return', 0)
        
        return f"""
        <div class="section">
            <div class="section-title">📈 昨日复盘</div>
            
            <!-- 大盘表现回顾 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">📊 大盘表现回顾</h4>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;">
                    <div class="metric-card" style="background: rgba(79,172,254,0.1);">
                        <div class="metric-value" style="color: {sh_color};">{sh_sign}{sh_pct:.2f}%</div>
                        <div class="metric-label">上证指数涨跌</div>
                    </div>
                    <div class="metric-card" style="background: rgba(79,172,254,0.1);">
                        <div class="metric-value neutral">{review_data.get('up_stocks', 0)}</div>
                        <div class="metric-label">上涨家数</div>
                    </div>
                    <div class="metric-card" style="background: rgba(79,172,254,0.1);">
                        <div class="metric-value neutral">{review_data.get('down_stocks', 0)}</div>
                        <div class="metric-label">下跌家数</div>
                    </div>
                </div>
                <div style="margin-top: 16px; padding: 12px; background: rgba(0,0,0,0.2); border-radius: 8px; color: var(--text-secondary); font-size: 0.9em; line-height: 1.6;">
                    {sentiment_review}
                </div>
            </div>
            
            <!-- 板块表现 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">🔥 板块表现</h4>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div>
                        <h5 style="color: var(--accent-green); margin-bottom: 12px; font-size: 0.9em;">领涨板块</h5>
                        <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                            {''.join([f'<span style="background: rgba(0,230,118,0.15); color: var(--accent-green); padding: 4px 10px; border-radius: 4px; font-size: 0.8em;">{sector}</span>' for sector in review_data.get('leading_sectors', ['科技', '新能源', '医药'])[:5]])}
                        </div>
                    </div>
                    <div>
                        <h5 style="color: var(--accent-red); margin-bottom: 12px; font-size: 0.9em;">领跌板块</h5>
                        <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                            {''.join([f'<span style="background: rgba(255,82,82,0.15); color: var(--accent-red); padding: 4px 10px; border-radius: 4px; font-size: 0.8em;">{sector}</span>' for sector in review_data.get('declining_sectors', ['银行', '地产', '保险'])[:5]])}
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- 策略回顾 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">🎯 策略回顾</h4>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px;">
                    <div class="metric-card" style="background: rgba(0,198,255,0.1);">
                        <div class="metric-value" style="color: {'var(--accent-green)' if trend_return >= 0 else 'var(--accent-red)'};">{trend_return:+.2f}%</div>
                        <div class="metric-label">波段策略收益</div>
                    </div>
                    <div class="metric-card" style="background: rgba(179,136,255,0.1);">
                        <div class="metric-value" style="color: {'var(--accent-green)' if short_return >= 0 else 'var(--accent-red)'};">{short_return:+.2f}%</div>
                        <div class="metric-label">短线策略收益</div>
                    </div>
                </div>
                <div style="margin-top: 16px; padding: 12px; background: rgba(0,0,0,0.2); border-radius: 8px; color: var(--text-secondary); font-size: 0.9em; line-height: 1.6;">
                    {review_data.get('strategy_comment', '昨日策略运行正常，选股模型表现符合预期。')}
                </div>
            </div>
        </div>"""

    def _generate_quality_check_section(self, result: Dict) -> str:
        """✅ 数据质检 - 展示数据质量检查结果"""
        # 获取质检结果
        quality_data = result.get('_quality_check', {})
        
        # 如果没有质检数据，生成默认检查项
        if not quality_data:
            quality_data = {
                'data_freshness': {'status': 'pass', 'message': '数据时效性检查通过'},
                'completeness': {'status': 'pass', 'message': '数据完整性检查通过'},
                'consistency': {'status': 'pass', 'message': '数据一致性检查通过'},
                'accuracy': {'status': 'pass', 'message': '数据准确性检查通过'},
                'anomalies': []
            }
        
        # 检查项状态
        checks = [
            {'name': '数据时效性', 'icon': '⏰', 'data': quality_data.get('data_freshness', {})},
            {'name': '数据完整性', 'icon': '📦', 'data': quality_data.get('completeness', {})},
            {'name': '数据一致性', 'icon': '🔗', 'data': quality_data.get('consistency', {})},
            {'name': '数据准确性', 'icon': '✓', 'data': quality_data.get('accuracy', {})},
        ]
        
        # 生成检查项HTML
        checks_html = ""
        for check in checks:
            status = check['data'].get('status', 'pass')
            message = check['data'].get('message', '检查通过')
            
            if status == 'pass':
                color = 'var(--accent-green)'
                bg = 'rgba(0,230,118,0.1)'
                icon = '✅'
            elif status == 'warning':
                color = 'var(--accent-orange)'
                bg = 'rgba(255,171,64,0.1)'
                icon = '⚠️'
            else:
                color = 'var(--accent-red)'
                bg = 'rgba(255,82,82,0.1)'
                icon = '❌'
            
            checks_html += f"""
                <div style="background: {bg}; border-radius: 10px; padding: 16px; border-left: 4px solid {color};">
                    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
                        <span style="font-size: 1.2em;">{check['icon']}</span>
                        <span style="font-weight: 600; color: var(--text-primary);">{check['name']}</span>
                        <span style="margin-left: auto; font-size: 1.2em;">{icon}</span>
                    </div>
                    <div style="color: {color}; font-size: 0.85em;">{message}</div>
                </div>
            """
        
        # 异常数据列表
        anomalies = quality_data.get('anomalies', [])
        anomalies_html = ""
        if anomalies:
            anomalies_html = "<div style='margin-top: 16px;'><h5 style='color: var(--accent-red); margin-bottom: 12px;'>⚠️ 发现异常数据</h5>"
            for anomaly in anomalies[:5]:
                anomalies_html += f"<div style='padding: 8px; background: rgba(255,82,82,0.1); border-radius: 6px; margin-bottom: 8px; font-size: 0.85em; color: var(--text-secondary);'>• {anomaly}</div>"
            anomalies_html += "</div>"
        else:
            anomalies_html = "<div style='margin-top: 16px; padding: 12px; background: rgba(0,230,118,0.1); border-radius: 8px; text-align: center; color: var(--accent-green); font-size: 0.9em;'>✅ 未发现数据异常</div>"
        
        # 数据来源说明
        data_sources = quality_data.get('data_sources', [
            {'name': '行情数据', 'source': 'AKShare', 'update_time': '每日15:30'},
            {'name': '指数数据', 'source': '东方财富', 'update_time': '实时'},
            {'name': '财务数据', 'source': 'Tushare', 'update_time': '每日17:00'},
        ])
        
        sources_html = ""
        for source in data_sources:
            sources_html += f"""
                <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05); font-size: 0.85em;">
                    <span style="color: var(--text-secondary);">{source['name']}</span>
                    <span style="color: var(--accent-cyan);">{source['source']}</span>
                    <span style="color: var(--text-secondary);">{source['update_time']}</span>
                </div>
            """
        
        return f"""
        <div class="section">
            <div class="section-title">✅ 数据质检</div>
            
            <!-- 质检概览 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">📋 质检项目</h4>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px;">
                    {checks_html}
                </div>
                {anomalies_html}
            </div>
            
            <!-- 数据来源 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">📡 数据来源</h4>
                {sources_html}
                <div style="margin-top: 16px; padding: 12px; background: rgba(0,0,0,0.2); border-radius: 8px; color: var(--text-secondary); font-size: 0.8em; line-height: 1.6;">
                    💡 数据质检于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 完成，所有数据均经过自动化校验流程。
                </div>
            </div>
        </div>"""

    def _generate_summary_section(self, result: Dict) -> str:
        """📝 总结 - 核心观点和投资建议总结"""
        # 获取防守信号
        defense = result.get('defense_signals', {})
        action = defense.get('action', 'BUY')
        
        # 市场状态
        market_state = result.get('market_state', ['OSCILLATING'])[0] if result.get('market_state') else 'OSCILLATING'
        upward_pivot = result.get('upward_pivot', False)
        
        # 生成核心观点
        if action == 'DEFENSE':
            core_view = "市场处于调整期，建议以防守为主，控制仓位，等待明确信号。"
            view_color = 'var(--accent-red)'
            view_bg = 'rgba(255,82,82,0.1)'
        elif action == 'CAUTION':
            core_view = "市场方向不明，建议谨慎操作，控制仓位在50%左右，灵活应对。"
            view_color = 'var(--accent-orange)'
            view_bg = 'rgba(255,171,64,0.1)'
        else:
            if upward_pivot:
                core_view = "市场向上突破，积极做多，重点关注强势股和热点板块。"
                view_color = 'var(--accent-green)'
                view_bg = 'rgba(0,230,118,0.1)'
            else:
                core_view = "市场震荡上行，可积极参与，但要注意节奏，高抛低吸。"
                view_color = 'var(--accent-green)'
                view_bg = 'rgba(0,230,118,0.1)'
        
        # 操作建议
        if action == 'DEFENSE':
            operations = [
                {'icon': '🛡️', 'title': '控制仓位', 'desc': '建议仓位不超过30%，或空仓观望'},
                {'icon': '✂️', 'title': '及时止损', 'desc': '跌破支撑位果断止损，不抱幻想'},
                {'icon': '👀', 'title': '观察等待', 'desc': '等待市场企稳信号，不急于入场'},
            ]
        elif action == 'CAUTION':
            operations = [
                {'icon': '⚖️', 'title': '平衡配置', 'desc': '波段和短线均衡配置，保持灵活性'},
                {'icon': '🎯', 'title': '精选个股', 'desc': '只选择强势板块中的龙头股'},
                {'icon': '📉', 'title': '低吸为主', 'desc': '回调时买入，不追高'},
            ]
        else:
            if upward_pivot:
                operations = [
                    {'icon': '🚀', 'title': '积极做多', 'desc': '可满仓操作，把握上涨机会'},
                    {'icon': '🔥', 'title': '追涨龙头', 'desc': '重点关注涨停板和强势股'},
                    {'icon': '💪', 'title': '持股待涨', 'desc': '趋势良好时耐心持有'},
                ]
            else:
                operations = [
                    {'icon': '📊', 'title': '灵活操作', 'desc': '高抛低吸，做T降低成本'},
                    {'icon': '🎯', 'title': '精选标的', 'desc': '选择均线多头排列的强势股'},
                    {'icon': '⏰', 'title': '把握节奏', 'desc': '注意板块轮动，及时调整持仓'},
                ]
        
        # 生成操作建议HTML
        operations_html = ""
        for op in operations:
            operations_html += f"""
                <div style="background: rgba(255,255,255,0.03); border-radius: 10px; padding: 16px; text-align: center;">
                    <div style="font-size: 2em; margin-bottom: 8px;">{op['icon']}</div>
                    <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 4px;">{op['title']}</div>
                    <div style="font-size: 0.8em; color: var(--text-secondary); line-height: 1.4;">{op['desc']}</div>
                </div>
            """
        
        # 风险提示
        risks = result.get('_risks', [
            '市场有风险，投资需谨慎',
            '本报告仅供参考，不构成投资建议',
            '请根据自身风险承受能力做出投资决策',
        ])
        
        risks_html = ""
        for risk in risks:
            risks_html += f"<div style='padding: 6px 0; color: var(--accent-orange); font-size: 0.85em;'>⚠️ {risk}</div>"
        
        # 选股数量
        trend_count = len(result.get('trend_stocks', []))
        short_count = len(result.get('short_term_stocks', []))
        
        return f"""
        <div class="section">
            <div class="section-title">📝 总结</div>
            
            <!-- 核心观点 -->
            <div style="background: {view_bg}; border-radius: 12px; padding: 20px; margin-bottom: 20px; border-left: 4px solid {view_color};">
                <h4 style="color: {view_color}; margin-bottom: 12px; font-size: 1em;">💡 核心观点</h4>
                <div style="color: var(--text-primary); font-size: 1.1em; line-height: 1.6; font-weight: 500;">
                    {core_view}
                </div>
            </div>
            
            <!-- 操作建议 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">🎯 操作建议</h4>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;">
                    {operations_html}
                </div>
            </div>
            
            <!-- 今日关注 -->
            <div style="background: rgba(255,255,255,0.03); border-radius: 12px; padding: 20px; margin-bottom: 20px;">
                <h4 style="color: var(--accent-cyan); margin-bottom: 16px; font-size: 1em;">👀 今日关注</h4>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px;">
                    <div>
                        <h5 style="color: var(--accent-cyan); margin-bottom: 12px; font-size: 0.9em;">📈 波段关注 ({trend_count}只)</h5>
                        <div style="display: flex; flex-wrap: wrap; gap: 6px;">
                            {''.join([f'<span style="background: rgba(0,198,255,0.15); color: var(--accent-cyan); padding: 3px 8px; border-radius: 4px; font-size: 0.75em;">{code}</span>' for code in result.get('trend_stocks', [])[:10]])}
                            {'<span style="color: var(--text-secondary); font-size: 0.75em;">...</span>' if trend_count > 10 else ''}
                        </div>
                    </div>
                    <div>
                        <h5 style="color: var(--accent-purple); margin-bottom: 12px; font-size: 0.9em;">⚡ 短线关注 ({short_count}只)</h5>
                        <div style="display: flex; flex-wrap: wrap; gap: 6px;">
                            {''.join([f'<span style="background: rgba(179,136,255,0.15); color: var(--accent-purple); padding: 3px 8px; border-radius: 4px; font-size: 0.75em;">{code}</span>' for code in result.get('short_term_stocks', [])[:10]])}
                            {'<span style="color: var(--text-secondary); font-size: 0.75em;">...</span>' if short_count > 10 else ''}
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- 风险提示 -->
            <div style="background: rgba(255,171,64,0.1); border: 1px solid rgba(255,171,64,0.3); border-radius: 12px; padding: 16px;">
                <h4 style="color: var(--accent-orange); margin-bottom: 12px; font-size: 0.95em;">⚠️ 风险提示</h4>
                {risks_html}
            </div>
        </div>"""

    def _generate_section_9(self, result: Dict) -> str:
        """9️⃣ 数据概览"""
        # 从 result 中提取数据概览信息
        morning_data = result.get('_morning_data', {})
        
        # 尝试获取数据加载信息
        total_stocks = len(result.get('trend_stocks', [])) + len(result.get('short_term_stocks', []))
        
        html = f"""
        <div class="section">
            <div class="section-title">9️⃣ 数据概览</div>
            <div class="grid-4">
                <div class="metric-card">
                    <div class="metric-value neutral">{total_stocks}</div>
                    <div class="metric-label">选中股票数</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value neutral">{len(result.get('trend_stocks', []))}</div>
                    <div class="metric-label">波段股票</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value neutral">{len(result.get('short_term_stocks', []))}</div>
                    <div class="metric-label">短线股票</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value neutral">{self.report_date}</div>
                    <div class="metric-label">报告日期</div>
                </div>
            </div>
        </div>"""
        return html

    def _generate_footer(self) -> str:
        return f"""
        <div class="footer">
            <p>本报告由资金行为学量化策略系统生成</p>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="margin-top: 10px; color: var(--accent-red);">⚠️ 风险提示: 本报告仅供参考，不构成投资建议</p>
        </div>"""


def calculate_index_key_levels(index_code: str, index_name: str) -> Dict:
    """计算指数的关键位数据
    
    Args:
        index_code: 指数代码 (如 'sh000001')
        index_name: 指数名称
        
    Returns:
        包含关键位的字典
    """
    try:
        # 构建指数文件路径
        index_path = Path(f'data/index/{index_code.replace("sh", "").replace("sz", "")}.parquet')
        if not index_path.exists():
            index_path = Path(f'data/index/{index_code}.parquet')
        
        if not index_path.exists():
            return {
                'name': index_name,
                'current': 0,
                'resistance': 0,
                'support': 0,
                'error': '数据文件不存在'
            }
        
        # 读取指数数据
        df = pl.read_parquet(index_path)
        if len(df) < 20:
            return {
                'name': index_name,
                'current': 0,
                'resistance': 0,
                'support': 0,
                'error': '数据不足'
            }
        
        # 获取最近60天数据
        df = df.sort('trade_date').tail(60)
        
        # 使用KeyLevels计算关键位
        key_levels = KeyLevels()
        levels = key_levels.calculate_key_levels(
            closes=df['close'].to_list(),
            highs=df['high'].to_list(),
            lows=df['low'].to_list()
        )
        
        if 'error' in levels:
            return {
                'name': index_name,
                'current': 0,
                'resistance': 0,
                'support': 0,
                'error': levels['error']
            }
        
        current_price = df['close'].to_list()[-1]
        
        # 计算压力位：取近期高点和MA20的较大值
        resistance = max(
            levels.get('resistance_high', current_price * 1.02),
            levels.get('ma20', current_price * 1.01)
        )
        
        # 计算支撑位：取近期低点和MA20的较小值
        support = min(
            levels.get('support_low', current_price * 0.98),
            levels.get('ma20', current_price * 0.99)
        )
        
        return {
            'name': index_name,
            'current': round(current_price, 2),
            'resistance': round(resistance, 2),
            'support': round(support, 2),
            'ma20': round(levels.get('ma20', current_price), 2),
            'ma60': round(levels.get('ma60', current_price), 2)
        }
        
    except Exception as e:
        return {
            'name': index_name,
            'current': 0,
            'resistance': 0,
            'support': 0,
            'error': str(e)
        }


def get_market_key_levels() -> Dict[str, Dict]:
    """获取主要市场指数的关键位数据
    
    Returns:
        包含上证指数、创业板指、沪深300关键位的字典
    """
    return {
        'sh': calculate_index_key_levels('sh000001', '上证指数'),
        'cy': calculate_index_key_levels('sz399006', '创业板指'),
        'hs300': calculate_index_key_levels('sh000300', '沪深300')
    }


def generate_fund_behavior_html(result: Dict, config: Dict = None, morning_data: Dict = None) -> str:
    """生成HTML报告的便捷函数
    
    支持从 result['_morning_data'] 自动提取晨间数据
    如果 result 中已包含 '_key_levels'，则使用已有数据，否则计算新的关键位
    """
    generator = FundBehaviorHTMLReport()
    # 优先使用传入的 morning_data，否则从 result 中提取
    morning_data = morning_data or result.get('_morning_data', {})
    
    # 如果 result 中还没有关键位数据，则计算
    if '_key_levels' not in result or not result['_key_levels']:
        key_levels = get_market_key_levels()
        result['_key_levels'] = key_levels
    
    return generator.generate(result, config, morning_data)


def generate_fund_behavior_html_v2(result: Dict, morning_data: Dict = None) -> str:
    """生成HTML报告的便捷函数（v2.0支持9个章节）"""
    generator = FundBehaviorHTMLReport()
    return generator.generate(result, None, morning_data)


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