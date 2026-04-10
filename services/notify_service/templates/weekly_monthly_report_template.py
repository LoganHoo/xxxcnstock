"""
资金行为学周度/月度报告HTML模板
"""
from typing import Dict, List, Optional
from datetime import datetime
import json


class WeeklyMonthlyHTMLReport:
    """周度/月度报告HTML生成器"""

    def __init__(self):
        self.report_date = datetime.now().strftime('%Y-%m-%d')

    def generate_weekly_report(self, weekly_data: Dict, daily_reports: List[Dict] = None) -> str:
        """生成周报HTML"""
        week_start = weekly_data.get('week_start', '')
        week_end = weekly_data.get('week_end', '')
        avg_v_total = weekly_data.get('avg_v_total', 0)
        avg_sentiment = weekly_data.get('avg_sentiment', 0)
        sentiment_trend = weekly_data.get('sentiment_trend', 'stable')
        state_dist = json.loads(weekly_data.get('market_state_distribution', '{}'))
        strong_days = weekly_data.get('strong_days', 0)
        oscillating_days = weekly_data.get('oscillating_days', 0)
        weak_days = weekly_data.get('weak_days', 0)
        upward_pivot_days = weekly_data.get('upward_pivot_days', 0)
        hedge_effect_days = weekly_data.get('hedge_effect_days', 0)
        total_position_avg = weekly_data.get('total_position_avg', 0)
        top_trend_stocks = json.loads(weekly_data.get('top_trend_stocks', '[]'))
        top_short_stocks = json.loads(weekly_data.get('top_short_stocks', '[]'))
        weekly_summary = weekly_data.get('weekly_summary', '')

        trend_icon = '📈' if sentiment_trend == 'rising' else '📉' if sentiment_trend == 'falling' else '➡️'
        sentiment_color = '#00e676' if sentiment_trend == 'rising' else '#ff5252' if sentiment_trend == 'falling' else '#ffab40'

        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>周度报告 - {week_start} ~ {week_end}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
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
            --border-color: #2d3562;
        }}
        body {{
            font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{
            text-align: center;
            padding: 40px 20px;
            margin-bottom: 30px;
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-card));
            border-radius: 20px;
            border: 1px solid var(--border-color);
        }}
        .header h1 {{
            font-size: 2.2em;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-green), var(--accent-cyan));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .header .period {{
            color: var(--text-secondary);
            font-size: 1.2em;
            margin-top: 10px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            text-align: center;
            border: 1px solid var(--border-color);
        }}
        .metric-value {{
            font-size: 2em;
            font-weight: 700;
            color: var(--accent-blue);
        }}
        .metric-label {{
            color: var(--text-secondary);
            margin-top: 8px;
        }}
        .state-distribution {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 30px;
            border: 1px solid var(--border-color);
        }}
        .state-bar {{
            display: flex;
            height: 40px;
            border-radius: 20px;
            overflow: hidden;
            margin: 20px 0;
        }}
        .state-strong {{ background: linear-gradient(90deg, #00c6ff, #0072ff); }}
        .state-oscillating {{ background: linear-gradient(90deg, #f093fb, #f5576c); }}
        .state-weak {{ background: linear-gradient(90deg, #667eea, #764ba2); }}
        .stock-section {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stock-card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
        }}
        .stock-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 16px;
        }}
        .stock-tag {{
            background: rgba(79, 172, 254, 0.15);
            color: var(--accent-cyan);
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.9em;
        }}
        .summary-section {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
        }}
        .summary-text {{
            line-height: 1.8;
            color: var(--text-secondary);
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: var(--text-secondary);
            font-size: 0.85em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 周度量化报告</h1>
            <p class="period">{week_start} ~ {week_end}</p>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{avg_v_total/10000:.2f}万亿</div>
                <div class="metric-label">周均成交额</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: {sentiment_color};">{avg_sentiment:.1f}°</div>
                <div class="metric-label">{trend_icon} 情绪趋势</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: var(--accent-green);">{upward_pivot_days}天</div>
                <div class="metric-label">向上变盘</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: var(--accent-orange);">{hedge_effect_days}天</div>
                <div class="metric-label">对冲有效</div>
            </div>
        </div>

        <div class="state-distribution">
            <h3 style="margin-bottom: 16px;">📈 市场状态分布</h3>
            <div class="state-bar">
                <div class="state-strong" style="flex: {strong_days};"></div>
                <div class="state-oscillating" style="flex: {oscillating_days};"></div>
                <div class="state-weak" style="flex: {weak_days};"></div>
            </div>
            <div style="display: flex; gap: 24px; justify-content: center;">
                <span><span style="color: #00c6ff;">●</span> 强势 {strong_days}天</span>
                <span><span style="color: #f5576c;">●</span> 震荡 {oscillating_days}天</span>
                <span><span style="color: #b388ff;">●</span> 弱势 {weak_days}天</span>
            </div>
        </div>

        <div class="stock-section">
            <div class="stock-card">
                <h3>📈 波段高频股票 TOP10</h3>
                <div class="stock-list">
                    {''.join([f'<span class="stock-tag">{s}</span>' for s in top_trend_stocks])}
                </div>
            </div>
            <div class="stock-card">
                <h3>⚡ 短线高频股票 TOP10</h3>
                <div class="stock-list">
                    {''.join([f'<span class="stock-tag">{s}</span>' for s in top_short_stocks])}
                </div>
            </div>
        </div>

        <div class="summary-section">
            <h3 style="margin-bottom: 16px;">📝 周度总结</h3>
            <p class="summary-text">{weekly_summary}</p>
        </div>

        <div class="footer">
            <p>本报告由资金行为学量化策略系统生成</p>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""

    def generate_monthly_report(self, monthly_data: Dict) -> str:
        """生成月报HTML"""
        year = monthly_data.get('year', 0)
        month = monthly_data.get('month', 0)
        avg_v_total = monthly_data.get('avg_v_total', 0)
        max_v_total = monthly_data.get('max_v_total', 0)
        min_v_total = monthly_data.get('min_v_total', 0)
        avg_sentiment = monthly_data.get('avg_sentiment', 0)
        max_sentiment = monthly_data.get('max_sentiment', 0)
        min_sentiment = monthly_data.get('min_sentiment', 0)
        sentiment_trend = monthly_data.get('sentiment_trend', 'stable')
        state_dist = json.loads(monthly_data.get('market_state_distribution', '{}'))
        strong_days = monthly_data.get('strong_days', 0)
        oscillating_days = monthly_data.get('oscillating_days', 0)
        weak_days = monthly_data.get('weak_days', 0)
        upward_pivot_ratio = monthly_data.get('upward_pivot_ratio', 0) * 100
        hedge_effect_ratio = monthly_data.get('hedge_effect_ratio', 0) * 100
        avg_position = monthly_data.get('avg_position', 0)
        top_trend_stocks = json.loads(monthly_data.get('top_trend_stocks', '[]'))
        top_short_stocks = json.loads(monthly_data.get('top_short_stocks', '[]'))
        monthly_summary = monthly_data.get('monthly_summary', '')
        performance_review = monthly_data.get('performance_review', '')

        trend_icon = '📈' if sentiment_trend == 'rising' else '📉' if sentiment_trend == 'falling' else '➡️'
        sentiment_color = '#00e676' if sentiment_trend == 'rising' else '#ff5252' if sentiment_trend == 'falling' else '#ffab40'

        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>月度报告 - {year}年{month}月</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
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
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{
            text-align: center;
            padding: 40px 20px;
            margin-bottom: 30px;
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-card));
            border-radius: 20px;
            border: 1px solid var(--border-color);
        }}
        .header h1 {{
            font-size: 2.5em;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-purple), var(--accent-cyan));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .header .period {{
            color: var(--text-secondary);
            font-size: 1.3em;
            margin-top: 10px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 20px;
            text-align: center;
            border: 1px solid var(--border-color);
        }}
        .metric-value {{
            font-size: 1.6em;
            font-weight: 700;
            color: var(--accent-blue);
        }}
        .metric-label {{
            color: var(--text-secondary);
            font-size: 0.85em;
            margin-top: 6px;
        }}
        .two-col {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
        }}
        .card h3 {{
            margin-bottom: 16px;
            color: var(--text-primary);
        }}
        .state-bar {{
            display: flex;
            height: 40px;
            border-radius: 20px;
            overflow: hidden;
            margin: 20px 0;
        }}
        .state-strong {{ background: linear-gradient(90deg, #00c6ff, #0072ff); }}
        .state-oscillating {{ background: linear-gradient(90deg, #f093fb, #f5576c); }}
        .state-weak {{ background: linear-gradient(90deg, #667eea, #764ba2); }}
        .stock-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .stock-tag {{
            background: rgba(79, 172, 254, 0.15);
            color: var(--accent-cyan);
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.85em;
        }}
        .summary-card {{
            background: var(--bg-card);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 30px;
            border: 1px solid var(--border-color);
        }}
        .summary-text {{
            line-height: 1.8;
            color: var(--text-secondary);
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: var(--text-secondary);
            font-size: 0.85em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 月度量化报告</h1>
            <p class="period">{year}年{month}月</p>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{avg_v_total/10000:.2f}万亿</div>
                <div class="metric-label">月均成交额</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{max_v_total/10000:.2f}万亿</div>
                <div class="metric-label">月最大成交额</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{min_v_total/10000:.2f}万亿</div>
                <div class="metric-label">月最小成交额</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: {sentiment_color};">{avg_sentiment:.1f}°</div>
                <div class="metric-label">{trend_icon} 情绪趋势</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: var(--accent-red);">{max_sentiment:.1f}°</div>
                <div class="metric-label">月最高情绪</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color: var(--accent-cyan);">{min_sentiment:.1f}°</div>
                <div class="metric-label">月最低情绪</div>
            </div>
        </div>

        <div class="two-col">
            <div class="card">
                <h3>📈 市场状态分布</h3>
                <div class="state-bar">
                    <div class="state-strong" style="flex: {strong_days};"></div>
                    <div class="state-oscillating" style="flex: {oscillating_days};"></div>
                    <div class="state-weak" style="flex: {weak_days};"></div>
                </div>
                <div style="display: flex; gap: 16px; justify-content: center; flex-wrap: wrap;">
                    <span><span style="color: #00c6ff;">●</span> 强势 {strong_days}天</span>
                    <span><span style="color: #f5576c;">●</span> 震荡 {oscillating_days}天</span>
                    <span><span style="color: #b388ff;">●</span> 弱势 {weak_days}天</span>
                </div>
            </div>
            <div class="card">
                <h3>⚡ 操作统计</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px;">
                    <div style="text-align: center;">
                        <div style="font-size: 1.5em; font-weight: 700; color: var(--accent-green);">{upward_pivot_ratio:.0f}%</div>
                        <div style="color: var(--text-secondary); font-size: 0.85em;">向上变盘占比</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5em; font-weight: 700; color: var(--accent-orange);">{hedge_effect_ratio:.0f}%</div>
                        <div style="color: var(--text-secondary); font-size: 0.85em;">对冲有效占比</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5em; font-weight: 700; color: var(--accent-blue);">{avg_position/10000:.0f}万</div>
                        <div style="color: var(--text-secondary); font-size: 0.85em;">平均仓位</div>
                    </div>
                </div>
            </div>
        </div>

        <div class="two-col">
            <div class="card">
                <h3>📈 波段高频股票 TOP15</h3>
                <div class="stock-list">
                    {''.join([f'<span class="stock-tag">{s}</span>' for s in top_trend_stocks])}
                </div>
            </div>
            <div class="card">
                <h3>⚡ 短线高频股票 TOP15</h3>
                <div class="stock-list">
                    {''.join([f'<span class="stock-tag">{s}</span>' for s in top_short_stocks])}
                </div>
            </div>
        </div>

        <div class="summary-card">
            <h3 style="margin-bottom: 16px;">📝 月度总结</h3>
            <p class="summary-text">{monthly_summary}</p>
        </div>

        <div class="summary-card">
            <h3 style="margin-bottom: 16px;">📋 表现回顾</h3>
            <p class="summary-text">{performance_review}</p>
        </div>

        <div class="footer">
            <p>本报告由资金行为学量化策略系统生成</p>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""


def generate_weekly_html(weekly_data: Dict, daily_reports: List[Dict] = None) -> str:
    """生成周报HTML"""
    generator = WeeklyMonthlyHTMLReport()
    return generator.generate_weekly_report(weekly_data, daily_reports)


def generate_monthly_html(monthly_data: Dict) -> str:
    """生成月报HTML"""
    generator = WeeklyMonthlyHTMLReport()
    return generator.generate_monthly_report(monthly_data)


if __name__ == "__main__":
    sample_weekly = {
        'week_start': '2026-03-31',
        'week_end': '2026-04-06',
        'avg_v_total': 18500,
        'avg_sentiment': 58.5,
        'sentiment_trend': 'rising',
        'market_state_distribution': '{"STRONG": 4, "OSCILLATING": 2, "WEAK": 1}',
        'strong_days': 4,
        'oscillating_days': 2,
        'weak_days': 1,
        'upward_pivot_days': 3,
        'hedge_effect_days': 5,
        'total_position_avg': 850000,
        'top_trend_stocks': '["000001", "600519", "600036"]',
        'top_short_stocks': '["300999", "688001"]',
        'weekly_summary': '本周市场以STRONG为主，周均情绪温度58.5°，情绪升温。'
    }
    html = generate_weekly_html(sample_weekly)
    with open('/tmp/weekly_report.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("周报HTML已生成: /tmp/weekly_report.html")