#!/usr/bin/env python3
"""
每日报告生成器

生成HTML格式的交易报告

使用方法:
    python scripts/report_generator.py --date 2024-04-19 --output reports/
"""
import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, output_dir: str = 'reports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_daily_report(
        self,
        date: str,
        signals: List[Dict],
        portfolio: Dict,
        market_data: Dict
    ) -> str:
        """生成每日报告"""
        logger.info(f"生成 {date} 日报")
        
        # 计算指标
        total_value = portfolio.get('cash', 0)
        for code, pos in portfolio.get('positions', {}).items():
            total_value += pos.get('value', 0)
        
        initial = portfolio.get('initial_capital', 1000000)
        return_pct = (total_value - initial) / initial
        
        # 生成HTML
        html = self._generate_html_template({
            'date': date,
            'signals': signals,
            'portfolio': portfolio,
            'total_value': total_value,
            'return_pct': return_pct,
            'market_data': market_data
        })
        
        # 保存报告
        report_path = self.output_dir / f'daily_report_{date}.html'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"报告已保存: {report_path}")
        return str(report_path)
    
    def _generate_html_template(self, data: Dict) -> str:
        """生成HTML模板"""
        date = data['date']
        signals = data['signals']
        total_value = data['total_value']
        return_pct = data['return_pct']
        portfolio = data['portfolio']
        
        signals_html = ""
        for i, signal in enumerate(signals[:20], 1):
            signals_html += f"""
            <tr>
                <td>{i}</td>
                <td>{signal.get('code', 'N/A')}</td>
                <td>{signal.get('strategy', 'N/A')}</td>
                <td>{signal.get('score', 0)}</td>
            </tr>
            """
        
        if not signals:
            signals_html = '<tr><td colspan="4" style="text-align:center;">今日无信号</td></tr>'
        
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化交易日报 - {date}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .header .date {{ opacity: 0.9; font-size: 16px; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        .stat-card h3 {{
            font-size: 14px;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
        }}
        .stat-card .value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
        }}
        .stat-card .change {{
            font-size: 14px;
            margin-top: 5px;
        }}
        .positive {{ color: #e74c3c; }}
        .negative {{ color: #27ae60; }}
        .section {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin-bottom: 20px;
        }}
        .section h2 {{
            font-size: 20px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f0f0f0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #f0f0f0;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #666;
        }}
        tr:hover {{ background: #f8f9fa; }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #999;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 量化交易日报</h1>
            <div class="date">{date}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>总资产</h3>
                <div class="value">¥{total_value:,.0f}</div>
            </div>
            <div class="stat-card">
                <h3>收益率</h3>
                <div class="value">{return_pct:+.2%}</div>
                <div class="change {'positive' if return_pct >= 0 else 'negative'}">
                    {'📈' if return_pct >= 0 else '📉'} 较昨日
                </div>
            </div>
            <div class="stat-card">
                <h3>今日信号</h3>
                <div class="value">{len(signals)}</div>
            </div>
            <div class="stat-card">
                <h3>持仓数量</h3>
                <div class="value">{len(portfolio.get('positions', {}))}</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📈 今日交易信号</h2>
            <table>
                <thead>
                    <tr>
                        <th>序号</th>
                        <th>股票代码</th>
                        <th>策略</th>
                        <th>评分</th>
                    </tr>
                </thead>
                <tbody>
                    {signals_html}
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>💰 持仓详情</h2>
            <table>
                <thead>
                    <tr>
                        <th>股票代码</th>
                        <th>持仓数量</th>
                        <th>成本价</th>
                        <th>当前价</th>
                        <th>市值</th>
                        <th>盈亏</th>
                    </tr>
                </thead>
                <tbody>
                    {self._generate_positions_html(portfolio.get('positions', {}))}
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>量化交易系统 v1.0.0 | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""
    
    def _generate_positions_html(self, positions: Dict) -> str:
        """生成持仓HTML"""
        if not positions:
            return '<tr><td colspan="6" style="text-align:center;">当前无持仓</td></tr>'
        
        html = ""
        for code, pos in positions.items():
            pnl = pos.get('pnl', 0)
            pnl_class = 'positive' if pnl >= 0 else 'negative'
            html += f"""
            <tr>
                <td>{code}</td>
                <td>{pos.get('quantity', 0)}</td>
                <td>¥{pos.get('cost', 0):.2f}</td>
                <td>¥{pos.get('price', 0):.2f}</td>
                <td>¥{pos.get('value', 0):,.0f}</td>
                <td class="{pnl_class}">{pnl:+.2%}</td>
            </tr>
            """
        return html
    
    def generate_summary_report(self, start_date: str, end_date: str, daily_results: List[Dict]) -> str:
        """生成汇总报告"""
        logger.info(f"生成 {start_date} ~ {end_date} 汇总报告")
        
        # 计算汇总指标
        total_signals = sum(len(d.get('signals', [])) for d in daily_results)
        avg_signals = total_signals / len(daily_results) if daily_results else 0
        
        final_value = daily_results[-1].get('portfolio_value', 1000000) if daily_results else 1000000
        initial_value = daily_results[0].get('portfolio_value', 1000000) if daily_results else 1000000
        total_return = (final_value - initial_value) / initial_value
        
        # 生成HTML
        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>量化交易汇总报告</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f7fa; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 40px; border-radius: 10px; }}
        h1 {{ color: #333; border-bottom: 3px solid #667eea; padding-bottom: 15px; }}
        .summary {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin: 30px 0; }}
        .metric {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 36px; font-weight: bold; color: #667eea; }}
        .metric-label {{ color: #666; margin-top: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 量化交易汇总报告</h1>
        <p>报告周期: {start_date} ~ {end_date}</p>
        
        <div class="summary">
            <div class="metric">
                <div class="metric-value">{total_return:+.2%}</div>
                <div class="metric-label">总收益率</div>
            </div>
            <div class="metric">
                <div class="metric-value">{total_signals}</div>
                <div class="metric-label">总信号数</div>
            </div>
            <div class="metric">
                <div class="metric-value">{avg_signals:.1f}</div>
                <div class="metric-label">日均信号</div>
            </div>
        </div>
    </div>
</body>
</html>"""
        
        report_path = self.output_dir / f'summary_report_{start_date}_{end_date}.html'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        return str(report_path)


def main():
    parser = argparse.ArgumentParser(description='报告生成器')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'), help='报告日期')
    parser.add_argument('--output', default='reports', help='输出目录')
    
    args = parser.parse_args()
    
    # 创建生成器
    generator = ReportGenerator(args.output)
    
    # 模拟数据
    mock_signals = [
        {'code': '000001', 'strategy': '尾盘选股', 'score': 85},
        {'code': '000002', 'strategy': '尾盘选股', 'score': 78},
        {'code': '000063', 'strategy': '龙回头', 'score': 92},
    ]
    
    mock_portfolio = {
        'cash': 800000,
        'initial_capital': 1000000,
        'positions': {
            '000001': {'quantity': 1000, 'cost': 10.5, 'price': 12.0, 'value': 12000, 'pnl': 0.14},
            '000002': {'quantity': 2000, 'cost': 8.0, 'price': 7.5, 'value': 15000, 'pnl': -0.06}
        }
    }
    
    mock_market = {'index_change': 0.02}
    
    # 生成报告
    report_path = generator.generate_daily_report(
        args.date,
        mock_signals,
        mock_portfolio,
        mock_market
    )
    
    print(f"✅ 报告已生成: {report_path}")


if __name__ == '__main__':
    main()
