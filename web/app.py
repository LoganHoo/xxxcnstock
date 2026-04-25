#!/usr/bin/env python3
"""
Web监控面板

提供实时交易监控和报告查看

使用方法:
    python web/app.py
    然后访问 http://localhost:5000
"""
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, render_template, jsonify, request

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.monitoring.metrics import (
    performance_monitor, 
    system_monitor, 
    get_monitoring_dashboard_data
)

app = Flask(__name__, 
    template_folder='templates',
    static_folder='static'
)

# 模拟数据存储
simulation_data = {
    'portfolio': {
        'cash': 1000000,
        'positions': {},
        'history': []
    },
    'signals': [],
    'daily_reports': []
}


@app.route('/')
def index():
    """首页"""
    return render_template('dashboard.html')


@app.route('/api/dashboard')
def api_dashboard():
    """API: 获取仪表板数据"""
    return jsonify(get_monitoring_dashboard_data())


@app.route('/api/portfolio')
def api_portfolio():
    """API: 获取投资组合数据"""
    total_value = simulation_data['portfolio']['cash']
    for code, pos in simulation_data['portfolio']['positions'].items():
        total_value += pos.get('value', 0)
    
    initial = 1000000
    return_pct = (total_value - initial) / initial
    
    return jsonify({
        'cash': simulation_data['portfolio']['cash'],
        'total_value': total_value,
        'return_pct': return_pct,
        'positions': simulation_data['portfolio']['positions'],
        'position_count': len(simulation_data['portfolio']['positions'])
    })


@app.route('/api/signals')
def api_signals():
    """API: 获取交易信号"""
    limit = request.args.get('limit', 50, type=int)
    return jsonify({
        'signals': simulation_data['signals'][-limit:],
        'total': len(simulation_data['signals'])
    })


@app.route('/api/reports')
def api_reports():
    """API: 获取报告列表"""
    reports_dir = Path('reports')
    if not reports_dir.exists():
        return jsonify({'reports': []})
    
    reports = []
    for f in sorted(reports_dir.glob('daily_report_*.html'), reverse=True):
        date_str = f.stem.replace('daily_report_', '')
        reports.append({
            'date': date_str,
            'filename': f.name,
            'url': f'/reports/{f.name}'
        })
    
    return jsonify({'reports': reports[:30]})


@app.route('/api/performance')
def api_performance():
    """API: 获取性能指标"""
    return jsonify(performance_monitor.get_performance_report())


@app.route('/api/health')
def api_health():
    """API: 获取健康状态"""
    return jsonify(system_monitor.run_health_checks())


@app.route('/reports/<path:filename>')
def serve_report(filename):
    """提供报告文件"""
    return app.send_static_file(f'reports/{filename}')


# 创建模板目录和基础模板
os.makedirs('web/templates', exist_ok=True)
os.makedirs('web/static/css', exist_ok=True)
os.makedirs('web/static/js', exist_ok=True)

# 基础HTML模板
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化交易监控面板</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f1419;
            color: #e0e0e0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1a1f2e 0%, #2d3748 100%);
            padding: 20px 40px;
            border-bottom: 1px solid #374151;
        }
        .header h1 { font-size: 24px; color: #60a5fa; }
        .header .status { 
            display: inline-block; 
            padding: 4px 12px; 
            border-radius: 12px; 
            font-size: 12px;
            margin-left: 15px;
        }
        .status.online { background: #065f46; color: #34d399; }
        .container { padding: 30px 40px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #1a1f2e;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #374151;
        }
        .stat-card h3 {
            font-size: 13px;
            color: #9ca3af;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
        }
        .stat-card .value {
            font-size: 32px;
            font-weight: 700;
            color: #f3f4f6;
        }
        .stat-card .change {
            font-size: 14px;
            margin-top: 8px;
            font-weight: 500;
        }
        .positive { color: #34d399; }
        .negative { color: #f87171; }
        .section {
            background: #1a1f2e;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #374151;
            margin-bottom: 20px;
        }
        .section h2 {
            font-size: 18px;
            margin-bottom: 20px;
            color: #60a5fa;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #374151;
        }
        th {
            color: #9ca3af;
            font-weight: 500;
            font-size: 13px;
        }
        td { color: #e0e0e0; }
        tr:hover { background: #252b3b; }
        .refresh-btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
        }
        .refresh-btn:hover { background: #2563eb; }
        .timestamp {
            color: #6b7280;
            font-size: 13px;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 量化交易监控面板 <span class="status online">● 运行中</span></h1>
    </div>
    
    <div class="container">
        <div class="stats-grid">
            <div class="stat-card">
                <h3>总资产</h3>
                <div class="value" id="total-value">¥1,000,000</div>
                <div class="change" id="return-pct">+0.00%</div>
            </div>
            <div class="stat-card">
                <h3>今日信号</h3>
                <div class="value" id="today-signals">0</div>
            </div>
            <div class="stat-card">
                <h3>持仓数量</h3>
                <div class="value" id="position-count">0</div>
            </div>
            <div class="stat-card">
                <h3>胜率</h3>
                <div class="value" id="win-rate">0%</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📈 最新交易信号</h2>
            <table>
                <thead>
                    <tr>
                        <th>时间</th>
                        <th>股票代码</th>
                        <th>策略</th>
                        <th>评分</th>
                    </tr>
                </thead>
                <tbody id="signals-table">
                    <tr><td colspan="4" style="text-align:center;color:#6b7280;">暂无数据</td></tr>
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>💰 持仓详情</h2>
            <table>
                <thead>
                    <tr>
                        <th>股票代码</th>
                        <th>数量</th>
                        <th>成本价</th>
                        <th>当前价</th>
                        <th>市值</th>
                        <th>盈亏</th>
                    </tr>
                </thead>
                <tbody id="positions-table">
                    <tr><td colspan="6" style="text-align:center;color:#6b7280;">暂无持仓</td></tr>
                </tbody>
            </table>
        </div>
        
        <button class="refresh-btn" onclick="refreshData()">🔄 刷新数据</button>
        <div class="timestamp" id="last-update">最后更新: --</div>
    </div>
    
    <script>
        async function refreshData() {
            try {
                // 获取投资组合数据
                const portfolioRes = await fetch('/api/portfolio');
                const portfolio = await portfolioRes.json();
                
                document.getElementById('total-value').textContent = 
                    '¥' + portfolio.total_value.toLocaleString();
                document.getElementById('return-pct').textContent = 
                    (portfolio.return_pct >= 0 ? '+' : '') + (portfolio.return_pct * 100).toFixed(2) + '%';
                document.getElementById('return-pct').className = 
                    'change ' + (portfolio.return_pct >= 0 ? 'positive' : 'negative');
                document.getElementById('position-count').textContent = portfolio.position_count;
                
                // 获取信号数据
                const signalsRes = await fetch('/api/signals?limit=10');
                const signalsData = await signalsRes.json();
                document.getElementById('today-signals').textContent = signalsData.total;
                
                const signalsTable = document.getElementById('signals-table');
                if (signalsData.signals.length > 0) {
                    signalsTable.innerHTML = signalsData.signals.map(s => `
                        <tr>
                            <td>${s.time || '--'}</td>
                            <td>${s.code}</td>
                            <td>${s.strategy}</td>
                            <td>${s.score}</td>
                        </tr>
                    `).join('');
                }
                
                // 获取性能数据
                const perfRes = await fetch('/api/performance');
                const perf = await perfRes.json();
                document.getElementById('win-rate').textContent = 
                    (perf.win_rate * 100).toFixed(1) + '%';
                
                // 更新持仓表
                const positionsTable = document.getElementById('positions-table');
                if (Object.keys(portfolio.positions).length > 0) {
                    positionsTable.innerHTML = Object.entries(portfolio.positions).map(([code, pos]) => {
                        const pnl = pos.pnl || 0;
                        return `
                            <tr>
                                <td>${code}</td>
                                <td>${pos.quantity}</td>
                                <td>¥${pos.cost?.toFixed(2) || '--'}</td>
                                <td>¥${pos.price?.toFixed(2) || '--'}</td>
                                <td>¥${pos.value?.toLocaleString() || '--'}</td>
                                <td class="${pnl >= 0 ? 'positive' : 'negative'}">${(pnl * 100).toFixed(2)}%</td>
                            </tr>
                        `;
                    }).join('');
                }
                
                document.getElementById('last-update').textContent = 
                    '最后更新: ' + new Date().toLocaleString();
            } catch (err) {
                console.error('刷新数据失败:', err);
            }
        }
        
        // 初始加载和定时刷新
        refreshData();
        setInterval(refreshData, 30000); // 每30秒刷新
    </script>
</body>
</html>'''

# 写入模板文件
with open('web/templates/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(DASHBOARD_HTML)


if __name__ == '__main__':
    print("🚀 启动监控面板...")
    print("📊 访问地址: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
