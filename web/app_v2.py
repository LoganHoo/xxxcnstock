#!/usr/bin/env python3
"""
Web监控面板 v2

增强功能:
- 实时数据推送 (WebSocket)
- 交互式图表 (Chart.js)
- 策略性能对比
- 实时告警

使用方法:
    python web/app_v2.py
    然后访问 http://localhost:5000
"""
import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from threading import Thread
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.monitoring.metrics import (
    performance_monitor, 
    system_monitor, 
    get_monitoring_dashboard_data
)
from core.data_adapter import data_source_manager, init_data_sources

app = Flask(__name__, 
    template_folder='templates',
    static_folder='static'
)
app.config['SECRET_KEY'] = 'quant-trading-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# 模拟数据存储
simulation_data = {
    'portfolio': {
        'cash': 1000000,
        'initial_capital': 1000000,
        'positions': {},
        'history': []
    },
    'signals': [],
    'daily_reports': [],
    'performance_history': []
}

# 告警配置
alerts = []


@app.route('/')
def index():
    """首页"""
    return render_template('dashboard_v2.html')


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
    
    initial = simulation_data['portfolio']['initial_capital']
    return_pct = (total_value - initial) / initial
    
    return jsonify({
        'cash': simulation_data['portfolio']['cash'],
        'total_value': total_value,
        'return_pct': return_pct,
        'positions': simulation_data['portfolio']['positions'],
        'position_count': len(simulation_data['portfolio']['positions']),
        'history': simulation_data['portfolio']['history'][-30:]  # 最近30天
    })


@app.route('/api/signals')
def api_signals():
    """API: 获取交易信号"""
    limit = request.args.get('limit', 50, type=int)
    strategy = request.args.get('strategy', None)
    
    signals = simulation_data['signals']
    if strategy:
        signals = [s for s in signals if s.get('strategy') == strategy]
    
    return jsonify({
        'signals': signals[-limit:],
        'total': len(signals),
        'by_strategy': {}
    })


@app.route('/api/performance')
def api_performance():
    """API: 获取性能指标"""
    return jsonify(performance_monitor.get_performance_report())


@app.route('/api/performance/history')
def api_performance_history():
    """API: 获取性能历史"""
    days = request.args.get('days', 30, type=int)
    return jsonify({
        'history': simulation_data['performance_history'][-days:]
    })


@app.route('/api/alerts')
def api_alerts():
    """API: 获取告警列表"""
    return jsonify({
        'alerts': alerts[-50:],
        'unread_count': len([a for a in alerts if not a.get('read', False)])
    })


@app.route('/api/market/data')
def api_market_data():
    """API: 获取市场数据"""
    code = request.args.get('code', '000001')
    days = request.args.get('days', 30, type=int)
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    df = data_source_manager.get_daily_data(code, start_date, end_date)
    
    if df.empty:
        return jsonify({'error': 'No data available'})
    
    return jsonify({
        'code': code,
        'data': df.to_dict('records')
    })


# WebSocket事件
@socketio.on('connect')
def handle_connect():
    """客户端连接"""
    print(f'客户端已连接: {request.sid}')
    emit('connected', {'data': 'Connected to Quant Trading System'})


@socketio.on('disconnect')
def handle_disconnect():
    """客户端断开"""
    print(f'客户端已断开: {request.sid}')


@socketio.on('subscribe_market')
def handle_subscribe_market(data):
    """订阅市场数据"""
    codes = data.get('codes', [])
    print(f'订阅股票: {codes}')
    emit('subscribed', {'codes': codes})


def broadcast_update():
    """广播更新"""
    while True:
        socketio.sleep(5)  # 每5秒推送一次
        
        # 获取最新数据
        data = get_monitoring_dashboard_data()
        
        # 广播到所有客户端
        socketio.emit('market_update', data)


# 创建增强版模板
DASHBOARD_V2_HTML = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化交易监控面板 v2</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0a0e17;
            color: #e0e0e0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1a2332 0%, #0d1117 100%);
            padding: 20px 40px;
            border-bottom: 1px solid #30363d;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { font-size: 24px; color: #58a6ff; }
        .connection-status {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 14px;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #238636;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .container { padding: 30px 40px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #161b22;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #30363d;
            transition: transform 0.2s;
        }
        .stat-card:hover { transform: translateY(-2px); }
        .stat-card h3 {
            font-size: 13px;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
        }
        .stat-card .value {
            font-size: 28px;
            font-weight: 700;
            color: #f0f6fc;
        }
        .stat-card .change {
            font-size: 14px;
            margin-top: 8px;
            font-weight: 500;
        }
        .positive { color: #3fb950; }
        .negative { color: #f85149; }
        .chart-container {
            background: #161b22;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #30363d;
            margin-bottom: 20px;
        }
        .chart-container h2 {
            font-size: 16px;
            margin-bottom: 20px;
            color: #58a6ff;
        }
        .grid-2 {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 20px;
        }
        .section {
            background: #161b22;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #30363d;
            margin-bottom: 20px;
        }
        .section h2 {
            font-size: 16px;
            margin-bottom: 20px;
            color: #58a6ff;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #30363d;
        }
        th {
            color: #8b949e;
            font-weight: 500;
        }
        tr:hover { background: #1f242c; }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }
        .badge-success { background: #238636; color: white; }
        .badge-warning { background: #9e6a03; color: white; }
        .badge-danger { background: #da3633; color: white; }
        .alert {
            background: #da3633;
            color: white;
            padding: 12px 16px;
            border-radius: 8px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .alert-info { background: #1f6feb; }
        .alert-warning { background: #9e6a03; }
        .timestamp {
            color: #6e7681;
            font-size: 12px;
            margin-top: 20px;
        }
        .live-indicator {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            color: #3fb950;
        }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>📊 量化交易监控面板 v2</h1>
            <span class="live-indicator">
                <span class="status-dot"></span>
                实时数据推送中
            </span>
        </div>
        <div class="connection-status">
            <span id="connection-status">连接中...</span>
        </div>
    </div>
    
    <div class="container">
        <!-- 告警区域 -->
        <div id="alerts-container"></div>
        
        <!-- 核心指标 -->
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
            <div class="stat-card">
                <h3>夏普比率</h3>
                <div class="value" id="sharpe-ratio">0.00</div>
            </div>
            <div class="stat-card">
                <h3>最大回撤</h3>
                <div class="value" id="max-drawdown">0.00%</div>
            </div>
        </div>
        
        <!-- 图表区域 -->
        <div class="grid-2">
            <div class="chart-container">
                <h2>📈 资产走势</h2>
                <canvas id="portfolio-chart"></canvas>
            </div>
            <div class="chart-container">
                <h2>📊 策略收益对比</h2>
                <canvas id="strategy-chart"></canvas>
            </div>
        </div>
        
        <!-- 交易信号 -->
        <div class="section">
            <h2>🔔 最新交易信号</h2>
            <table>
                <thead>
                    <tr>
                        <th>时间</th>
                        <th>股票代码</th>
                        <th>策略</th>
                        <th>评分</th>
                        <th>状态</th>
                    </tr>
                </thead>
                <tbody id="signals-table">
                    <tr><td colspan="5" style="text-align:center;color:#6e7681;">暂无数据</td></tr>
                </tbody>
            </table>
        </div>
        
        <!-- 持仓详情 -->
        <div class="section">
            <h2>💼 持仓详情</h2>
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
                    <tr><td colspan="6" style="text-align:center;color:#6e7681;">暂无持仓</td></tr>
                </tbody>
            </table>
        </div>
        
        <div class="timestamp" id="last-update">最后更新: --</div>
    </div>
    
    <script>
        // 初始化Socket.IO
        const socket = io();
        
        // 图表实例
        let portfolioChart, strategyChart;
        
        // 连接状态
        socket.on('connect', () => {
            document.getElementById('connection-status').textContent = '已连接';
            document.getElementById('connection-status').style.color = '#3fb950';
        });
        
        socket.on('disconnect', () => {
            document.getElementById('connection-status').textContent = '已断开';
            document.getElementById('connection-status').style.color = '#f85149';
        });
        
        // 实时数据更新
        socket.on('market_update', (data) => {
            updateDashboard(data);
        });
        
        // 初始化图表
        function initCharts() {
            // 资产走势图
            const portfolioCtx = document.getElementById('portfolio-chart').getContext('2d');
            portfolioChart = new Chart(portfolioCtx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: '总资产',
                        data: [],
                        borderColor: '#58a6ff',
                        backgroundColor: 'rgba(88, 166, 255, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                        y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } }
                    }
                }
            });
            
            // 策略收益对比图
            const strategyCtx = document.getElementById('strategy-chart').getContext('2d');
            strategyChart = new Chart(strategyCtx, {
                type: 'bar',
                data: {
                    labels: ['尾盘选股', '龙回头', '涨停回调'],
                    datasets: [{
                        label: '收益率',
                        data: [0, 0, 0],
                        backgroundColor: ['#3fb950', '#58a6ff', '#a371f7']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                        y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } }
                    }
                }
            });
        }
        
        // 更新仪表板
        async function updateDashboard(wsData = null) {
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
                
                // 更新资产走势图
                if (portfolio.history && portfolioChart) {
                    portfolioChart.data.labels = portfolio.history.map(h => h.date);
                    portfolioChart.data.datasets[0].data = portfolio.history.map(h => h.value);
                    portfolioChart.update();
                }
                
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
                            <td><span class="badge badge-success">活跃</span></td>
                        </tr>
                    `).join('');
                }
                
                // 获取性能数据
                const perfRes = await fetch('/api/performance');
                const perf = await perfRes.json();
                document.getElementById('win-rate').textContent = 
                    (perf.win_rate * 100).toFixed(1) + '%';
                document.getElementById('sharpe-ratio').textContent = 
                    (perf.sharpe_ratio || 0).toFixed(2);
                document.getElementById('max-drawdown').textContent = 
                    (perf.max_drawdown || 0).toFixed(2) + '%';
                
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
                console.error('更新数据失败:', err);
            }
        }
        
        // 初始化
        initCharts();
        updateDashboard();
        
        // 定时刷新（作为WebSocket的备用）
        setInterval(updateDashboard, 10000);
    </script>
</body>
</html>'''

# 写入模板文件
os.makedirs('web/templates', exist_ok=True)
with open('web/templates/dashboard_v2.html', 'w', encoding='utf-8') as f:
    f.write(DASHBOARD_V2_HTML)


def start_background_tasks():
    """启动后台任务"""
    def run_broadcast():
        while True:
            socketio.sleep(5)
            data = get_monitoring_dashboard_data()
            socketio.emit('market_update', data)
    
    socketio.start_background_task(run_broadcast)


if __name__ == '__main__':
    print("🚀 启动监控面板 v2...")
    print("📊 访问地址: http://localhost:5000")
    
    # 初始化数据源
    init_data_sources({
        'tushare': {'enabled': True},
        'baostock': {'enabled': True}
    })
    
    # 启动后台任务
    start_background_tasks()
    
    # 启动服务器
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
