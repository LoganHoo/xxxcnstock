#!/usr/bin/env python3
"""
双调度器监控面板
提供Web界面查看主备状态
"""

import sys
import os
import json
import redis
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, render_template_string

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

app = Flask(__name__)

# HTML模板
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>XCNStock 双调度器监控</title>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="10">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            margin-bottom: 30px;
        }
        .status-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .status-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        .status-title {
            font-size: 18px;
            font-weight: 600;
        }
        .status-badge {
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 14px;
            font-weight: 500;
        }
        .status-healthy {
            background: #d4edda;
            color: #155724;
        }
        .status-degraded {
            background: #fff3cd;
            color: #856404;
        }
        .status-down {
            background: #f8d7da;
            color: #721c24;
        }
        .status-unknown {
            background: #e2e3e5;
            color: #383d41;
        }
        .status-primary {
            background: #cce5ff;
            color: #004085;
        }
        .status-backup {
            background: #e2e3e5;
            color: #383d41;
        }
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        .info-item {
            padding: 10px;
            background: #f8f9fa;
            border-radius: 4px;
        }
        .info-label {
            font-size: 12px;
            color: #666;
            margin-bottom: 4px;
        }
        .info-value {
            font-size: 14px;
            font-weight: 500;
            color: #333;
        }
        .overview {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .overview-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .overview-number {
            font-size: 36px;
            font-weight: 700;
            color: #333;
        }
        .overview-label {
            font-size: 14px;
            color: #666;
            margin-top: 5px;
        }
        .refresh-time {
            text-align: right;
            color: #999;
            font-size: 12px;
            margin-top: 20px;
        }
        .priority-badge {
            display: inline-block;
            background: #ffd700;
            color: #333;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            margin-left: 10px;
        }
        .strategy-info {
            background: #e7f3ff;
            border-left: 4px solid #0066cc;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 4px;
        }
        .strategy-title {
            font-weight: 600;
            color: #0066cc;
            margin-bottom: 8px;
        }
        .strategy-item {
            font-size: 13px;
            color: #555;
            margin: 4px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔧 XCNStock 双调度器监控面板 <span class="priority-badge">⭐ Kestra优先</span></h1>

        <div class="strategy-info">
            <div class="strategy-title">📋 优先级策略: Kestra > APScheduler</div>
            <div class="strategy-item">1. Kestra健康时，始终是主调度器</div>
            <div class="strategy-item">2. Kestra宕机时，APScheduler接管</div>
            <div class="strategy-item">3. Kestra恢复后，自动切回Kestra</div>
        </div>

        <div class="overview">
            <div class="overview-card">
                <div class="overview-number">{{ kestra.role == 'PRIMARY' and '主 ⭐' or '备' }}</div>
                <div class="overview-label">Kestra 角色</div>
            </div>
            <div class="overview-card">
                <div class="overview-number">{{ apscheduler.role == 'PRIMARY' and '主' or '备' }}</div>
                <div class="overview-label">APScheduler 角色</div>
            </div>
            <div class="overview-card">
                <div class="overview-number">{{ task_count }}</div>
                <div class="overview-label">监控任务数</div>
            </div>
        </div>
        
        <div class="status-card">
            <div class="status-header">
                <span class="status-title">🚀 Kestra 调度器</span>
                <span class="status-badge status-{{ kestra.status }}">{{ kestra.status_text }}</span>
            </div>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">角色</div>
                    <div class="info-value">
                        <span class="status-badge status-{{ kestra.role == 'PRIMARY' and 'primary' or 'backup' }}">
                            {{ kestra.role == 'PRIMARY' and '主调度器' or '备调度器' }}
                        </span>
                    </div>
                </div>
                <div class="info-item">
                    <div class="info-label">最后心跳</div>
                    <div class="info-value">{{ kestra.last_heartbeat }}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">活跃任务</div>
                    <div class="info-value">{{ kestra.active_tasks }}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">连续失败</div>
                    <div class="info-value">{{ kestra.failed_checks }}</div>
                </div>
            </div>
        </div>
        
        <div class="status-card">
            <div class="status-header">
                <span class="status-title">⏰ APScheduler 调度器</span>
                <span class="status-badge status-{{ apscheduler.status }}">{{ apscheduler.status_text }}</span>
            </div>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">角色</div>
                    <div class="info-value">
                        <span class="status-badge status-{{ apscheduler.role == 'PRIMARY' and 'primary' or 'backup' }}">
                            {{ apscheduler.role == 'PRIMARY' and '主调度器' or '备调度器' }}
                        </span>
                    </div>
                </div>
                <div class="info-item">
                    <div class="info-label">最后心跳</div>
                    <div class="info-value">{{ apscheduler.last_heartbeat }}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">活跃任务</div>
                    <div class="info-value">{{ apscheduler.active_tasks }}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">连续失败</div>
                    <div class="info-value">{{ apscheduler.failed_checks }}</div>
                </div>
            </div>
        </div>
        
        <div class="refresh-time">
            最后更新: {{ refresh_time }}
        </div>
    </div>
</body>
</html>
"""


def get_redis_client():
    """获取Redis连接"""
    try:
        return redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
    except Exception as e:
        print(f"Redis连接失败: {e}")
        return None


def get_scheduler_states():
    """获取调度器状态"""
    client = get_redis_client()
    if not client:
        return {}, {}
    
    try:
        states = client.hgetall("xcnstock:scheduler:heartbeat")
        
        kestra_state = {}
        apscheduler_state = {}
        
        for name, state_json in states.items():
            try:
                state = json.loads(state_json)
                if 'kestra' in name:
                    kestra_state = state
                elif 'apscheduler' in name:
                    apscheduler_state = state
            except:
                pass
        
        return kestra_state, apscheduler_state
    except Exception as e:
        print(f"获取状态失败: {e}")
        return {}, {}


def format_status(status):
    """格式化状态显示"""
    status_map = {
        'healthy': ('healthy', '健康'),
        'degraded': ('degraded', '降级'),
        'down': ('down', '宕机'),
        'unknown': ('unknown', '未知')
    }
    return status_map.get(status, ('unknown', '未知'))


def format_datetime(dt_str):
    """格式化日期时间"""
    if not dt_str:
        return "无数据"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return dt_str


@app.route('/')
def dashboard():
    """监控面板"""
    kestra_state, apscheduler_state = get_scheduler_states()
    
    kestra_status, kestra_status_text = format_status(kestra_state.get('status', 'unknown'))
    apscheduler_status, apscheduler_status_text = format_status(apscheduler_state.get('status', 'unknown'))
    
    return render_template_string(
        DASHBOARD_TEMPLATE,
        kestra={
            'role': kestra_state.get('role', 'UNKNOWN'),
            'status': kestra_status,
            'status_text': kestra_status_text,
            'last_heartbeat': format_datetime(kestra_state.get('last_heartbeat')),
            'active_tasks': kestra_state.get('active_tasks', 0),
            'failed_checks': kestra_state.get('failed_checks', 0)
        },
        apscheduler={
            'role': apscheduler_state.get('role', 'UNKNOWN'),
            'status': apscheduler_status,
            'status_text': apscheduler_status_text,
            'last_heartbeat': format_datetime(apscheduler_state.get('last_heartbeat')),
            'active_tasks': apscheduler_state.get('active_tasks', 0),
            'failed_checks': apscheduler_state.get('failed_checks', 0)
        },
        task_count=40,
        refresh_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


@app.route('/api/status')
def api_status():
    """API状态接口"""
    kestra_state, apscheduler_state = get_scheduler_states()
    
    return jsonify({
        'kestra': kestra_state,
        'apscheduler': apscheduler_state,
        'timestamp': datetime.now().isoformat()
    })


def main():
    """主函数"""
    app.run(host='0.0.0.0', port=8083, debug=False)


if __name__ == '__main__':
    main()
