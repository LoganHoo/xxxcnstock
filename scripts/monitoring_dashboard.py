#!/usr/bin/env python3
"""
任务监控面板 - 生成HTML监控报告
功能：
1. 读取任务状态文件
2. 生成可视化监控面板
3. 支持自动刷新
4. 发送监控摘要邮件

使用方法:
    python scripts/monitoring_dashboard.py
    python scripts/monitoring_dashboard.py --refresh  # 生成带自动刷新的页面
    python scripts/monitoring_dashboard.py --email    # 发送监控摘要
"""
import sys
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

os.environ['PYTHONUNBUFFERED'] = '1'

from core.logger import setup_logger

logger = setup_logger(
    name="monitoring_dashboard",
    level="INFO",
    log_file="system/monitoring_dashboard.log",
    rotation="1 day",
    retention="7 days"
)

# 配置
STATE_FILE = project_root / "logs" / "task_states.json"
CACHE_HEALTH_FILE = project_root / "logs" / "cache_health.json"
OUTPUT_DIR = project_root / "reports"
OUTPUT_FILE = OUTPUT_DIR / "monitoring_dashboard.html"


@dataclass
class TaskInfo:
    """任务信息数据类"""
    job_id: str
    status: str
    last_run: Optional[str]
    retries: int
    result: Optional[str]
    date: str


class MonitoringDashboard:
    """监控面板生成器"""
    
    def __init__(self):
        self.tasks: List[TaskInfo] = []
        self.cache_health: Optional[Dict] = None
        self.today = datetime.now().strftime('%Y%m%d')
        
    def load_task_states(self) -> List[TaskInfo]:
        """加载任务状态"""
        tasks = []
        
        if not STATE_FILE.exists():
            logger.warning(f"状态文件不存在: {STATE_FILE}")
            return tasks
        
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                states = json.load(f)
            
            for key, state in states.items():
                # 解析任务ID和日期
                if '_' in key:
                    parts = key.rsplit('_', 1)
                    if len(parts) == 2 and len(parts[1]) == 8:  # YYYYMMDD
                        job_id, date = parts
                        
                        # 只加载最近7天的数据
                        if date >= (datetime.now() - timedelta(days=7)).strftime('%Y%m%d'):
                            tasks.append(TaskInfo(
                                job_id=job_id,
                                status=state.get('status', 'unknown'),
                                last_run=state.get('last_run'),
                                retries=state.get('retries', 0),
                                result=state.get('result'),
                                date=date
                            ))
            
            self.tasks = tasks
            logger.info(f"加载了 {len(tasks)} 个任务状态")
            
        except Exception as e:
            logger.error(f"加载任务状态失败: {e}")
        
        return tasks
    
    def load_cache_health(self) -> Optional[Dict]:
        """加载缓存健康状态"""
        if not CACHE_HEALTH_FILE.exists():
            return None
        
        try:
            with open(CACHE_HEALTH_FILE, 'r', encoding='utf-8') as f:
                self.cache_health = json.load(f)
            return self.cache_health
        except Exception as e:
            logger.error(f"加载缓存健康状态失败: {e}")
            return None
    
    def get_status_stats(self) -> Dict[str, int]:
        """获取状态统计"""
        today_tasks = [t for t in self.tasks if t.date == self.today]
        
        stats = {
            'total': len(today_tasks),
            'completed': sum(1 for t in today_tasks if t.status == 'completed'),
            'failed': sum(1 for t in today_tasks if t.status == 'failed'),
            'running': sum(1 for t in today_tasks if t.status == 'running'),
            'pending': sum(1 for t in today_tasks if t.status == 'pending'),
            'retry_scheduled': sum(1 for t in today_tasks if t.status == 'retry_scheduled'),
        }
        return stats
    
    def get_task_groups(self) -> Dict[str, List[TaskInfo]]:
        """按组分类任务"""
        groups = {
            '晨间任务': [],
            '盘后任务': [],
            '晚间任务': [],
            '系统任务': [],
            '其他': []
        }
        
        morning_tasks = ['morning_data', 'morning_report', 'fund_behavior_report', 'collect_news']
        afternoon_tasks = ['data_fetch', 'data_quality_check', 'market_review', 'review_report']
        evening_tasks = ['precompute', 'night_analysis', 'update_tracking']
        system_tasks = ['scheduler_watchdog', 'cache_cleanup']
        
        today_tasks = [t for t in self.tasks if t.date == self.today]
        
        for task in today_tasks:
            if any(task.job_id.startswith(m) for m in morning_tasks):
                groups['晨间任务'].append(task)
            elif any(task.job_id.startswith(a) for a in afternoon_tasks):
                groups['盘后任务'].append(task)
            elif any(task.job_id.startswith(e) for e in evening_tasks):
                groups['晚间任务'].append(task)
            elif any(task.job_id.startswith(s) for s in system_tasks):
                groups['系统任务'].append(task)
            else:
                groups['其他'].append(task)
        
        return groups
    
    def generate_html(self, auto_refresh: bool = False) -> str:
        """生成HTML监控面板"""
        stats = self.get_status_stats()
        groups = self.get_task_groups()
        
        refresh_meta = '<meta http-equiv="refresh" content="60">' if auto_refresh else ''
        
        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    {refresh_meta}
    <title>XCNStock 任务监控面板</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .header p {{ opacity: 0.9; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-card h3 {{ font-size: 14px; color: #666; margin-bottom: 10px; }}
        .stat-card .number {{ font-size: 36px; font-weight: bold; }}
        .stat-card.completed {{ border-top: 4px solid #52c41a; }}
        .stat-card.failed {{ border-top: 4px solid #f5222d; }}
        .stat-card.running {{ border-top: 4px solid #1890ff; }}
        .stat-card.pending {{ border-top: 4px solid #faad14; }}
        .stat-card.total {{ border-top: 4px solid #722ed1; }}
        .section {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            font-size: 18px;
            margin-bottom: 15px;
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
            background: #fafafa;
            font-weight: 600;
            color: #666;
        }}
        tr:hover {{ background: #f5f5f5; }}
        .status {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }}
        .status.completed {{ background: #f6ffed; color: #52c41a; }}
        .status.failed {{ background: #fff2f0; color: #f5222d; }}
        .status.running {{ background: #e6f7ff; color: #1890ff; }}
        .status.pending {{ background: #fffbe6; color: #faad14; }}
        .status.retry_scheduled {{ background: #f9f0ff; color: #722ed1; }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #999;
            font-size: 12px;
        }}
        .cache-info {{
            background: #f6ffed;
            border: 1px solid #b7eb8f;
            border-radius: 8px;
            padding: 15px;
            margin-top: 15px;
        }}
        .cache-info.warning {{
            background: #fffbe6;
            border-color: #ffe58f;
        }}
        .cache-info.error {{
            background: #fff2f0;
            border-color: #ffccc7;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 XCNStock 任务监控面板</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        {f'<p>⏱️ 自动刷新: 60秒</p>' if auto_refresh else ''}
    </div>
    
    <div class="container">
        <div class="stats-grid">
            <div class="stat-card total">
                <h3>今日任务总数</h3>
                <div class="number" style="color: #722ed1;">{stats['total']}</div>
            </div>
            <div class="stat-card completed">
                <h3>已完成</h3>
                <div class="number" style="color: #52c41a;">{stats['completed']}</div>
            </div>
            <div class="stat-card failed">
                <h3>失败</h3>
                <div class="number" style="color: #f5222d;">{stats['failed']}</div>
            </div>
            <div class="stat-card running">
                <h3>运行中</h3>
                <div class="number" style="color: #1890ff;">{stats['running']}</div>
            </div>
            <div class="stat-card pending">
                <h3>待执行</h3>
                <div class="number" style="color: #faad14;">{stats['pending']}</div>
            </div>
        </div>
"""
        
        # 添加各组任务
        for group_name, tasks in groups.items():
            if not tasks:
                continue
            
            html += f"""
        <div class="section">
            <h2>{group_name} ({len(tasks)}个)</h2>
            <table>
                <thead>
                    <tr>
                        <th>任务名称</th>
                        <th>状态</th>
                        <th>最后运行</th>
                        <th>重试次数</th>
                        <th>结果</th>
                    </tr>
                </thead>
                <tbody>
"""
            for task in sorted(tasks, key=lambda x: x.last_run or '', reverse=True):
                status_class = task.status if task.status in ['completed', 'failed', 'running', 'pending', 'retry_scheduled'] else 'pending'
                html += f"""
                    <tr>
                        <td>{task.job_id}</td>
                        <td><span class="status {status_class}">{task.status}</span></td>
                        <td>{task.last_run or 'N/A'}</td>
                        <td>{task.retries}</td>
                        <td>{task.result or 'N/A'}</td>
                    </tr>
"""
            
            html += """
                </tbody>
            </table>
        </div>
"""
        
        # 添加缓存健康信息
        if self.cache_health:
            cache_class = 'cache-info'
            if self.cache_health.get('files_corrupted', 0) > 0:
                cache_class += ' error'
            elif len(self.cache_health.get('files_expired', [])) > 0:
                cache_class += ' warning'
            
            html += f"""
        <div class="section">
            <h2>🗄️ 缓存健康状态</h2>
            <div class="{cache_class}">
                <p><strong>最后检查:</strong> {self.cache_health.get('timestamp', 'N/A')}</p>
                <p><strong>检查文件数:</strong> {self.cache_health.get('files_checked', 0)}</p>
                <p><strong>损坏文件:</strong> {self.cache_health.get('files_corrupted', 0)}</p>
                <p><strong>过期文件:</strong> {len(self.cache_health.get('files_expired', []))}</p>
            </div>
        </div>
"""
        
        html += """
        <div class="footer">
            <p>XCNStock 监控系统 | 数据每10分钟更新</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def save_dashboard(self, auto_refresh: bool = False) -> Path:
        """保存监控面板"""
        html = self.generate_html(auto_refresh=auto_refresh)
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"监控面板已保存: {OUTPUT_FILE}")
        return OUTPUT_FILE
    
    def send_summary_email(self):
        """发送监控摘要邮件"""
        try:
            from services.email_sender import EmailSender
            
            stats = self.get_status_stats()
            
            subject = f"📊 任务监控摘要 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            content = f"""
任务监控摘要
============
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

今日任务统计:
- 总数: {stats['total']}
- 已完成: {stats['completed']} ✅
- 失败: {stats['failed']} ❌
- 运行中: {stats['running']} 🔄
- 待执行: {stats['pending']} ⏳

"""
            
            # 添加失败任务详情
            failed_tasks = [t for t in self.tasks if t.date == self.today and t.status == 'failed']
            if failed_tasks:
                content += "\n失败任务:\n"
                for task in failed_tasks:
                    content += f"- {task.job_id}: {task.result}\n"
            
            content += f"\n查看详情: {OUTPUT_FILE}\n"
            
            sender = EmailSender()
            sender.send(subject, content)
            
            logger.info("监控摘要邮件已发送")
            
        except Exception as e:
            logger.error(f"发送监控摘要邮件失败: {e}")


def main():
    parser = argparse.ArgumentParser(description='任务监控面板')
    parser.add_argument('--refresh', '-r', action='store_true', help='启用自动刷新(60秒)')
    parser.add_argument('--email', '-e', action='store_true', help='发送监控摘要邮件')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("生成监控面板")
    logger.info("=" * 60)
    
    dashboard = MonitoringDashboard()
    dashboard.load_task_states()
    dashboard.load_cache_health()
    
    # 生成并保存面板
    output_path = dashboard.save_dashboard(auto_refresh=args.refresh)
    print(f"\n✅ 监控面板已生成: {output_path}")
    
    # 发送邮件
    if args.email:
        dashboard.send_summary_email()
        print("✅ 监控摘要邮件已发送")
    
    # 输出统计
    stats = dashboard.get_status_stats()
    print(f"\n今日任务统计:")
    print(f"  总数: {stats['total']}")
    print(f"  已完成: {stats['completed']} ✅")
    print(f"  失败: {stats['failed']} ❌")
    print(f"  运行中: {stats['running']} 🔄")
    
    logger.info("监控面板生成完成")


if __name__ == "__main__":
    main()
