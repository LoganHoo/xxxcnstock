#!/usr/bin/env python3
"""
XCNStock 统一调度器
从 config/scheduler.yaml 读取配置执行任务
"""
import sys
import os
import yaml
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def load_config():
    """加载调度器配置"""
    config_path = project_root / 'config' / 'scheduler.yaml'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置失败: {e}")
        return None


def run_task(task_config):
    """执行单个任务"""
    name = task_config['name']
    script = task_config['script']
    
    print(f"[{datetime.now()}] ▶️ 开始执行: {name}")
    
    try:
        script_path = project_root / script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=task_config.get('timeout', 600),
            cwd=str(project_root)
        )
        
        if result.returncode == 0:
            print(f"[{datetime.now()}] ✅ 完成: {name}")
            return True
        else:
            print(f"[{datetime.now()}] ❌ 失败: {name}")
            print(f"   错误: {result.stderr[:500]}")
            return False
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 异常: {name} - {e}")
        return False


def main():
    """主函数"""
    config = load_config()
    if not config:
        return 1
    
    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    
    for task in config.get('tasks', []):
        if not task.get('enabled', True):
            continue
            
        name = task['name']
        schedule = task['schedule']
        
        try:
            trigger = CronTrigger.from_crontab(schedule)
            scheduler.add_job(
                run_task,
                trigger=trigger,
                args=[task],
                id=name,
                name=task.get('description', name)
            )
            print(f"✅ 已注册任务: {name} ({schedule})")
        except Exception as e:
            print(f"❌ 注册任务失败 {name}: {e}")
    
    print(f"\n调度器已启动，共 {len(scheduler.get_jobs())} 个任务\n")
    scheduler.start()


if __name__ == '__main__':
    sys.exit(main())
