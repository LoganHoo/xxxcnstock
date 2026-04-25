#!/usr/bin/env python3
"""
XCNStock 统一调度器 (APScheduler)
从 config/scheduler.yaml 读取配置执行任务
支持分布式锁，与 Kestra 双调度器协调
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

from core.distributed_lock import DistributedLock
from core.pipeline_monitor import TaskMetricsCollector
import redis


def load_config():
    """加载调度器配置"""
    config_path = project_root / 'config' / 'scheduler.yaml'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置失败: {e}")
        return None


def check_distributed_lock(lock_key: str = "data_collection") -> bool:
    """
    检查分布式锁状态

    Args:
        lock_key: 锁键名

    Returns:
        bool: 是否可以执行任务（True=可以执行，False=锁被占用）
    """
    try:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True,
            socket_connect_timeout=3
        )

        lock = DistributedLock(
            redis_client=redis_client,
            lock_key=lock_key,
            ttl_seconds=300,
            auto_renew=False
        )

        # 尝试获取锁（非阻塞）
        acquired = lock.acquire(blocking=False)

        if acquired:
            # 立即释放，我们只是检查
            lock.release()
            return True
        else:
            print(f"[{datetime.now()}] 🔒 分布式锁被占用，跳过执行")
            return False

    except redis.ConnectionError:
        print(f"[{datetime.now()}] ⚠️ Redis 不可用，以降级模式执行")
        return True  # Redis 故障时允许执行
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 锁检查异常: {e}，允许执行")
        return True


def run_task(task_config):
    """执行单个任务（带分布式锁检查）"""
    name = task_config['name']
    script = task_config['script']
    requires_lock = task_config.get('requires_lock', False)
    lock_key = task_config.get('lock_key', 'data_collection')

    # 检查是否需要分布式锁
    if requires_lock:
        if not check_distributed_lock(lock_key):
            print(f"[{datetime.now()}] ⏭️ 跳过任务: {name} (锁被占用)")
            return False

    print(f"[{datetime.now()}] ▶️ 开始执行: {name}")

    # 初始化监控
    metrics = TaskMetricsCollector()
    task_metrics = metrics.start_task(name, {'script': script})

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
            metrics.end_task(name, status='success')
            return True
        else:
            print(f"[{datetime.now()}] ❌ 失败: {name}")
            print(f"   错误: {result.stderr[:500]}")
            metrics.end_task(name, status='failed', error=result.stderr[:500])
            return False
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 异常: {name} - {e}")
        metrics.end_task(name, status='failed', error=str(e))
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
