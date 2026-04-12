#!/usr/bin/env python3
"""
Cron 备用方案 - 独立任务调度器
当 Docker Cron 不可用时，使用此脚本直接执行任务
使用方法: python3 scripts/backup_scheduler.py
"""
import sys
import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from croniter import croniter

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/backup_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TASKS = [
    {
        'name': 'morning_data',
        'script': 'scripts/pipeline/morning_update.py',
        'schedule': '30 8 * * 1-5',
        'description': '晨间更新',
        'last_run': None
    },
    {
        'name': 'morning_report',
        'script': 'scripts/pipeline/send_morning.py',
        'schedule': '45 8 * * 1-5',
        'description': '晨间报告',
        'depends_on': 'morning_data',
        'last_run': None
    },
    {
        'name': 'fund_behavior_report',
        'script': 'scripts/run_fund_behavior_strategy.py',
        'schedule': '26 9 * * 1-5',
        'description': '资金行为报告',
        'depends_on': 'morning_data',
        'last_run': None
    },
    {
        'name': 'data_fetch',
        'script': 'scripts/pipeline/data_collect.py',
        'schedule': '0 16 * * 1-5',
        'description': '数据采集',
        'last_run': None
    },
    {
        'name': 'data_quality_check',
        'script': 'scripts/pipeline/data_audit.py',
        'schedule': '0 17 * * 1-5',
        'description': '数据质检',
        'depends_on': 'data_fetch',
        'last_run': None
    },
]

class BackupScheduler:
    def __init__(self):
        self.tasks = TASKS
        self.check_interval = 60

    def is_weekday(self):
        """检查是否是交易日"""
        now = datetime.now()
        return now.weekday() < 5

    def should_run(self, task):
        """检查任务是否应该运行"""
        if not self.is_weekday():
            return False

        schedule = task['schedule']
        now = datetime.now()

        try:
            cron = croniter(schedule, now)
            prev_run = cron.get_prev(datetime)
            next_run = cron.get_next(datetime)

            # 如果距离下次运行时间小于2分钟，且上次未运行
            time_diff = (next_run - now).total_seconds()
            if 0 < time_diff < 120:
                if task['last_run'] is None or task['last_run'] < prev_run:
                    return True
        except Exception as e:
            logger.error(f"Cron解析错误 {task['name']}: {e}")

        return False

    def run_task(self, task):
        """执行任务"""
        import subprocess

        logger.info(f"▶️  执行任务: {task['description']}")
        start_time = time.time()

        try:
            script_path = project_root / task['script']
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=task.get('timeout', 600),
                cwd=str(project_root)
            )

            elapsed = time.time() - start_time
            if result.returncode == 0:
                logger.info(f"✅ 任务完成: {task['description']} ({elapsed:.1f}s)")
                task['last_run'] = datetime.now()
                return True
            else:
                logger.error(f"❌ 任务失败: {task['description']}, exit={result.returncode}")
                logger.error(f"   stderr: {result.stderr[:500]}")
                return False
        except subprocess.TimeoutExpired:
            logger.error(f"❌ 任务超时: {task['description']}")
            return False
        except Exception as e:
            logger.error(f"❌ 任务异常: {task['description']}, {e}")
            return False

    def check_cron_health(self):
        """检查 Cron 是否正常运行"""
        try:
            import subprocess
            result = subprocess.run(
                ['docker', 'exec', 'xcnstock-cron', 'cat', '/var/run/crond.pid'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                return False

            pid = result.stdout.strip()
            result = subprocess.run(
                ['docker', 'exec', 'xcnstock-cron', 'cat', f'/proc/{pid}/status'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if 'State:\tS (sleeping)' in result.stdout or 'State:\tR (running)' in result.stdout:
                return True
        except Exception:
            pass
        return False

    def run(self):
        """主循环"""
        logger.info("=" * 60)
        logger.info("备用调度器启动")
        logger.info(f"监控任务数: {len(self.tasks)}")
        logger.info("=" * 60)

        while True:
            try:
                # 检查 Cron 健康状态
                cron_healthy = self.check_cron_health()

                if cron_healthy:
                    logger.debug("Cron 运行正常，等待...")
                else:
                    logger.warning("⚠️  Cron 异常，备用调度器接管")

                # 检查每个任务
                for task in self.tasks:
                    if self.should_run(task):
                        # 检查依赖
                        dep = task.get('depends_on')
                        if dep:
                            dep_task = next((t for t in self.tasks if t['name'] == dep), None)
                            if dep_task and dep_task['last_run'] is None:
                                logger.info(f"跳过 {task['description']}，等待依赖 {dep}")
                                continue

                        self.run_task(task)

                time.sleep(self.check_interval)

            except KeyboardInterrupt:
                logger.info("收到中断信号，退出...")
                break
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                time.sleep(self.check_interval)

if __name__ == '__main__':
    scheduler = BackupScheduler()
    scheduler.run()