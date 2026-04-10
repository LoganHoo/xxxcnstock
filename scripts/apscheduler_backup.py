#!/usr/bin/env python3
"""
APScheduler 备份调度器 - 独立容器运行
当 Docker Cron 不可用时，使用 APScheduler 直接执行任务
环境变量: PYTHONUNBUFFERED=1 确保日志实时输出
"""
import sys
import os
import logging
import time
import threading
from datetime import datetime
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 配置日志 - 确保实时输出
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

os.environ['PYTHONUNBUFFERED'] = '1'

HEARTBEAT_FILE = '/app/logs/scheduler_heartbeat'
HEARTBEAT_INTERVAL = 60


def write_heartbeat():
    """写入心跳文件"""
    try:
        with open(HEARTBEAT_FILE, 'w') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        logger.error(f"心跳写入失败: {e}")


def heartbeat_loop():
    """心跳线程"""
    write_heartbeat()
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        write_heartbeat()
        logger.debug(f"心跳: {datetime.now().strftime('%H:%M:%S')}")


class APSchedulerBackup:
    """APScheduler 备份调度器"""

    def __init__(self):
        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        self.setup_listeners()
        self.redis_client = self._init_redis()

    def _init_redis(self):
        """初始化 Redis 连接"""
        try:
            import redis
            redis_host = os.getenv('REDIS_HOST', '49.233.10.199')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_password = os.getenv('REDIS_PASSWORD', '100200')
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                decode_responses=True
            )
            client.ping()
            logger.info("✅ Redis 连接成功")
            return client
        except Exception as e:
            logger.warning(f"⚠️  Redis 连接失败: {e}，任务锁将禁用")
            return None

    def acquire_lock(self, job_id, timeout=600):
        """获取分布式锁"""
        if not self.redis_client:
            return True
        lock_key = f"scheduler:lock:{job_id}"
        try:
            result = self.redis_client.set(lock_key, "1", nx=True, ex=timeout)
            if result:
                logger.info(f"🔐 获得任务锁: {job_id}")
                return True
            else:
                logger.warning(f"⏳ 任务正在执行中: {job_id}")
                return False
        except Exception as e:
            logger.error(f"❌ 获取锁失败: {e}")
            return True

    def release_lock(self, job_id):
        """释放分布式锁"""
        if not self.redis_client:
            return
        lock_key = f"scheduler:lock:{job_id}"
        try:
            self.redis_client.delete(lock_key)
            logger.info(f"🔓 释放任务锁: {job_id}")
        except Exception as e:
            logger.error(f"❌ 释放锁失败: {e}")

    def setup_listeners(self):
        """设置执行监听器"""
        self.scheduler.add_listener(
            self.job_executed,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )

    def job_executed(self, event):
        """任务执行回调"""
        if event.exception:
            logger.error(f"任务 {event.job_id} 执行失败: {event.exception}")
        else:
            logger.info(f"任务 {event.job_id} 执行成功")

    def log_job(self, job_id):
        """记录任务开始"""
        logger.info(f"{'='*60}")
        logger.info(f"▶️  开始执行任务: {job_id}")
        logger.info(f"    时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

    def run_script(self, script_path, job_name, job_id=None, timeout=600):
        """执行脚本"""
        import subprocess

        if job_id and not self.acquire_lock(job_id, timeout):
            logger.warning(f"⏭️  跳过任务（已有实例运行）: {job_name}")
            return False

        self.log_job(job_name)

        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=False,
                timeout=timeout,
                cwd=str(project_root)
            )
            success = result.returncode == 0
            if job_id:
                self.release_lock(job_id)
            return success
        except subprocess.TimeoutExpired:
            logger.error(f"任务超时: {job_name}")
            if job_id:
                self.release_lock(job_id)
            return False
        except Exception as e:
            logger.error(f"任务异常: {job_name}, {e}")
            if job_id:
                self.release_lock(job_id)
            return False

    def add_jobs(self):
        """添加所有任务"""
        jobs = [
            {
                'id': 'morning_data',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/morning_update.py',
                    '晨间数据更新',
                    job_id='morning_data'
                ),
                'cron': '30 8 * * 1-5',
                'name': '晨间更新'
            },
            {
                'id': 'morning_report',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/send_morning.py',
                    '晨间报告',
                    job_id='morning_report'
                ),
                'cron': '45 8 * * 1-5',
                'name': '晨间报告',
                'depends_on': 'morning_data'
            },
            {
                'id': 'fund_behavior_report',
                'func': lambda: self.run_script(
                    project_root / 'scripts/run_fund_behavior_strategy.py',
                    '资金行为报告',
                    job_id='fund_behavior_report'
                ),
                'cron': '9 9 * * 1-5',
                'name': '量化决策报告'
            },
            {
                'id': 'data_fetch',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/data_collect.py',
                    '数据采集',
                    job_id='data_fetch'
                ),
                'cron': '0 16 * * 1-5',
                'name': '数据采集'
            },
            {
                'id': 'scheduled_tasks',
                'func': lambda: self.run_script(
                    project_root / 'scripts/scheduled_tasks.py',
                    '综合定时任务',
                    job_id='scheduled_tasks',
                    timeout=900
                ),
                'cron': '0 15 * * 1-5',
                'name': '综合定时任务'
            },
            {
                'id': 'data_quality_check',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/data_audit.py',
                    '数据质检',
                    job_id='data_quality_check'
                ),
                'cron': '0 17 * * 1-5',
                'name': '数据质检'
            },
            {
                'id': 'market_review',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/daily_review.py',
                    '复盘分析',
                    job_id='market_review'
                ),
                'cron': '30 17 * * 1-5',
                'name': '复盘分析'
            },
            {
                'id': 'review_report',
                'func': lambda: self.run_script(
                    project_root / 'scripts/send_review_report.py',
                    '复盘报告',
                    job_id='review_report'
                ),
                'cron': '0 18 * * 1-5',
                'name': '复盘报告'
            },
            {
                'id': 'picks_review',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/stock_pick.py',
                    '选股复盘',
                    job_id='picks_review'
                ),
                'cron': '45 17 * * 1-5',
                'name': '选股复盘'
            },
            {
                'id': 'review_brief',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/morning_push.py',
                    '复盘快报',
                    job_id='review_brief'
                ),
                'cron': '0 19 * * 1-5',
                'name': '复盘快报'
            },
            {
                'id': 'update_tracking',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/update_tracking.py',
                    '跟踪更新',
                    job_id='update_tracking'
                ),
                'cron': '30 19 * * 1-5',
                'name': '跟踪更新'
            },
            {
                'id': 'precompute',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/precompute.py',
                    '预计算',
                    job_id='precompute'
                ),
                'cron': '0 20 * * 1-5',
                'name': '预计算评分'
            },
            {
                'id': 'night_analysis',
                'func': lambda: self.run_script(
                    project_root / 'scripts/pipeline/night_picks.py',
                    '晚间分析',
                    job_id='night_analysis'
                ),
                'cron': '30 20 * * 1-5',
                'name': '晚间分析'
            },
        ]

        for job in jobs:
            trigger = CronTrigger.from_crontab(job['cron'])
            self.scheduler.add_job(
                job['func'],
                trigger=trigger,
                id=job['id'],
                name=job['name'],
                replace_existing=True
            )
            logger.info(f"✅ 已添加任务: {job['id']} ({job['name']}) - {job['cron']}")

    def start(self):
        """启动调度器"""
        logger.info("=" * 70)
        logger.info("APScheduler 主调度器启动")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        self.add_jobs()

        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        logger.info(f"✅ 心跳线程已启动 (间隔 {HEARTBEAT_INTERVAL}秒)")

        logger.info("\n调度器状态:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.id}: {job.name} ({job.trigger})")

        logger.info("\n" + "=" * 70)
        logger.info("开始监听任务...")
        logger.info("=" * 70)

        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            logger.info("收到中断信号，退出...")
            self.scheduler.shutdown()


if __name__ == '__main__':
    scheduler = APSchedulerBackup()
    scheduler.start()