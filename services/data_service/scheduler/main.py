"""调度服务入口"""
import os
import sys
import logging
import signal
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger

setup_logger("scheduler", log_file="scheduler/main.log")
logger = logging.getLogger(__name__)

import redis
import yaml
from services.data_service.scheduler.scheduler_service import SchedulerService, ScheduledTask
from services.data_service.scheduler.api.health import HealthCheckAPI
from services.data_service.scheduler.tasks.executor import TaskExecutor
from services.data_service.scheduler.locks.redis_lock import RedisLockManager


def load_config(config_path: str = None):
    """加载调度配置"""
    if config_path is None:
        config_path = project_root / "config" / "scheduler.yaml"

    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    """主入口"""
    logger.info("=" * 50)
    logger.info("XCNStock 调度服务启动")
    logger.info("=" * 50)

    config = load_config()
    scheduler_config = config.get("scheduler", {})
    tasks_config = config.get("tasks", [])
    redis_config = config.get("lock", {}).get("redis", {})

    redis_host = os.environ.get("REDIS_HOST", redis_config.get("host", "localhost"))
    redis_port = int(os.environ.get("REDIS_PORT", redis_config.get("port", 6379)))
    redis_password = os.environ.get("REDIS_PASSWORD", redis_config.get("password", None))

    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, db=0, socket_timeout=5)
        redis_client.ping()
        logger.info(f"Redis 连接成功: {redis_host}:{redis_port}")
    except Exception as e:
        logger.warning(f"Redis 连接失败: {e}，将使用无锁模式")
        redis_client = None

    scheduler_service = SchedulerService(
        timezone=scheduler_config.get("timezone", "Asia/Shanghai"),
        max_workers=scheduler_config.get("max_workers", 4)
    )

    task_executor = TaskExecutor(redis_client=redis_client)
    scheduler_service.set_executor(task_executor)

    lock_manager = RedisLockManager(redis_client) if redis_client else None

    for task_config in tasks_config:
        if not task_config.get("enabled", True):
            continue

        task = ScheduledTask(
            name=task_config["name"],
            script=task_config["script"],
            schedule=task_config["schedule"],
            timeout=task_config.get("timeout", 3600),
            requires_lock=task_config.get("requires_lock", True),
            lock_key=task_config.get("lock_key"),
            enabled=task_config.get("enabled", True)
        )
        scheduler_service.add_task(task)

    api = HealthCheckAPI(scheduler_service, redis_client)

    def signal_handler(signum, frame):
        logger.info("收到退出信号，正在关闭...")
        scheduler_service.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    scheduler_service.start()

    logger.info("调度服务已启动，API 监听 0.0.0.0:5001")
    api.run(host="0.0.0.0", port=5001)


if __name__ == "__main__":
    main()
