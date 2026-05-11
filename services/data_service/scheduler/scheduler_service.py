"""APScheduler 调度服务封装"""
import logging
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore

logger = logging.getLogger(__name__)


@dataclass
class ScheduledTask:
    """调度任务定义"""
    name: str
    script: str
    schedule: str
    timeout: int = 3600
    requires_lock: bool = True
    lock_key: Optional[str] = None
    enabled: bool = True


class SchedulerService:
    """APScheduler 调度服务"""

    def __init__(self, timezone: str = "Asia/Shanghai", max_workers: int = 4):
        """
        初始化调度服务

        Args:
            timezone: 时区
            max_workers: 最大工作线程数
        """
        self.timezone = timezone
        self.scheduler = BackgroundScheduler(
            jobstores={"default": MemoryJobStore()},
            executors={"default": {"type": "threadpool", "max_workers": max_workers}},
            job_defaults={"coalesce": True, "max_instances": 1},
            timezone=timezone
        )
        self._tasks: Dict[str, ScheduledTask] = {}
        self._task_executor = None

    def set_executor(self, executor):
        """设置任务执行器"""
        self._task_executor = executor

    def add_task(self, task: ScheduledTask) -> bool:
        """
        添加调度任务

        Args:
            task: 任务定义

        Returns:
            是否添加成功
        """
        if task.name in self._tasks:
            logger.warning(f"任务已存在: {task.name}")
            return False

        if not task.enabled:
            return False

        try:
            trigger = CronTrigger.from_crontab(task.schedule, timezone=self.timezone)

            def job_wrapper():
                if self._task_executor:
                    if task.requires_lock and task.lock_key:
                        from .tasks.executor import TaskExecutor
                        if isinstance(self._task_executor, TaskExecutor):
                            asyncio_run = self._task_executor.execute_with_lock(
                                task.name, task.script,
                                lock_timeout=task.timeout,
                                task_timeout=task.timeout
                            )
                            return asyncio_run
                    return self._task_executor.execute(task.script, task.timeout)
                else:
                    import subprocess
                    return subprocess.run(["python", task.script], timeout=task.timeout)

            self.scheduler.add_job(
                job_wrapper,
                trigger=trigger,
                id=task.name,
                name=task.name,
                replace_existing=True
            )
            self._tasks[task.name] = task
            logger.info(f"任务已添加: {task.name} ({task.schedule})")
            return True

        except Exception as e:
            logger.error(f"添加任务失败 {task.name}: {e}")
            return False

    def remove_task(self, task_name: str) -> bool:
        """
        移除调度任务

        Args:
            task_name: 任务名称

        Returns:
            是否移除成功
        """
        try:
            self.scheduler.remove_job(task_name)
            del self._tasks[task_name]
            logger.info(f"任务已移除: {task_name}")
            return True
        except Exception as e:
            logger.warning(f"移除任务失败 {task_name}: {e}")
            return False

    def get_jobs(self) -> List[Dict]:
        """
        获取所有调度任务状态

        Returns:
            任务状态列表
        """
        jobs = self.scheduler.get_jobs()
        result = []
        for job in jobs:
            task_info = {
                "name": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "pending": job.pending
            }
            if job.id in self._tasks:
                task_info["schedule"] = self._tasks[job.id].schedule
            result.append(task_info)
        return result

    def get_task(self, task_name: str) -> Optional[ScheduledTask]:
        """获取任务定义"""
        return self._tasks.get(task_name)

    def start(self):
        """启动调度器"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("调度器已启动")

    def shutdown(self, wait: bool = True):
        """关闭调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("调度器已关闭")

    def pause(self):
        """暂停调度器"""
        self.scheduler.pause()
        logger.info("调度器已暂停")

    def resume(self):
        """恢复调度器"""
        self.scheduler.resume()
        logger.info("调度器已恢复")
