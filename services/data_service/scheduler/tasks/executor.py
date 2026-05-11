"""任务执行器"""
import asyncio
import logging
import subprocess
import signal
from typing import Dict, Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    """任务执行结果"""
    success: bool
    returncode: int
    stdout: str
    stderr: str
    duration: float


class TaskExecutor:
    """统一任务执行器，支持分布式锁和超时控制"""

    def __init__(self, redis_client=None, default_timeout: int = 3600):
        """
        初始化任务执行器

        Args:
            redis_client: Redis 客户端实例（用于分布式锁）
            default_timeout: 默认超时时间(秒)
        """
        self.redis = redis_client
        self.default_timeout = default_timeout
        self._running_tasks: Dict[str, subprocess.Popen] = {}

    def execute(self, script_path: str, timeout: Optional[int] = None,
                cwd: Optional[str] = None, env: Optional[Dict] = None) -> TaskResult:
        """
        执行任务脚本

        Args:
            script_path: 脚本路径
            timeout: 超时时间(秒)
            cwd: 工作目录
            env: 环境变量

        Returns:
            TaskResult 执行结果
        """
        timeout = timeout or self.default_timeout
        start_time = asyncio.get_event_loop().time()

        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                timeout=timeout,
                cwd=cwd,
                env=env
            )
            duration = asyncio.get_event_loop().time() - start_time

            return TaskResult(
                success=result.returncode == 0,
                returncode=result.returncode,
                stdout=result.stdout.decode('utf-8', errors='replace'),
                stderr=result.stderr.decode('utf-8', errors='replace'),
                duration=duration
            )
        except subprocess.TimeoutExpired:
            duration = asyncio.get_event_loop().time() - start_time
            logger.error(f"任务执行超时 {script_path}: {timeout}s")
            return TaskResult(
                success=False,
                returncode=-1,
                stdout="",
                stderr=f"Timeout after {timeout}s",
                duration=duration
            )
        except Exception as e:
            duration = asyncio.get_event_loop().time() - start_time
            logger.error(f"任务执行失败 {script_path}: {e}")
            return TaskResult(
                success=False,
                returncode=-2,
                stdout="",
                stderr=str(e),
                duration=duration
            )

    async def execute_with_lock(self, task_name: str, script_path: str,
                                 lock_timeout: int = 7200, task_timeout: Optional[int] = None,
                                 lock_manager=None) -> TaskResult:
        """
        使用分布式锁执行任务

        Args:
            task_name: 任务名称
            script_path: 脚本路径
            lock_timeout: 锁超时时间
            task_timeout: 任务超时时间
            lock_manager: RedisLockManager 实例

        Returns:
            TaskResult 执行结果
        """
        from .redis_lock import RedisLockManager
        lm = lock_manager or (RedisLockManager(self.redis) if self.redis else None)

        if not lm:
            return self.execute(script_path, task_timeout)

        lock_key = f"scheduler:{task_name}"
        if not lm.acquire(lock_key, timeout=lock_timeout, blocking=False):
            logger.warning(f"任务 {task_name} 已在运行，跳过")
            return TaskResult(False, -3, "", f"Task {task_name} is already locked", 0)

        try:
            result = self.execute(script_path, task_timeout)
            return result
        finally:
            lm.release(lock_key)

    def kill(self, task_name: str) -> bool:
        """
        强制终止运行中的任务

        Args:
            task_name: 任务名称

        Returns:
            是否成功终止
        """
        if task_name in self._running_tasks:
            proc = self._running_tasks[task_name]
            proc.send_signal(signal.SIGTERM)
            del self._running_tasks[task_name]
            return True
        return False
