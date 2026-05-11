"""任务执行器"""
import time
import json
import logging
import subprocess
import signal
import os
from typing import Dict, Optional, Callable, List
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    """任务执行结果"""
    success: bool
    returncode: int
    stdout: str
    stderr: str
    duration: float


class ExecutionHistory:
    """执行历史记录"""
    
    def __init__(self, history_dir: str = "/app/logs/scheduler"):
        self.history_dir = history_dir
        os.makedirs(history_dir, exist_ok=True)
        self.history_file = os.path.join(history_dir, "execution_history.json")
    
    def add(self, task_name: str, result: TaskResult):
        """添加执行记录"""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "task_name": task_name,
            "success": result.success,
            "returncode": result.returncode,
            "duration": result.duration,
            "stderr": result.stderr[:1000] if result.stderr else ""
        }
        
        history = self.load()
        history.insert(0, entry)
        history = history[:1000]
        
        with open(self.history_file, 'w') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    
    def load(self) -> List[Dict]:
        """加载执行历史"""
        if not os.path.exists(self.history_file):
            return []
        try:
            with open(self.history_file, 'r') as f:
                return json.load(f)
        except Exception:
            return []
    
    def get_recent(self, limit: int = 50) -> List[Dict]:
        """获取最近执行记录"""
        return self.load()[:limit]


class TaskExecutor:
    """统一任务执行器，支持分布式锁和超时控制"""

    def __init__(self, redis_client=None, default_timeout: int = 3600, history_dir: str = None):
        """
        初始化任务执行器

        Args:
            redis_client: Redis 客户端实例（用于分布式锁）
            default_timeout: 默认超时时间(秒)
            history_dir: 执行历史保存目录
        """
        import os
        self.redis = redis_client
        self.default_timeout = default_timeout
        self._running_tasks: Dict[str, subprocess.Popen] = {}
        default_history = os.environ.get("LOG_DIR", "/app/logs")
        history_path = history_dir or os.path.join(default_history, "scheduler")
        self.history = ExecutionHistory(history_path)

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
        start_time = time.time()

        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                timeout=timeout,
                cwd=cwd,
                env=env
            )
            duration = time.time() - start_time

            return TaskResult(
                success=result.returncode == 0,
                returncode=result.returncode,
                stdout=result.stdout.decode('utf-8', errors='replace'),
                stderr=result.stderr.decode('utf-8', errors='replace'),
                duration=duration
            )
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            logger.error(f"任务执行超时 {script_path}: {timeout}s")
            return TaskResult(
                success=False,
                returncode=-1,
                stdout="",
                stderr=f"Timeout after {timeout}s",
                duration=duration
            )
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"任务执行失败 {script_path}: {e}")
            return TaskResult(
                success=False,
                returncode=-2,
                stdout="",
                stderr=str(e),
                duration=duration
            )

    def execute_with_lock(self, task_name: str, script_path: str,
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
        from services.data_service.scheduler.locks.redis_lock import RedisLockManager
        lm = lock_manager or (RedisLockManager(self.redis) if self.redis else None)

        if not lm:
            result = self.execute(script_path, task_timeout)
            self.history.add(task_name, result)
            return result

        lock_key = f"scheduler:{task_name}"
        if not lm.acquire(lock_key, timeout=lock_timeout, blocking=False):
            logger.warning(f"任务 {task_name} 已在运行，跳过")
            return TaskResult(False, -3, "", f"Task {task_name} is already locked", 0)

        try:
            result = self.execute(script_path, task_timeout)
            self.history.add(task_name, result)
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
