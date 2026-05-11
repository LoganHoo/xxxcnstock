"""健康检查 API"""
import logging
from datetime import datetime
from typing import Dict, Optional
from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)


class HealthCheckAPI:
    """Flask 健康检查 API"""

    def __init__(self, scheduler_service, redis_client=None):
        """
        初始化 API

        Args:
            scheduler_service: SchedulerService 实例
            redis_client: Redis 客户端实例
        """
        self.app = Flask(__name__)
        self.scheduler = scheduler_service
        self.redis = redis_client
        self._running_tasks: Dict[str, Dict] = {}
        self._register_routes()

    def _register_routes(self):
        """注册路由"""

        @self.app.route("/health")
        def health():
            """健康检查端点"""
            return jsonify({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "scheduler_running": self.scheduler.scheduler.running if self.scheduler else False,
                "running_tasks": list(self._running_tasks.keys())
            })

        @self.app.route("/tasks")
        def list_tasks():
            """获取任务列表"""
            jobs = self.scheduler.get_jobs() if self.scheduler else []
            return jsonify({
                "tasks": jobs,
                "total": len(jobs)
            })

        @self.app.route("/tasks/<task_name>")
        def task_status(task_name: str):
            """获取单个任务状态"""
            task = self.scheduler.get_task(task_name) if self.scheduler else None
            if not task:
                return jsonify({"error": "Task not found"}), 404

            jobs = self.scheduler.get_jobs()
            job_info = next((j for j in jobs if j["name"] == task_name), None)

            return jsonify({
                "name": task_name,
                "script": task.script,
                "schedule": task.schedule,
                "timeout": task.timeout,
                "enabled": task.enabled,
                "next_run": job_info.get("next_run") if job_info else None,
                "pending": job_info.get("pending", False) if job_info else False
            })

        @self.app.route("/tasks/<task_name>/run", methods=["POST"])
        def run_task(task_name: str):
            """手动触发任务"""
            task = self.scheduler.get_task(task_name) if self.scheduler else None
            if not task:
                return jsonify({"error": "Task not found"}), 404

            if task_name in self._running_tasks:
                return jsonify({"error": "Task is already running"}), 409

            try:
                from .tasks.executor import TaskExecutor
                executor = TaskExecutor(redis_client=self.redis)
                result = executor.execute_with_lock(
                    task_name, task.script,
                    lock_timeout=task.timeout,
                    task_timeout=task.timeout
                )
                return jsonify({
                    "success": result.success,
                    "returncode": result.returncode,
                    "duration": result.duration,
                    "stderr": result.stderr[:500] if result.stderr else None
                })
            except Exception as e:
                logger.error(f"手动执行任务失败 {task_name}: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route("/lock/<lock_key>")
        def lock_status(lock_key: str):
            """检查锁状态"""
            if not self.redis:
                return jsonify({"error": "Redis not configured"}), 400

            from .locks.redis_lock import RedisLockManager
            lm = RedisLockManager(self.redis)
            is_locked = lm.is_locked(f"scheduler:{lock_key}")
            ttl = lm.get_lock_ttl(f"scheduler:{lock_key}") if is_locked else -2

            return jsonify({
                "lock_key": lock_key,
                "is_locked": is_locked,
                "ttl": ttl
            })

    def run(self, host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
        """启动 API 服务器"""
        logger.info(f"启动 API 服务: {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)
