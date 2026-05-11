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

        @self.app.route("/")
        def index():
            """API 索引"""
            return jsonify({
                "name": "XCNStock Scheduler API",
                "version": "1.0",
                "endpoints": [
                    "/health",
                    "/tasks",
                    "/tasks/<task_name>",
                    "/tasks/<task_name>/run",
                    "/lock/<lock_key>"
                ]
            })

        @self.app.route("/tasks/html")
        def tasks_html():
            """HTML 表格视图"""
            jobs = self.scheduler.get_jobs() if self.scheduler else []
            task_list = []
            for job in jobs:
                task_info = {
                    "name": job.get("name", ""),
                    "schedule": job.get("schedule", ""),
                    "next_run": job.get("next_run", ""),
                    "pending": "是" if job.get("pending") else "否"
                }
                if job.get("name"):
                    task = self.scheduler.get_task(job["name"])
                    if task:
                        task_info["script"] = task.script
                        task_info["timeout"] = f"{task.timeout}s"
                        task_info["enabled"] = "是" if task.enabled else "否"
                task_list.append(task_info)

            html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>XCNStock 任务调度</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }
        h1 { color: #333; }
        table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #4a90d9; color: white; }
        tr:hover { background: #f9f9f9; }
        .status-yes { color: #52c41a; }
        .status-no { color: #999; }
        .run-btn { background: #4a90d9; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
        .run-btn:hover { background: #357abd; }
        .refresh { margin-bottom: 20px; }
        .api-info { margin-top: 30px; padding: 20px; background: white; border-radius: 8px; }
        code { background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>📅 XCNStock 任务调度</h1>
    <div class="refresh">
        <button class="run-btn" onclick="location.reload()">🔄 刷新</button>
    </div>
    <table>
        <thead>
            <tr>
                <th>任务名称</th>
                <th>调度规则</th>
                <th>下次执行</th>
                <th>脚本</th>
                <th>超时</th>
                <th>启用</th>
                <th>排队</th>
                <th>操作</th>
            </tr>
        </thead>
        <tbody>
"""

            for t in task_list:
                pending_class = "status-no" if t.get("pending") == "否" else "status-yes"
                html += f"""
            <tr>
                <td><strong>{t.get('name', '')}</strong></td>
                <td><code>{t.get('schedule', '')}</code></td>
                <td>{t.get('next_run', '')}</td>
                <td><small>{t.get('script', '')}</small></td>
                <td>{t.get('timeout', '')}</td>
                <td class="{'status-yes' if t.get('enabled') == '是' else 'status-no'}">{t.get('enabled', '')}</td>
                <td class="{pending_class}">{t.get('pending', '')}</td>
                <td><button class="run-btn" onclick="runTask('{t.get('name', '')}', this)">▶ 执行</button></td>
            </tr>
"""

            html += """
        </tbody>
    </table>
    <div id="result-area" style="margin-top:20px;"></div>
    <div class="api-info">
        <h3>API 接口</h3>
        <p><code>GET /tasks</code> - JSON 任务列表</p>
        <p><code>POST /tasks/&lt;name&gt;/run</code> - 手动触发任务</p>
        <p><code>GET /lock/&lt;key&gt;</code> - 查看锁状态</p>
    </div>
    <script>
    function runTask(name, btn) {
        btn.disabled = true;
        btn.textContent = '执行中...';
        var resultArea = document.getElementById('result-area');
        resultArea.innerHTML = '<div style="padding:10px;background:#e6f7ff;border-radius:4px;">⏳ 正在执行 ' + name + '...</div>';
        
        fetch('/tasks/' + name + '/run', { method: 'POST' })
            .then(r => r.json())
            .then(d => {
                btn.disabled = false;
                btn.textContent = '▶ 执行';
                if (d.success) {
                    resultArea.innerHTML = '<div style="padding:10px;background:#d4edda;border-radius:4px;color:#155724;">✅ ' + name + ' 执行成功 (耗时:' + d.duration.toFixed(2) + 's)</div>';
                } else {
                    resultArea.innerHTML = '<div style="padding:10px;background:#f8d7da;border-radius:4px;color:#721c24;"><b>❌ ' + name + ' 执行失败</b><pre style="margin:5px 0 0 0;white-space:pre-wrap;max-height:200px;overflow:auto;">' + (d.stderr || d.error || '未知错误') + '</pre></div>';
                }
            })
            .catch(e => {
                btn.disabled = false;
                btn.textContent = '▶ 执行';
                resultArea.innerHTML = '<div style="padding:10px;background:#f8d7da;border-radius:4px;color:#721c24;">❌ 错误: ' + e + '</div>';
            });
    }
    </script>
</body>
</html>"""
            return html

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
                from services.data_service.scheduler.tasks.executor import TaskExecutor
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

        @self.app.route("/history")
        def execution_history():
            """获取执行历史"""
            try:
                from services.data_service.scheduler.tasks.executor import TaskExecutor
                executor = TaskExecutor(redis_client=self.redis)
                history = executor.history.get_recent(limit=50)
                return jsonify({"history": history, "total": len(history)})
            except Exception as e:
                logger.error(f"获取执行历史失败: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route("/history/<task_name>")
        def task_history(task_name: str):
            """获取指定任务的执行历史"""
            try:
                from services.data_service.scheduler.tasks.executor import TaskExecutor
                executor = TaskExecutor(redis_client=self.redis)
                all_history = executor.history.get_recent(limit=500)
                filtered = [h for h in all_history if h.get("task_name") == task_name]
                return jsonify({"task": task_name, "history": filtered, "total": len(filtered)})
            except Exception as e:
                logger.error(f"获取任务历史失败 {task_name}: {e}")
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
