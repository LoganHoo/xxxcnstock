"""
SchedulerEngine: BlockingScheduler with subprocess execution,
crash isolation, signal handling, and health endpoint.
"""

import json
import os
import signal
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from services.data_service.scheduler.task_config import (
    SchedulerConfig,
    TaskConfig,
    load_config,
)
from services.data_service.scheduler.executor import run_subprocess


class SchedulerEngine:
    """APScheduler-based scheduler engine with subprocess task execution."""

    def __init__(
        self,
        config_path: Path = Path("config/scheduler.yaml"),
        *,
        use_background: bool = False,
    ):
        """Initialize the scheduler engine.

        Args:
            config_path: Path to scheduler.yaml.
            use_background: Use BackgroundScheduler instead of BlockingScheduler
                           (for testing).
        """
        self._config_path = config_path
        self._scheduler_config: Optional[SchedulerConfig] = None
        self._config_tasks: list[TaskConfig] = []
        self._shutdown_event = threading.Event()
        self._start_time: Optional[float] = None
        self._health_server: Optional[HTTPServer] = None

        # Load config immediately to detect errors early
        self._scheduler_config, self._config_tasks = load_config(config_path)

        scheduler_cls = BackgroundScheduler if use_background else BlockingScheduler
        executor = ThreadPoolExecutor(max_workers=self._scheduler_config.max_workers)

        self._scheduler = scheduler_cls(
            timezone=self._scheduler_config.timezone,
            executors={"default": executor},
        )

    @property
    def scheduler_config(self) -> SchedulerConfig:
        return self._scheduler_config

    @property
    def config_tasks(self) -> list[TaskConfig]:
        return self._config_tasks

    @property
    def scheduler(self):
        return self._scheduler

    @property
    def running(self) -> bool:
        return self._scheduler.running

    def _create_job_callback(self, task: TaskConfig) -> callable:
        """Create a job callback that runs the task script as subprocess."""

        def job_callback():
            try:
                script_path = Path(task.script)
                if script_path.suffix == ".py":
                    cmd = [sys.executable, str(script_path)]
                elif script_path.suffix == ".sh":
                    cmd = ["/bin/bash", str(script_path)]
                else:
                    cmd = [str(script_path)]

                if task.args:
                    cmd.extend(task.args)

                env = os.environ.copy()
                env.update({k: str(v) for k, v in task.env.items()})

                cwd = str(Path.cwd())

                returncode, stdout, stderr = run_subprocess(
                    cmd=cmd,
                    timeout=task.timeout,
                    cwd=cwd,
                    env=env,
                )

                if returncode == 0:
                    logger.info(f"Task '{task.name}' completed successfully")
                else:
                    stderr_excerpt = stderr[:500] if stderr else ""
                    logger.error(
                        f"Task '{task.name}' failed with exit code {returncode}: "
                        f"{stderr_excerpt}"
                    )

            except Exception as e:
                logger.error(f"Task '{task.name}' raised exception: {e}")
                # Swallow exception -- scheduler must stay alive

        return job_callback

    def register_tasks(self) -> int:
        """Register all enabled tasks as cron jobs. Returns count registered."""
        registered = 0
        for task in self._config_tasks:
            try:
                trigger = CronTrigger.from_crontab(task.schedule)
                self._scheduler.add_job(
                    self._create_job_callback(task),
                    trigger=trigger,
                    id=task.name,
                    name=task.description or task.name,
                    replace_existing=True,
                    max_instances=1,
                )
                registered += 1
                logger.info(f"Registered task: {task.name} ({task.schedule})")
            except Exception as e:
                logger.error(f"Failed to register task '{task.name}': {e}")
        return registered

    def _setup_event_listeners(self) -> None:
        """Set up APScheduler event listeners for crash isolation."""

        def on_job_error(event: JobEvent):
            if event.exception:
                logger.error(
                    f"Job '{event.job_id}' error: {event.exception}"
                )

        def on_job_missed(event: JobEvent):
            logger.warning(
                f"Job '{event.job_id}' missed scheduled run at "
                f"{event.scheduled_run_time}"
            )

        self._scheduler.add_listener(on_job_error, EVENT_JOB_ERROR)
        self._scheduler.add_listener(on_job_missed, EVENT_JOB_MISSED)

    def _setup_signal_handlers(self) -> None:
        """Set up SIGTERM/SIGINT for graceful shutdown."""

        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.info(f"Received {sig_name}, initiating graceful shutdown...")
            self._shutdown_event.set()
            # Shutdown in daemon thread to avoid deadlock in signal handler
            threading.Thread(
                target=self._scheduler.shutdown, kwargs={"wait": True}, daemon=True
            ).start()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _start_health_server(self) -> None:
        """Start HTTP health endpoint in a daemon thread."""
        engine = self

        class HealthHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Silence access logs

            def do_GET(self):
                if self.path == "/health":
                    uptime = (
                        time.time() - engine._start_time
                        if engine._start_time
                        else 0
                    )
                    response = {
                        "status": "alive",
                        "uptime_seconds": round(uptime, 1),
                        "tasks_registered": len(engine._config_tasks),
                    }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(response, ensure_ascii=False).encode()
                    )
                else:
                    self.send_error(404)

        port = self._scheduler_config.health_check_port
        try:
            self._health_server = HTTPServer(("0.0.0.0", port), HealthHandler)
            thread = threading.Thread(
                target=self._health_server.serve_forever, daemon=True
            )
            thread.start()
            logger.info(f"Health endpoint started on port {port}")
        except Exception as e:
            logger.warning(f"Health endpoint failed to start: {e}")

    def start(self) -> None:
        """Start the scheduler (blocking)."""
        self._start_time = time.time()

        registered = self.register_tasks()
        self._setup_event_listeners()
        self._setup_signal_handlers()
        self._start_health_server()

        logger.info(f"Scheduler starting with {registered} tasks")
        self._scheduler.start()

    def shutdown(self) -> None:
        """Gracefully shutdown the scheduler."""
        self._shutdown_event.set()
        self._scheduler.shutdown(wait=True)
        if self._health_server:
            self._health_server.shutdown()
        logger.info("Scheduler shut down complete")
