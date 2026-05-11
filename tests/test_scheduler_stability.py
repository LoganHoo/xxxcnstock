"""Tests for scheduler stability: crash isolation, subprocess timeout,
graceful shutdown, health endpoint, and task loading."""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import pytest
import yaml

from services.data_service.scheduler.engine import SchedulerEngine
from services.data_service.scheduler.task_config import load_config


def _write_test_config(path: Path, tasks: list[dict], port: int = 0):
    """Write a minimal test config YAML."""
    data = {
        "scheduler": {
            "timezone": "UTC",
            "max_workers": 2,
            "health_check_port": port,
        },
        "tasks": tasks,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


class TestTaskCrashIsolation:
    """Scheduler stays alive when a task crashes."""

    def test_crash_isolation(self, tmp_path):
        """A crashing task does not kill the scheduler."""
        config_path = tmp_path / "test_config.yaml"
        _write_test_config(
            config_path,
            tasks=[
                {
                    "name": "crasher",
                    "script": "-c",
                    "schedule": "*/1 * * * *",  # every minute
                    "enabled": True,
                    "timeout": 10,
                },
            ],
            port=0,
        )

        engine = SchedulerEngine(config_path, use_background=True)
        registered = engine.register_tasks()

        # Override the job callback to a crashing one
        engine.scheduler.remove_all_jobs()

        def crash_job():
            raise RuntimeError("Intentional crash for testing")

        from apscheduler.triggers.date import DateTrigger
        from datetime import datetime, timedelta

        trigger = DateTrigger(run_date=datetime.now() + timedelta(seconds=1))
        engine.scheduler.add_job(
            crash_job,
            trigger=trigger,
            id="crasher",
            max_instances=1,
        )

        engine._setup_event_listeners()
        engine._start_time = time.time()
        engine.scheduler.start()

        # Wait for the job to fire
        time.sleep(3)

        # Scheduler should still be running
        assert engine.scheduler.running, "Scheduler died after task crash"

        engine.scheduler.shutdown(wait=True)


class TestSubprocessTimeout:
    """Subprocess timeout kills child process with no zombies."""

    def test_timeout_kills_process(self, tmp_path):
        """A long-running subprocess is killed after timeout."""
        from services.data_service.scheduler.executor import run_subprocess

        with pytest.raises(subprocess.TimeoutExpired):
            run_subprocess(
                cmd=[sys.executable, "-c", "import time; time.sleep(60)"],
                timeout=2,
                cwd=str(tmp_path),
            )

    def test_no_zombie_after_timeout(self, tmp_path):
        """After timeout, child process is gone (no zombie)."""
        from services.data_service.scheduler.executor import run_subprocess

        # Create a script that writes its PID
        pid_file = tmp_path / "child.pid"
        script = f"import os, time; open('{pid_file}', 'w').write(str(os.getpid())); time.sleep(60)"

        with pytest.raises(subprocess.TimeoutExpired):
            run_subprocess(
                cmd=[sys.executable, "-c", script],
                timeout=2,
                cwd=str(tmp_path),
            )

        # Check the child process is gone
        time.sleep(0.5)
        if pid_file.exists():
            child_pid = int(pid_file.read_text().strip())
            try:
                os.kill(child_pid, 0)
                pytest.fail(f"Child process {child_pid} still alive (zombie)")
            except ProcessLookupError:
                pass  # Expected -- process is gone


class TestGracefulShutdown:
    """SIGTERM causes graceful shutdown."""

    def test_sigterm_shutdown(self, tmp_path):
        """Sending SIGTERM shuts down the scheduler cleanly."""
        config_path = tmp_path / "test_config.yaml"
        _write_test_config(
            config_path,
            tasks=[
                {
                    "name": "noop",
                    "script": "-c",
                    "schedule": "0 0 1 1 *",
                    "enabled": True,
                    "timeout": 10,
                },
            ],
            port=0,
        )

        engine = SchedulerEngine(config_path, use_background=True)
        engine._start_time = time.time()
        engine._setup_signal_handlers()
        engine.scheduler.start()

        assert engine.scheduler.running

        # Send SIGTERM
        os.kill(os.getpid(), signal.SIGTERM)

        # Wait for shutdown
        deadline = time.time() + 5
        while engine.scheduler.running and time.time() < deadline:
            time.sleep(0.1)

        assert not engine.scheduler.running, "Scheduler did not shut down after SIGTERM"


class TestHealthEndpoint:
    """Health endpoint returns alive status."""

    def test_health_endpoint(self, tmp_path):
        """GET /health returns JSON with status=alive."""
        port = _find_free_port()
        config_path = tmp_path / "test_config.yaml"
        _write_test_config(
            config_path,
            tasks=[
                {
                    "name": "noop",
                    "script": "-c",
                    "schedule": "0 0 1 1 *",
                    "enabled": True,
                    "timeout": 10,
                },
            ],
            port=port,
        )

        engine = SchedulerEngine(config_path, use_background=True)
        engine._start_time = time.time()
        engine._start_health_server()
        engine.scheduler.start()

        try:
            # Wait for server to be ready
            time.sleep(1)

            resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/health")
            data = json.loads(resp.read().decode())

            assert data["status"] == "alive"
            assert "uptime_seconds" in data
            assert data["uptime_seconds"] >= 0
            assert "tasks_registered" in data
        finally:
            engine.scheduler.shutdown(wait=False)
            if engine._health_server:
                engine._health_server.shutdown()


class TestEngineLoadsAllEnabledTasks:
    """Engine loads all enabled tasks from the real config."""

    def test_loads_all_tasks(self):
        """Load real config and verify all enabled tasks are registered."""
        config_path = Path("config/scheduler.yaml")
        if not config_path.exists():
            pytest.skip("config/scheduler.yaml not found")

        engine = SchedulerEngine(config_path, use_background=True)
        registered = engine.register_tasks()

        jobs = engine.scheduler.get_jobs()
        assert len(jobs) == len(engine.config_tasks)
        assert len(jobs) > 30

        job_ids = {j.id for j in jobs}
        for task in engine.config_tasks:
            assert task.name in job_ids, f"Task '{task.name}' not registered"

        if engine.scheduler.running:
            engine.scheduler.shutdown(wait=False)


def _find_free_port() -> int:
    """Find a free TCP port for testing."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
