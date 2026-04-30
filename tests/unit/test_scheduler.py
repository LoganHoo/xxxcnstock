#!/usr/bin/env python3
"""
调度器单元测试
"""
import io
import json
from datetime import datetime
from pathlib import Path

import pytest

from scripts import scheduler


class DummyHistoryDB:
    """最小历史库桩对象。"""

    def __init__(self):
        self.stats_calls = []

    def get_task_stats(self, task_name: str, days: int = 7):
        self.stats_calls.append((task_name, days))
        return {
            "total": 1,
            "success": 1,
            "failed": 0,
            "timeout": 0,
            "avg_duration_ms": 12.5,
        }


def build_task(**overrides):
    """构造最小任务配置。"""
    task = {
        "name": "demo_task",
        "script": "scripts/scheduler.py",
        "enabled": True,
        "timeout": 60,
        "day_type": "weekday",
        "_global": {
            "max_retries": 0,
            "retry_delay": 1,
            "retry_backoff": 1,
            "use_redis_lock": True,
            "redis_lock_timeout": 30,
            "market_calendar": {
                "enabled": True,
                "special_holidays": ["2026-05-01"],
            },
        },
        "_env_defaults": {"ENV_A": "default"},
    }
    task.update(overrides)
    return task


def test_load_cron_tasks_rejects_duplicate_yaml_keys(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "cron_tasks.yaml").write_text(
        "tasks:\n"
        "  - name: demo\n"
        "    script: a.py\n"
        "    script: b.py\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(scheduler, "project_root", tmp_path)

    with pytest.raises(ValueError, match="重复键"):
        scheduler.load_cron_tasks()


def test_should_run_today_respects_holiday():
    task = build_task()

    result = scheduler.should_run_today(task, now=datetime(2026, 5, 1, 9, 0, 0))

    assert result is False


def test_run_task_skips_lock_when_disabled(monkeypatch):
    task = build_task(_global={**build_task()["_global"], "use_redis_lock": False})
    called = {"lock": False, "subprocess": False}

    class FailIfCalledLock:
        def __init__(self, *_args, **_kwargs):
            called["lock"] = True
            raise AssertionError("不应创建分布式锁")

    monkeypatch.setattr(scheduler, "TaskDistributedLock", FailIfCalledLock)
    monkeypatch.setattr(scheduler, "check_dependencies", lambda _task: True)
    monkeypatch.setattr(scheduler.shutdown_event, "is_set", lambda: False)
    monkeypatch.setattr(scheduler, "should_run_today", lambda _task: True)
    monkeypatch.setattr(scheduler, "get_history_db", lambda: type("DB", (), {
        "record_start": lambda self, _name: 1,
        "record_complete": lambda self, *_args, **_kwargs: None,
    })())

    def fake_subprocess(cmd, timeout, cwd, env=None, task_name=None, progress_config=None, max_output_size=None):
        called["subprocess"] = True
        return 0, "ok", ""

    monkeypatch.setattr(scheduler, "run_subprocess_with_timeout", fake_subprocess)

    assert scheduler.run_task(task) is True
    assert called["lock"] is False
    assert called["subprocess"] is True


def test_build_task_env_merges_defaults_and_task_env(monkeypatch):
    monkeypatch.setenv("ENV_BASE", "base")
    task = build_task(
        _global={**build_task()["_global"], "path": "/tmp/custom-bin"},
        _env_defaults={"ENV_A": "default", "ENV_B": "global"},
        env={"ENV_B": "task", "ENV_C": "local"},
    )

    env = scheduler.build_task_env(task)

    assert env["ENV_BASE"] == "base"
    assert env["ENV_A"] == "default"
    assert env["ENV_B"] == "task"
    assert env["ENV_C"] == "local"
    assert env["PATH"] == "/tmp/custom-bin"


def test_health_handler_stats_initializes_db(monkeypatch):
    dummy_db = DummyHistoryDB()
    payload = {}

    monkeypatch.setattr(scheduler, "load_cron_tasks", lambda: [
        {"name": "task_a"},
        {"name": "task_b"},
    ])
    monkeypatch.setattr(scheduler, "get_history_db", lambda: dummy_db)

    handler = scheduler.HealthHandler.__new__(scheduler.HealthHandler)
    handler.send_response = lambda code: payload.setdefault("status", code)
    handler.send_header = lambda *_args, **_kwargs: None
    handler.end_headers = lambda: None
    handler.wfile = io.BytesIO()

    handler._send_stats_response()

    body = json.loads(handler.wfile.getvalue().decode("utf-8"))
    assert payload["status"] == 200
    assert set(body.keys()) == {"task_a", "task_b"}
    assert dummy_db.stats_calls == [("task_a", 7), ("task_b", 7)]


def test_run_once_skips_after_same_day_success(monkeypatch):
    task = build_task(run_once=True)

    monkeypatch.setattr(scheduler, "has_successful_run_today", lambda name, now=None: True)
    monkeypatch.setattr(scheduler, "check_dependencies", lambda _task: True)
    monkeypatch.setattr(scheduler.shutdown_event, "is_set", lambda: False)
    monkeypatch.setattr(scheduler, "should_run_today", lambda _task: True)

    assert scheduler.run_task(task) is False


def test_circuit_breaker_opens_after_threshold(tmp_path, monkeypatch):
    state_file = tmp_path / "breaker.json"
    monkeypatch.setattr(scheduler, "CIRCUIT_BREAKER_STATE_FILE", state_file)

    task = build_task(
        name="cb_task",
        circuit_breaker={
            "enabled": True,
            "failure_threshold": 3,
            "recovery_timeout": 3600,
        },
    )

    for _ in range(3):
        scheduler.record_circuit_breaker_failure(task, "boom", now=datetime(2026, 4, 30, 10, 0, 0))

    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["cb_task"]["failure_count"] == 3
    assert state["cb_task"]["state"] == "open"


def test_progress_check_detects_stalled_task(tmp_path, monkeypatch):
    progress_dir = tmp_path / "progress"
    progress_dir.mkdir()
    progress_file = progress_dir / "demo_task.json"
    progress_file.write_text(json.dumps({
        "progress": 5,
        "updated_at": "2026-04-30T10:00:00",
        "message": "still running",
    }), encoding="utf-8")

    monkeypatch.setattr(scheduler, "PROGRESS_STATE_DIR", progress_dir)
    task = build_task(
        progress_check={
            "enabled": True,
            "interval": 60,
            "min_progress": 10,
            "alert_on_stall": True,
        }
    )

    result = scheduler.evaluate_task_progress(task, now=datetime(2026, 4, 30, 10, 12, 0))

    assert result["is_stalled"] is True
    assert result["progress"] == 5
    assert "停滞" in result["reason"]


def test_run_task_executes_fallback_when_circuit_breaker_is_open(monkeypatch):
    task = build_task(
        name="fallback_demo",
        circuit_breaker={
            "enabled": True,
            "failure_threshold": 3,
            "recovery_timeout": 3600,
            "fallback_script": "scripts/pipeline/send_report_fallback.py",
        },
    )
    executed = []

    monkeypatch.setattr(
        scheduler,
        "should_skip_by_circuit_breaker",
        lambda task_config: (task_config["name"] == "fallback_demo", "breaker open"),
    )
    monkeypatch.setattr(scheduler, "check_dependencies", lambda _task: True)
    monkeypatch.setattr(scheduler.shutdown_event, "is_set", lambda: False)
    monkeypatch.setattr(scheduler, "should_run_today", lambda _task: True)
    monkeypatch.setattr(scheduler, "has_successful_run_today", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(scheduler, "TaskDistributedLock", lambda *_args, **_kwargs: pytest.fail("fallback 不应走分布式锁构造"))

    def fake_run_subprocess(cmd, timeout, cwd, env=None, task_name=None, progress_config=None, max_output_size=None):
        executed.append({
            "cmd": cmd,
            "task_name": task_name,
            "progress_check": progress_config,
        })
        return 0, "fallback ok", ""

    monkeypatch.setattr(scheduler, "run_subprocess_with_timeout", fake_run_subprocess)
    monkeypatch.setattr(
        scheduler,
        "get_history_db",
        lambda: type("DB", (), {
            "record_start": lambda self, _name: 1,
            "record_complete": lambda self, *_args, **_kwargs: None,
        })(),
    )

    assert scheduler.run_task(task) is True
    assert len(executed) == 1
    assert executed[0]["task_name"] == "fallback_demo__fallback"
    assert executed[0]["cmd"][1].endswith("send_report_fallback.py")
    assert executed[0]["progress_check"] == {}
