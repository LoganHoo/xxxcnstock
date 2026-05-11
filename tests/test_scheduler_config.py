"""Tests for scheduler config loading and validation."""

import pytest
from pathlib import Path
import yaml
import tempfile
import os

from services.data_service.scheduler.task_config import (
    load_config,
    SchedulerConfig,
    TaskConfig,
    CircuitBreakerConfig,
    ProgressCheckConfig,
)


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Create a temp directory for config files."""
    return tmp_path


def _write_yaml(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False)


def test_load_config_success(tmp_config_dir):
    """Config loads valid YAML and returns typed dataclasses."""
    config_path = tmp_config_dir / "scheduler.yaml"
    _write_yaml(config_path, {
        "scheduler": {
            "timezone": "Asia/Shanghai",
            "max_workers": 4,
            "health_check_port": 8080,
        },
        "tasks": [
            {
                "name": "task_a",
                "script": "scripts/a.py",
                "schedule": "0 16 * * 1-5",
                "timeout": 600,
                "enabled": True,
            },
            {
                "name": "task_b",
                "script": "scripts/b.py",
                "schedule": "30 8 * * 1-5",
                "timeout": 300,
                "enabled": True,
            },
        ],
    })

    sched_cfg, tasks = load_config(config_path)

    assert isinstance(sched_cfg, SchedulerConfig)
    assert sched_cfg.timezone == "Asia/Shanghai"
    assert sched_cfg.max_workers == 4
    assert len(tasks) == 2
    assert all(isinstance(t, TaskConfig) for t in tasks)
    assert tasks[0].name == "task_a"
    assert tasks[1].name == "task_b"


def test_load_config_missing_field(tmp_config_dir):
    """Config raises ValueError when a task is missing a required field."""
    config_path = tmp_config_dir / "scheduler.yaml"
    _write_yaml(config_path, {
        "scheduler": {},
        "tasks": [
            {
                "name": "bad_task",
                "script": "scripts/a.py",
                # missing "schedule"
            },
        ],
    })

    with pytest.raises(ValueError, match="missing 'schedule'"):
        load_config(config_path)


def test_load_config_filters_disabled(tmp_config_dir):
    """Disabled tasks are excluded from the returned list."""
    config_path = tmp_config_dir / "scheduler.yaml"
    _write_yaml(config_path, {
        "scheduler": {},
        "tasks": [
            {
                "name": "enabled_task",
                "script": "scripts/a.py",
                "schedule": "0 * * * *",
                "enabled": True,
            },
            {
                "name": "disabled_task",
                "script": "scripts/b.py",
                "schedule": "0 * * * *",
                "enabled": False,
            },
        ],
    })

    _, tasks = load_config(config_path)
    assert len(tasks) == 1
    assert tasks[0].name == "enabled_task"


def test_load_config_global_settings(tmp_config_dir):
    """SchedulerConfig fields are populated from the scheduler: section."""
    config_path = tmp_config_dir / "scheduler.yaml"
    _write_yaml(config_path, {
        "scheduler": {
            "name": "Test Scheduler",
            "timezone": "UTC",
            "max_workers": 8,
            "health_check_port": 9090,
            "api_port": 6001,
            "use_redis_lock": False,
            "redis_lock_timeout": 300,
            "max_retries": 5,
            "retry_delay": 30,
            "retry_backoff": 3,
            "max_backoff": 120,
            "market_calendar": {
                "enabled": True,
                "source": "exchange",
                "special_holidays": ["2026-01-01"],
            },
        },
        "tasks": [],
    })

    sched_cfg, tasks = load_config(config_path)

    assert sched_cfg.name == "Test Scheduler"
    assert sched_cfg.timezone == "UTC"
    assert sched_cfg.max_workers == 8
    assert sched_cfg.health_check_port == 9090
    assert sched_cfg.api_port == 6001
    assert sched_cfg.use_redis_lock is False
    assert sched_cfg.redis_lock_timeout == 300
    assert sched_cfg.max_retries == 5
    assert sched_cfg.retry_delay == 30
    assert sched_cfg.retry_backoff == 3
    assert sched_cfg.max_backoff == 120
    assert sched_cfg.market_calendar.enabled is True
    assert sched_cfg.market_calendar.source == "exchange"
    assert "2026-01-01" in sched_cfg.market_calendar.special_holidays
    assert len(tasks) == 0


def test_load_config_with_optional_fields(tmp_config_dir):
    """Tasks with circuit_breaker, progress_check, env, args parse correctly."""
    config_path = tmp_config_dir / "scheduler.yaml"
    _write_yaml(config_path, {
        "scheduler": {},
        "tasks": [
            {
                "name": "full_task",
                "script": "scripts/full.py",
                "schedule": "0 16 * * 1-5",
                "args": ["--mode", "fast"],
                "env": {"FOO": "bar"},
                "circuit_breaker": {
                    "enabled": True,
                    "failure_threshold": 5,
                    "recovery_timeout": 7200,
                    "fallback_script": "scripts/fallback.py",
                },
                "progress_check": {
                    "enabled": True,
                    "interval": 300,
                    "min_progress": 15.0,
                },
            },
        ],
    })

    _, tasks = load_config(config_path)
    assert len(tasks) == 1
    t = tasks[0]
    assert t.args == ["--mode", "fast"]
    assert t.env == {"FOO": "bar"}
    assert isinstance(t.circuit_breaker, CircuitBreakerConfig)
    assert t.circuit_breaker.enabled is True
    assert t.circuit_breaker.failure_threshold == 5
    assert t.circuit_breaker.recovery_timeout == 7200
    assert isinstance(t.progress_check, ProgressCheckConfig)
    assert t.progress_check.enabled is True
    assert t.progress_check.interval == 300
    assert t.progress_check.min_progress == 15.0


def test_load_real_config():
    """Load the actual project config/scheduler.yaml and verify all tasks."""
    config_path = Path("config/scheduler.yaml")
    if not config_path.exists():
        pytest.skip("config/scheduler.yaml not found")

    sched_cfg, tasks = load_config(config_path)

    assert sched_cfg.timezone == "Asia/Shanghai"
    assert len(tasks) > 30, f"Expected 30+ tasks, got {len(tasks)}"

    # Verify all tasks have required fields
    for task in tasks:
        assert task.name, "Task missing name"
        assert task.script, f"Task '{task.name}' missing script"
        assert task.schedule, f"Task '{task.name}' missing schedule"
        assert task.timeout > 0, f"Task '{task.name}' has non-positive timeout"
