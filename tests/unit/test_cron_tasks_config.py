#!/usr/bin/env python3
"""
cron_tasks 配置契约测试
"""
from pathlib import Path

import yaml

from scripts import scheduler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "cron_tasks.yaml"
REVIEW_FILE = PROJECT_ROOT / "docs" / "plans" / "2026-04-30-cron-tasks-review.md"


def load_raw_config():
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def test_enabled_tasks_reference_existing_scripts():
    tasks = scheduler.load_cron_tasks()

    for task in tasks:
        if not task.get("enabled", True):
            continue
        script = PROJECT_ROOT / task["script"]
        assert script.exists(), task["name"]


def test_enabled_task_dependencies_resolve_to_real_enabled_tasks():
    tasks = scheduler.load_cron_tasks()
    enabled_task_names = {
        task["name"]
        for task in tasks
        if task.get("enabled", True)
    }

    for task in tasks:
        if not task.get("enabled", True):
            continue
        depends_on = task.get("depends_on")
        if depends_on:
            assert depends_on in enabled_task_names, task["name"]


def test_planned_tasks_are_disabled_and_marked():
    config = load_raw_config()
    planned_tasks = config.get("planned_tasks")

    assert planned_tasks, "planned_tasks"
    for task in planned_tasks:
        assert task["enabled"] is False, task["name"]
        assert task["status"] == "planned", task["name"]


def test_cron_config_keeps_execution_contract_in_tasks_only():
    config = load_raw_config()
    loaded_tasks = scheduler.load_cron_tasks()

    task_names = [task["name"] for task in config["tasks"]]
    loaded_task_names = [task["name"] for task in loaded_tasks]
    planned_task_names = {
        task["name"]
        for task in config.get("planned_tasks", [])
    }

    assert loaded_task_names == task_names
    assert planned_task_names.isdisjoint(loaded_task_names)
    assert all(task.get("enabled", True) for task in config["tasks"])


def test_cron_review_lists_active_and_planned_tasks():
    review = REVIEW_FILE.read_text(encoding="utf-8")

    assert "当前可执行任务" in review
    assert "规划态任务" in review


def test_scheduler_can_load_cron_tasks_config():
    tasks = scheduler.load_cron_tasks()

    assert tasks
