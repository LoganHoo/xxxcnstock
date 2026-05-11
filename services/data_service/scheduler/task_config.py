"""
Unified scheduler configuration loader.

Loads config/scheduler.yaml, validates task definitions, and returns typed dataclasses.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass
class ProgressCheckConfig:
    enabled: bool = False
    interval: int = 600
    min_progress: float = 0.0


@dataclass
class CircuitBreakerConfig:
    enabled: bool = False
    failure_threshold: int = 3
    recovery_timeout: int = 3600
    fallback_script: str = ""


@dataclass
class TaskConfig:
    name: str
    script: str
    schedule: str
    description: str = ""
    enabled: bool = True
    timeout: int = 600
    day_type: Optional[str] = None
    requires_lock: bool = False
    lock_key: Optional[str] = None
    depends_on: Optional[str] = None
    run_once: bool = False
    alert_on_failure: bool = False
    priority: str = "normal"
    optional: bool = False
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    progress_check: ProgressCheckConfig = field(default_factory=ProgressCheckConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)


@dataclass
class MarketCalendarConfig:
    enabled: bool = True
    source: str = "tushare"
    fallback_weekend: bool = True
    special_holidays: list[str] = field(default_factory=list)


@dataclass
class SchedulerConfig:
    name: str = "XCNStock APScheduler"
    timezone: str = "Asia/Shanghai"
    max_workers: int = 4
    health_check_port: int = 8080
    api_port: int = 5001
    use_redis_lock: bool = True
    redis_lock_timeout: int = 1800
    max_retries: int = 3
    retry_delay: int = 60
    retry_backoff: int = 2
    max_backoff: int = 600
    market_calendar: MarketCalendarConfig = field(default_factory=MarketCalendarConfig)


def _parse_progress_check(raw: dict[str, Any] | None) -> ProgressCheckConfig:
    if not raw:
        return ProgressCheckConfig()
    return ProgressCheckConfig(
        enabled=raw.get("enabled", False),
        interval=int(raw.get("interval", 600)),
        min_progress=float(raw.get("min_progress", 0.0)),
    )


def _parse_circuit_breaker(raw: dict[str, Any] | None) -> CircuitBreakerConfig:
    if not raw:
        return CircuitBreakerConfig()
    return CircuitBreakerConfig(
        enabled=raw.get("enabled", False),
        failure_threshold=int(raw.get("failure_threshold", 3)),
        recovery_timeout=int(raw.get("recovery_timeout", 3600)),
        fallback_script=raw.get("fallback_script", ""),
    )


def _parse_task(raw: dict[str, Any]) -> TaskConfig:
    name = raw.get("name", "")
    for required_field in ("name", "script", "schedule"):
        if not raw.get(required_field):
            raise ValueError(f"Task '{name}': missing '{required_field}'")

    return TaskConfig(
        name=name,
        script=raw["script"],
        schedule=raw["schedule"],
        description=raw.get("description", ""),
        enabled=raw.get("enabled", True),
        timeout=int(raw.get("timeout", 600)),
        day_type=raw.get("day_type"),
        requires_lock=raw.get("requires_lock", False),
        lock_key=raw.get("lock_key"),
        depends_on=raw.get("depends_on"),
        run_once=raw.get("run_once", False),
        alert_on_failure=raw.get("alert_on_failure", False),
        priority=raw.get("priority", "normal"),
        optional=raw.get("optional", False),
        args=raw.get("args", []),
        env=raw.get("env", {}),
        progress_check=_parse_progress_check(raw.get("progress_check")),
        circuit_breaker=_parse_circuit_breaker(raw.get("circuit_breaker")),
    )


def _parse_scheduler_config(raw: dict[str, Any]) -> SchedulerConfig:
    cal_raw = raw.get("market_calendar", {})
    return SchedulerConfig(
        name=raw.get("name", "XCNStock APScheduler"),
        timezone=raw.get("timezone", "Asia/Shanghai"),
        max_workers=int(raw.get("max_workers", 4)),
        health_check_port=int(raw.get("health_check_port", 8080)),
        api_port=int(raw.get("api_port", 5001)),
        use_redis_lock=raw.get("use_redis_lock", True),
        redis_lock_timeout=int(raw.get("redis_lock_timeout", 1800)),
        max_retries=int(raw.get("max_retries", 3)),
        retry_delay=int(raw.get("retry_delay", 60)),
        retry_backoff=int(raw.get("retry_backoff", 2)),
        max_backoff=int(raw.get("max_backoff", 600)),
        market_calendar=MarketCalendarConfig(
            enabled=cal_raw.get("enabled", True),
            source=cal_raw.get("source", "tushare"),
            fallback_weekend=cal_raw.get("fallback_weekend", True),
            special_holidays=cal_raw.get("special_holidays", []),
        ),
    )


def load_config(
    config_path: Path = Path("config/scheduler.yaml"),
) -> tuple[SchedulerConfig, list[TaskConfig]]:
    """Load and validate scheduler configuration.

    Returns (SchedulerConfig, list of enabled TaskConfig).
    Disabled tasks are filtered out.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data:
        raise ValueError(f"Empty config file: {config_path}")

    scheduler_config = _parse_scheduler_config(data.get("scheduler", {}))

    tasks: list[TaskConfig] = []
    for raw_task in data.get("tasks", []):
        task = _parse_task(raw_task)
        if task.enabled:
            tasks.append(task)

    return scheduler_config, tasks
