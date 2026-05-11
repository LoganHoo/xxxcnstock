"""配置加载器"""
import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class RedisConfig:
    """Redis 配置"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    socket_timeout: int = 5


@dataclass
class LockConfig:
    """锁配置"""
    redis: RedisConfig = field(default_factory=RedisConfig)
    default_ttl: int = 7200
    auto_renew: bool = True


@dataclass
class TaskConfig:
    """任务配置"""
    name: str
    script: str
    schedule: str
    timeout: int = 3600
    requires_lock: bool = True
    lock_key: Optional[str] = None
    enabled: bool = True
    description: str = ""


@dataclass
class SchedulerConfig:
    """调度器配置"""
    name: str = "XCNStock APScheduler"
    timezone: str = "Asia/Shanghai"
    max_workers: int = 4


@dataclass
class AppConfig:
    """完整应用配置"""
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    tasks: List[TaskConfig] = field(default_factory=list)
    lock: LockConfig = field(default_factory=LockConfig)


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    加载 YAML 配置文件

    Args:
        config_path: 配置文件路径

    Returns:
        配置字典
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def _resolve_env_var(value):
    """解析环境变量引用 ${VAR:-default}"""
    if not isinstance(value, str):
        return value

    import re
    pattern = r'\$\{([^}:]+)(?::-([^}]*))?\}'
    match = re.match(pattern, value)
    if match:
        var_name = match.group(1)
        default = match.group(2) or ''
        return os.environ.get(var_name, default)
    return value


def _get_env_int(config: Dict, key: str, default: int) -> int:
    """获取整数配置，支持环境变量"""
    value = config.get(key, default)
    value = _resolve_env_var(value)
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _get_env_str(config: Dict, key: str, default: str) -> str:
    """获取字符串配置，支持环境变量"""
    value = config.get(key, default)
    value = _resolve_env_var(value)
    return str(value) if value is not None else default


def parse_redis_config(config: Dict) -> RedisConfig:
    """解析 Redis 配置"""
    redis_cfg = config.get('redis', {})
    return RedisConfig(
        host=_get_env_str(redis_cfg, 'host', 'localhost'),
        port=_get_env_int(redis_cfg, 'port', 6379),
        db=redis_cfg.get('db', 0),
        socket_timeout=redis_cfg.get('socket_timeout', 5)
    )


def parse_lock_config(config: Dict) -> LockConfig:
    """解析锁配置"""
    lock_cfg = config.get('lock', {})
    return LockConfig(
        redis=parse_redis_config(lock_cfg),
        default_ttl=lock_cfg.get('default_ttl', 7200),
        auto_renew=lock_cfg.get('auto_renew', True)
    )


def parse_task_config(task: Dict) -> TaskConfig:
    """解析任务配置"""
    return TaskConfig(
        name=task['name'],
        script=task['script'],
        schedule=task['schedule'],
        timeout=task.get('timeout', 3600),
        requires_lock=task.get('requires_lock', True),
        lock_key=task.get('lock_key'),
        enabled=task.get('enabled', True),
        description=task.get('description', '')
    )


def parse_scheduler_config(config: Dict) -> SchedulerConfig:
    """解析调度器配置"""
    sched_cfg = config.get('scheduler', {})
    return SchedulerConfig(
        name=sched_cfg.get('name', 'XCNStock APScheduler'),
        timezone=sched_cfg.get('timezone', 'Asia/Shanghai'),
        max_workers=sched_cfg.get('max_workers', 4)
    )


def load_app_config(config_path: str = None) -> AppConfig:
    """
    加载完整应用配置

    Args:
        config_path: 配置文件路径，默认使用 config/scheduler.yaml

    Returns:
        AppConfig 实例
    """
    if config_path is None:
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / "config" / "scheduler.yaml"

    config = load_yaml_config(str(config_path))

    tasks = [parse_task_config(t) for t in config.get('tasks', [])]

    return AppConfig(
        scheduler=parse_scheduler_config(config),
        tasks=tasks,
        lock=parse_lock_config(config)
    )


def validate_config(config: AppConfig) -> List[str]:
    """
    验证配置有效性

    Args:
        config: 应用配置

    Returns:
        错误列表（空表示有效）
    """
    errors = []

    if not config.scheduler.timezone:
        errors.append("时区不能为空")

    if config.scheduler.max_workers < 1:
        errors.append("max_workers 必须 >= 1")

    task_names = set()
    for task in config.tasks:
        if not task.name:
            errors.append("任务名称不能为空")
        if not task.script:
            errors.append(f"任务 {task.name} 的 script 不能为空")
        if not task.schedule:
            errors.append(f"任务 {task.name} 的 schedule 不能为空")

        if task.name in task_names:
            errors.append(f"任务名称重复: {task.name}")
        task_names.add(task.name)

        if task.requires_lock and not task.lock_key:
            errors.append(f"任务 {task.name} 需要锁但未指定 lock_key")

    return errors
