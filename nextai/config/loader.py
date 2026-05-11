"""配置管理 - 支持 YAML + 环境变量覆盖"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


_CONFIG: Optional[Dict[str, Any]] = None

_CONFIG_PATH = Path(__file__).parent / "settings.yaml"


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _env_override(config: dict, prefix: str = "XCN") -> dict:
    env_mapping = {
        f"{prefix}_API_HOST": ("api", "host"),
        f"{prefix}_API_PORT": ("api", "port"),
        f"{prefix}_API_DEBUG": ("api", "debug"),
        f"{prefix}_DB_HOST": ("database", "host"),
        f"{prefix}_DB_PORT": ("database", "port"),
        f"{prefix}_DB_USER": ("database", "user"),
        f"{prefix}_DB_PASSWORD": ("database", "password"),
        f"{prefix}_DB_NAME": ("database", "database"),
        f"{prefix}_REDIS_HOST": ("redis", "host"),
        f"{prefix}_REDIS_PORT": ("redis", "port"),
        f"{prefix}_REDIS_PASSWORD": ("redis", "password"),
        f"{prefix}_LOG_LEVEL": ("logging", "level"),
        f"{prefix}_DATA_DIR": ("data", "kline_dir"),
    }

    overrides = {}
    for env_key, path in env_mapping.items():
        value = os.environ.get(env_key)
        if value is not None:
            section, key = path
            if section not in overrides:
                overrides[section] = {}
            int_keys = {"port", "pool_size", "pool_recycle", "db"}
            if key in int_keys:
                value = int(value)
            elif key == "debug":
                value = value.lower() in ("true", "1", "yes")
            overrides[section][key] = value

    return _deep_merge(config, overrides)


def load_config(config_path: Optional[str] = None, reload: bool = False) -> Dict[str, Any]:
    global _CONFIG

    if _CONFIG is not None and not reload:
        return _CONFIG

    path = Path(config_path) if config_path else _CONFIG_PATH

    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    config = _env_override(config)
    _CONFIG = config
    return _CONFIG


def get_config(key: str, default: Any = None) -> Any:
    config = load_config()
    keys = key.split(".")
    value = config
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k)
        else:
            return default
        if value is None:
            return default
    return value
