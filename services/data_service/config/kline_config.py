#!/usr/bin/env python3
"""
K线数据配置模块

提供K线数据采集相关的配置管理
"""

from pathlib import Path
import yaml


def get_kline_config():
    """
    获取K线配置

    Returns:
        dict: 配置字典
    """
    project_root = Path(__file__).parent.parent.parent.parent
    config_file = project_root / "config" / "datasource.yaml"

    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)

        return yaml_data

    return {}