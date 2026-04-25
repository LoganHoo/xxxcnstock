#!/usr/bin/env python3
"""
数据源模块
"""
from .manager import DataSourceManager

# 全局数据源管理器实例
_datasource_manager = None


def get_datasource_manager(config=None):
    """获取数据源管理器实例 (单例模式)"""
    global _datasource_manager
    if _datasource_manager is None:
        _datasource_manager = DataSourceManager(config)
    return _datasource_manager
