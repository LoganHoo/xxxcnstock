#!/usr/bin/env python3
"""
调度器模块

支持多种调度方式:
1. DolphinScheduler - 分布式工作流调度
2. APScheduler - 本地定时任务调度
3. 手动触发 - 命令行执行
"""

# 延迟导入，避免循环依赖和版本兼容问题
# from .dolphinscheduler_client import DolphinSchedulerClient

__all__ = []
