#!/usr/bin/env python3
"""
数据采集器模块

包含三种采集场景的实现：
- historical: 历史数据采集
- realtime: 实时数据采集
- intraday: 盘中数据采集
"""

from .historical_collector import HistoricalCollector
from .realtime_collector import RealtimeCollector
from .intraday_collector import IntradayCollector

__all__ = [
    'HistoricalCollector',
    'RealtimeCollector',
    'IntradayCollector'
]
