#!/usr/bin/env python3
"""
公告数据获取器模块

提供公告数据的获取和分类功能:
- announcement_fetcher: 公告采集器
- announcement_classifier: 公告分类器
- announcement_analyzer: 公告分析器
"""

from .announcement_fetcher import (
    AnnouncementFetcher,
    AnnouncementData,
    AnnouncementType,
    fetch_announcements,
    fetch_major_events
)

__all__ = [
    'AnnouncementFetcher',
    'AnnouncementData',
    'AnnouncementType',
    'fetch_announcements',
    'fetch_major_events',
]
