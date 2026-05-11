#!/usr/bin/env python3
"""
异步工具模块

提供同步运行异步代码的实用函数
"""

import asyncio
from typing import Any, Coroutine, TypeVar


T = TypeVar('T')


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """
    在同步上下文中运行异步协程

    Args:
        coro: 异步协程对象

    Returns:
        协程的返回值

    Example:
        async def fetch_data():
            return await api.get_data()

        result = run_async(fetch_data())
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.run(coro)

    future = asyncio.ensure_future(coro)

    return asyncio.run(future)
