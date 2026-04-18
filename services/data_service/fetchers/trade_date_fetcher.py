#!/usr/bin/env python3
"""
交易日查询模块 - 微服务内部使用

支持BaoStock交易日查询API:
- query_trade_dates: 获取指定日期范围的交易日信息
"""
import asyncio
import importlib
import pandas as pd
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from core.logger import setup_logger

logger = setup_logger("trade_date_fetcher", log_file="system/trade_date_fetcher.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


@dataclass
class TradeDate:
    """交易日信息"""
    date: str           # 日期 (YYYY-MM-DD)
    is_trading_day: bool  # 是否为交易日


class TradeDateFetcher:
    """交易日查询获取器"""

    def __init__(self):
        self._bs = None
        self._logged_in = False

    def _login(self) -> bool:
        """登录BaoStock"""
        if self._logged_in:
            return True

        try:
            bs = _get_baostock_client()
            lg = bs.login()
            if lg.error_code == '0':
                self._bs = bs
                self._logged_in = True
                logger.info("BaoStock登录成功")
                return True
            else:
                logger.error(f"BaoStock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"BaoStock登录异常: {e}")
            return False

    def _logout(self):
        """登出BaoStock"""
        if self._logged_in and self._bs:
            try:
                self._bs.logout()
                logger.info("BaoStock登出成功")
            except:
                pass
            finally:
                self._logged_in = False
                self._bs = None

    async def fetch_trade_dates(self, start_date: str, end_date: str) -> List[TradeDate]:
        """
        获取指定日期范围的交易日信息

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            交易日列表
        """
        if not self._login():
            return []

        try:
            rs = self._bs.query_trade_dates(start_date=start_date, end_date=end_date)

            if rs.error_code != '0':
                logger.error(f"获取交易日信息失败: {rs.error_msg}")
                return []

            trade_dates = []
            while rs.next():
                data = rs.get_row_data()
                trade_dates.append(TradeDate(
                    date=data[0],
                    is_trading_day=data[1] == '1'
                ))

            logger.info(f"获取交易日信息: {len(trade_dates)} 天")
            return trade_dates

        except Exception as e:
            logger.error(f"获取交易日信息异常: {e}")
            return []

    async def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定范围内的所有交易日

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日期字符串列表 (YYYY-MM-DD)
        """
        trade_dates = await self.fetch_trade_dates(start_date, end_date)
        return [td.date for td in trade_dates if td.is_trading_day]

    async def get_last_trading_day(self, date: str = None) -> Optional[str]:
        """
        获取指定日期的最近一个交易日

        Args:
            date: 查询日期，默认今天

        Returns:
            最近交易日 (YYYY-MM-DD)
        """
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        # 查询前后30天
        query_date = datetime.strptime(date, '%Y-%m-%d')
        start_date = (query_date - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = (query_date + timedelta(days=30)).strftime('%Y-%m-%d')

        trade_dates = await self.fetch_trade_dates(start_date, end_date)
        trading_days = [td for td in trade_dates if td.is_trading_day]

        # 找到最近的交易日
        query_dt = datetime.strptime(date, '%Y-%m-%d')
        closest = None
        min_diff = float('inf')

        for td in trading_days:
            td_dt = datetime.strptime(td.date, '%Y-%m-%d')
            diff = abs((td_dt - query_dt).days)
            if diff < min_diff:
                min_diff = diff
                closest = td.date

        return closest

    async def is_trading_day(self, date: str) -> bool:
        """
        判断指定日期是否为交易日

        Args:
            date: 查询日期 (YYYY-MM-DD)

        Returns:
            是否为交易日
        """
        trade_dates = await self.fetch_trade_dates(date, date)
        if trade_dates:
            return trade_dates[0].is_trading_day
        return False


# ==================== 同步接口 ====================

def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def fetch_trade_dates(start_date: str, end_date: str) -> List[Dict]:
    """同步接口：获取交易日信息"""
    fetcher = TradeDateFetcher()
    dates = run_async(fetcher.fetch_trade_dates(start_date, end_date))
    return [d.__dict__ for d in dates]


def get_trading_days(start_date: str, end_date: str) -> List[str]:
    """同步接口：获取所有交易日"""
    fetcher = TradeDateFetcher()
    return run_async(fetcher.get_trading_days(start_date, end_date))


def get_last_trading_day(date: str = None) -> Optional[str]:
    """同步接口：获取最近交易日"""
    fetcher = TradeDateFetcher()
    return run_async(fetcher.get_last_trading_day(date))


def is_trading_day(date: str) -> bool:
    """同步接口：判断是否为交易日"""
    fetcher = TradeDateFetcher()
    return run_async(fetcher.is_trading_day(date))


if __name__ == "__main__":
    # 测试
    import json

    print("=== 交易日查询测试 ===")

    # 获取最近交易日
    last_trading = get_last_trading_day()
    print(f"最近交易日: {last_trading}")

    # 判断今天是否为交易日
    today = datetime.now().strftime('%Y-%m-%d')
    is_trading = is_trading_day(today)
    print(f"{today} 是否为交易日: {is_trading}")

    # 获取本月交易日
    current_month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    current_month_end = (datetime.now().replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    current_month_end = current_month_end.strftime('%Y-%m-%d')

    trading_days = get_trading_days(current_month_start, current_month_end)
    print(f"\n本月交易日 ({len(trading_days)} 天):")
    print(trading_days[:10], "..." if len(trading_days) > 10 else "")
