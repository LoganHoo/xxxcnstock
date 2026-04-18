#!/usr/bin/env python3
"""
指数成份股获取器 - 微服务内部使用

支持BaoStock指数成份股API:
- 上证50成份股 (query_sz50_stocks)
- 沪深300成份股 (query_hs300_stocks)
- 中证500成份股 (query_zz500_stocks)

同时支持行业分类查询 (query_stock_industry)
"""
import asyncio
import importlib
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from core.logger import setup_logger

logger = setup_logger("index_constituent_fetcher", log_file="system/index_constituent_fetcher.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


class IndexType(Enum):
    """指数类型"""
    SZ50 = "sz50"       # 上证50
    HS300 = "hs300"     # 沪深300
    ZZ500 = "zz500"     # 中证500


@dataclass
class IndexConstituent:
    """指数成份股"""
    code: str           # 股票代码
    name: str           # 股票名称
    update_date: str    # 更新日期
    index_type: str     # 指数类型


@dataclass
class IndustryInfo:
    """行业分类信息"""
    code: str                   # 股票代码
    name: str                   # 股票名称
    industry: str               # 所属行业
    industry_classification: str # 行业分类标准
    update_date: str            # 更新日期


class IndexConstituentFetcher:
    """指数成份股获取器"""

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

    def _extract_code(self, code_bs: str) -> str:
        """从BaoStock代码格式提取6位数字代码"""
        return code_bs.split('.')[-1] if '.' in code_bs else code_bs

    async def fetch_sz50_stocks(self, date: str = None) -> List[IndexConstituent]:
        """
        获取上证50成份股

        Args:
            date: 查询日期 (YYYY-MM-DD)，默认今天
        """
        if not self._login():
            return []

        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            rs = self._bs.query_sz50_stocks(date=date)

            if rs.error_code != '0':
                logger.error(f"获取上证50成份股失败: {rs.error_msg}")
                return []

            constituents = []
            while rs.next():
                data = rs.get_row_data()
                constituents.append(IndexConstituent(
                    code=self._extract_code(data[1]),
                    name=data[2],
                    update_date=data[0],
                    index_type=IndexType.SZ50.value
                ))

            logger.info(f"获取上证50成份股: {len(constituents)} 只")
            return constituents

        except Exception as e:
            logger.error(f"获取上证50成份股异常: {e}")
            return []

    async def fetch_hs300_stocks(self, date: str = None) -> List[IndexConstituent]:
        """
        获取沪深300成份股

        Args:
            date: 查询日期 (YYYY-MM-DD)，默认今天
        """
        if not self._login():
            return []

        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            rs = self._bs.query_hs300_stocks(date=date)

            if rs.error_code != '0':
                logger.error(f"获取沪深300成份股失败: {rs.error_msg}")
                return []

            constituents = []
            while rs.next():
                data = rs.get_row_data()
                constituents.append(IndexConstituent(
                    code=self._extract_code(data[1]),
                    name=data[2],
                    update_date=data[0],
                    index_type=IndexType.HS300.value
                ))

            logger.info(f"获取沪深300成份股: {len(constituents)} 只")
            return constituents

        except Exception as e:
            logger.error(f"获取沪深300成份股异常: {e}")
            return []

    async def fetch_zz500_stocks(self, date: str = None) -> List[IndexConstituent]:
        """
        获取中证500成份股

        Args:
            date: 查询日期 (YYYY-MM-DD)，默认今天
        """
        if not self._login():
            return []

        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            rs = self._bs.query_zz500_stocks(date=date)

            if rs.error_code != '0':
                logger.error(f"获取中证500成份股失败: {rs.error_msg}")
                return []

            constituents = []
            while rs.next():
                data = rs.get_row_data()
                constituents.append(IndexConstituent(
                    code=self._extract_code(data[1]),
                    name=data[2],
                    update_date=data[0],
                    index_type=IndexType.ZZ500.value
                ))

            logger.info(f"获取中证500成份股: {len(constituents)} 只")
            return constituents

        except Exception as e:
            logger.error(f"获取中证500成份股异常: {e}")
            return []

    async def fetch_all_index_constituents(self, date: str = None) -> Dict[str, List[IndexConstituent]]:
        """
        获取所有指数成份股

        Args:
            date: 查询日期

        Returns:
            指数类型 -> 成份股列表 的字典
        """
        logger.info("获取所有指数成份股...")

        # 并发获取三个指数的成份股
        sz50, hs300, zz500 = await asyncio.gather(
            self.fetch_sz50_stocks(date),
            self.fetch_hs300_stocks(date),
            self.fetch_zz500_stocks(date),
            return_exceptions=True
        )

        result = {}

        if not isinstance(sz50, Exception):
            result[IndexType.SZ50.value] = sz50
        else:
            logger.error(f"上证50获取失败: {sz50}")
            result[IndexType.SZ50.value] = []

        if not isinstance(hs300, Exception):
            result[IndexType.HS300.value] = hs300
        else:
            logger.error(f"沪深300获取失败: {hs300}")
            result[IndexType.HS300.value] = []

        if not isinstance(zz500, Exception):
            result[IndexType.ZZ500.value] = zz500
        else:
            logger.error(f"中证500获取失败: {zz500}")
            result[IndexType.ZZ500.value] = []

        total = sum(len(v) for v in result.values())
        logger.info(f"指数成份股获取完成: 总计 {total} 只")

        return result

    async def fetch_stock_industry(self, code: str = "", date: str = None) -> List[IndustryInfo]:
        """
        获取行业分类信息

        Args:
            code: 股票代码，为空则获取所有股票
            date: 查询日期
        """
        if not self._login():
            return []

        if not date:
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            code_bs = ""
            if code:
                code_str = str(code).zfill(6)
                if code_str.startswith('6'):
                    code_bs = f"sh.{code_str}"
                else:
                    code_bs = f"sz.{code_str}"

            rs = self._bs.query_stock_industry(code=code_bs, date=date)

            if rs.error_code != '0':
                logger.error(f"获取行业分类失败: {rs.error_msg}")
                return []

            industries = []
            while rs.next():
                data = rs.get_row_data()
                industries.append(IndustryInfo(
                    code=self._extract_code(data[1]),
                    name=data[2],
                    industry=data[3],
                    industry_classification=data[4],
                    update_date=data[0]
                ))

            logger.info(f"获取行业分类: {len(industries)} 只股票")
            return industries

        except Exception as e:
            logger.error(f"获取行业分类异常: {e}")
            return []


# ==================== 同步接口 ====================

def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def fetch_sz50_stocks(date: str = None) -> List[Dict]:
    """同步接口：获取上证50成份股"""
    fetcher = IndexConstituentFetcher()
    constituents = run_async(fetcher.fetch_sz50_stocks(date))
    return [c.__dict__ for c in constituents]


def fetch_hs300_stocks(date: str = None) -> List[Dict]:
    """同步接口：获取沪深300成份股"""
    fetcher = IndexConstituentFetcher()
    constituents = run_async(fetcher.fetch_hs300_stocks(date))
    return [c.__dict__ for c in constituents]


def fetch_zz500_stocks(date: str = None) -> List[Dict]:
    """同步接口：获取中证500成份股"""
    fetcher = IndexConstituentFetcher()
    constituents = run_async(fetcher.fetch_zz500_stocks(date))
    return [c.__dict__ for c in constituents]


def fetch_all_index_constituents(date: str = None) -> Dict[str, List[Dict]]:
    """同步接口：获取所有指数成份股"""
    fetcher = IndexConstituentFetcher()
    result = run_async(fetcher.fetch_all_index_constituents(date))
    return {k: [c.__dict__ for c in v] for k, v in result.items()}


def fetch_stock_industry(code: str = "", date: str = None) -> List[Dict]:
    """同步接口：获取行业分类"""
    fetcher = IndexConstituentFetcher()
    industries = run_async(fetcher.fetch_stock_industry(code, date))
    return [i.__dict__ for i in industries]


if __name__ == "__main__":
    # 测试
    import json

    print("=== 上证50成份股 ===")
    sz50 = fetch_sz50_stocks()
    print(f"获取到 {len(sz50)} 只")
    if sz50:
        print(json.dumps(sz50[:3], indent=2, ensure_ascii=False))

    print("\n=== 沪深300成份股 ===")
    hs300 = fetch_hs300_stocks()
    print(f"获取到 {len(hs300)} 只")

    print("\n=== 中证500成份股 ===")
    zz500 = fetch_zz500_stocks()
    print(f"获取到 {len(zz500)} 只")

    print("\n=== 行业分类示例 ===")
    industries = fetch_stock_industry("000001")
    print(json.dumps(industries, indent=2, ensure_ascii=False))
