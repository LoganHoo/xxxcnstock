#!/usr/bin/env python3
"""
财务数据获取器 - 微服务内部使用

支持BaoStock所有财务相关API:
- 季频盈利能力 (query_profit_data)
- 季频营运能力 (query_operation_data)
- 季频成长能力 (query_growth_data)
- 季频偿债能力 (query_balance_data)
- 季频现金流量 (query_cash_flow_data)
- 季频杜邦指数 (query_dupont_data)
- 除权除息信息 (query_dividend_data)
- 复权因子信息 (query_adjust_factor)
"""
import asyncio
import importlib
import pandas as pd
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from core.logger import setup_logger

logger = setup_logger("financial_fetcher", log_file="system/financial_fetcher.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


class FinancialDataType(Enum):
    """财务数据类型"""
    PROFIT = "profit"           # 盈利能力
    OPERATION = "operation"     # 营运能力
    GROWTH = "growth"           # 成长能力
    BALANCE = "balance"         # 偿债能力
    CASH_FLOW = "cash_flow"     # 现金流量
    DUPONT = "dupont"           # 杜邦指数
    DIVIDEND = "dividend"       # 除权除息
    ADJUST_FACTOR = "adjust"    # 复权因子


@dataclass
class ProfitData:
    """盈利能力数据"""
    code: str
    pub_date: str           # 发布日期
    stat_date: str          # 统计日期
    roe_avg: float          # 净资产收益率(平均)
    np_margin: float        # 销售净利率
    gp_margin: float        # 销售毛利率
    net_profit: float       # 净利润
    eps_ttm: float          # 每股收益
    mb_revenue: float       # 主营营业收入
    total_share: float      # 总股本
    liqa_share: float       # 流通股本


@dataclass
class OperationData:
    """营运能力数据"""
    code: str
    pub_date: str
    stat_date: str
    nr_turn_ratio: float    # 应收账款周转率
    nr_turn_days: float     # 应收账款周转天数
    inv_turn_ratio: float   # 存货周转率
    inv_turn_days: float    # 存货周转天数
    ca_turn_ratio: float    # 流动资产周转率
    asset_turn_ratio: float # 总资产周转率


@dataclass
class GrowthData:
    """成长能力数据"""
    code: str
    pub_date: str
    stat_date: str
    yoy_equity: float       # 净资产同比增长率
    yoy_asset: float        # 总资产同比增长率
    yoy_ni: float           # 净利润同比增长率
    yoy_eps_basic: float    # 基本每股收益同比增长率
    yoy_pni: float          # 归属母公司净利润同比增长率


@dataclass
class BalanceData:
    """偿债能力数据"""
    code: str
    pub_date: str
    stat_date: str
    current_ratio: float    # 流动比率
    quick_ratio: float      # 速动比率
    cash_ratio: float       # 现金比率
    yoy_liability: float    # 总负债同比增长率
    liability_to_asset: float   # 资产负债率
    asset_to_equity: float  # 权益乘数


@dataclass
class CashFlowData:
    """现金流量数据"""
    code: str
    pub_date: str
    stat_date: str
    ca_to_asset: float      # 流动资产占比
    nca_to_asset: float     # 非流动资产占比
    tangible_asset_to_asset: float  # 有形资产占比
    ebit_to_interest: float # 利息保障倍数
    cfo_to_or: float        # 经营活动现金流/营业收入
    cfo_to_np: float        # 经营活动现金流/净利润
    cfo_to_gr: float        # 经营活动现金流/营业总收入


@dataclass
class DupontData:
    """杜邦指数数据"""
    code: str
    pub_date: str
    stat_date: str
    dupont_roe: float               # 净资产收益率
    dupont_asset_sto_equity: float  # 权益乘数
    dupont_asset_turn: float        # 总资产周转率
    dupont_pnitoni: float           # 利润总额/营业收入
    dupont_nitogr: float            # 净利润/营业总收入
    dupont_tax_burden: float        # 净利润/利润总额
    dupont_int_burden: float        # 利润总额/息税前利润
    dupont_ebit_to_gr: float        # 息税前利润/营业总收入


@dataclass
class DividendData:
    """除权除息数据"""
    code: str
    divid_pre_notice_date: str      # 预披露公告日
    divid_agm_pum_date: str         # 股东大会公告日
    divid_plan_announce_date: str   # 预案公告日
    divid_plan_date: str            # 分红预案公告日
    divid_regist_date: str          # 股权登记日
    divid_operate_date: str         # 除权除息日
    divid_pay_date: str             # 红利支付日
    divid_cash_ps_before_tax: float # 每股股利(税前)
    divid_cash_ps_after_tax: float  # 每股股利(税后)
    divid_stocks_ps: float          # 每股送转股


@dataclass
class AdjustFactor:
    """复权因子数据"""
    code: str
    divid_operate_date: str     # 除权除息日
    fore_adjust_factor: float   # 前复权因子
    back_adjust_factor: float   # 后复权因子
    adjust_factor: float        # 复权因子


class FinancialFetcher:
    """财务数据获取器"""

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

    def _convert_code(self, code: str) -> str:
        """转换股票代码格式"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz.{code}"
        return code

    async def fetch_profit_data(self, code: str, year: int, quarter: int) -> Optional[ProfitData]:
        """
        获取盈利能力数据

        Args:
            code: 股票代码
            year: 年份
            quarter: 季度 (1,2,3,4)
        """
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_profit_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取盈利能力数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return ProfitData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    roe_avg=float(data[3]) if data[3] else None,
                    np_margin=float(data[4]) if data[4] else None,
                    gp_margin=float(data[5]) if data[5] else None,
                    net_profit=float(data[6]) if data[6] else None,
                    eps_ttm=float(data[7]) if data[7] else None,
                    mb_revenue=float(data[8]) if data[8] else None,
                    total_share=float(data[9]) if data[9] else None,
                    liqa_share=float(data[10]) if data[10] else None
                )

        except Exception as e:
            logger.error(f"获取盈利能力数据异常: {e}")

        return None

    async def fetch_operation_data(self, code: str, year: int, quarter: int) -> Optional[OperationData]:
        """获取营运能力数据"""
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_operation_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取营运能力数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return OperationData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    nr_turn_ratio=float(data[3]) if data[3] else None,
                    nr_turn_days=float(data[4]) if data[4] else None,
                    inv_turn_ratio=float(data[5]) if data[5] else None,
                    inv_turn_days=float(data[6]) if data[6] else None,
                    ca_turn_ratio=float(data[7]) if data[7] else None,
                    asset_turn_ratio=float(data[8]) if data[8] else None
                )

        except Exception as e:
            logger.error(f"获取营运能力数据异常: {e}")

        return None

    async def fetch_growth_data(self, code: str, year: int, quarter: int) -> Optional[GrowthData]:
        """获取成长能力数据"""
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_growth_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取成长能力数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return GrowthData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    yoy_equity=float(data[3]) if data[3] else None,
                    yoy_asset=float(data[4]) if data[4] else None,
                    yoy_ni=float(data[5]) if data[5] else None,
                    yoy_eps_basic=float(data[6]) if data[6] else None,
                    yoy_pni=float(data[7]) if data[7] else None
                )

        except Exception as e:
            logger.error(f"获取成长能力数据异常: {e}")

        return None

    async def fetch_balance_data(self, code: str, year: int, quarter: int) -> Optional[BalanceData]:
        """获取偿债能力数据"""
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_balance_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取偿债能力数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return BalanceData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    current_ratio=float(data[3]) if data[3] else None,
                    quick_ratio=float(data[4]) if data[4] else None,
                    cash_ratio=float(data[5]) if data[5] else None,
                    yoy_liability=float(data[6]) if data[6] else None,
                    liability_to_asset=float(data[7]) if data[7] else None,
                    asset_to_equity=float(data[8]) if data[8] else None
                )

        except Exception as e:
            logger.error(f"获取偿债能力数据异常: {e}")

        return None

    async def fetch_cash_flow_data(self, code: str, year: int, quarter: int) -> Optional[CashFlowData]:
        """获取现金流量数据"""
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_cash_flow_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取现金流量数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return CashFlowData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    ca_to_asset=float(data[3]) if data[3] else None,
                    nca_to_asset=float(data[4]) if data[4] else None,
                    tangible_asset_to_asset=float(data[5]) if data[5] else None,
                    ebit_to_interest=float(data[6]) if data[6] else None,
                    cfo_to_or=float(data[7]) if data[7] else None,
                    cfo_to_np=float(data[8]) if data[8] else None,
                    cfo_to_gr=float(data[9]) if data[9] else None
                )

        except Exception as e:
            logger.error(f"获取现金流量数据异常: {e}")

        return None

    async def fetch_dupont_data(self, code: str, year: int, quarter: int) -> Optional[DupontData]:
        """获取杜邦指数数据"""
        if not self._login():
            return None

        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_dupont_data(code=code_bs, year=year, quarter=quarter)

            if rs.error_code != '0':
                logger.error(f"获取杜邦指数数据失败: {rs.error_msg}")
                return None

            if rs.next():
                data = rs.get_row_data()
                return DupontData(
                    code=code,
                    pub_date=data[1],
                    stat_date=data[2],
                    dupont_roe=float(data[3]) if data[3] else None,
                    dupont_asset_sto_equity=float(data[4]) if data[4] else None,
                    dupont_asset_turn=float(data[5]) if data[5] else None,
                    dupont_pnitoni=float(data[6]) if data[6] else None,
                    dupont_nitogr=float(data[7]) if data[7] else None,
                    dupont_tax_burden=float(data[8]) if data[8] else None,
                    dupont_int_burden=float(data[9]) if data[9] else None,
                    dupont_ebit_to_gr=float(data[10]) if data[10] else None
                )

        except Exception as e:
            logger.error(f"获取杜邦指数数据异常: {e}")

        return None

    async def fetch_all_financial_data(self, code: str, year: int, quarter: int) -> Dict[str, Any]:
        """
        获取所有财务数据

        Args:
            code: 股票代码
            year: 年份
            quarter: 季度

        Returns:
            包含所有财务数据的字典
        """
        logger.info(f"获取 {code} {year}Q{quarter} 财务数据...")

        # 并发获取所有财务数据
        profit, operation, growth, balance, cash_flow, dupont = await asyncio.gather(
            self.fetch_profit_data(code, year, quarter),
            self.fetch_operation_data(code, year, quarter),
            self.fetch_growth_data(code, year, quarter),
            self.fetch_balance_data(code, year, quarter),
            self.fetch_cash_flow_data(code, year, quarter),
            self.fetch_dupont_data(code, year, quarter),
            return_exceptions=True
        )

        result = {
            'code': code,
            'year': year,
            'quarter': quarter,
            'profit': profit.__dict__ if profit else None,
            'operation': operation.__dict__ if operation else None,
            'growth': growth.__dict__ if growth else None,
            'balance': balance.__dict__ if balance else None,
            'cash_flow': cash_flow.__dict__ if cash_flow else None,
            'dupont': dupont.__dict__ if dupont else None,
        }

        # 统计成功获取的数据类型
        success_count = sum(1 for v in [profit, operation, growth, balance, cash_flow, dupont] if v is not None)
        logger.info(f"财务数据获取完成: {success_count}/6 类数据成功")

        return result


# ==================== 同步接口 ====================

def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def fetch_financial_data(code: str, year: int, quarter: int) -> Dict[str, Any]:
    """
    同步接口：获取所有财务数据

    Args:
        code: 股票代码
        year: 年份
        quarter: 季度 (1,2,3,4)

    Returns:
        财务数据字典
    """
    fetcher = FinancialFetcher()
    return run_async(fetcher.fetch_all_financial_data(code, year, quarter))


def fetch_profit_data(code: str, year: int, quarter: int) -> Optional[ProfitData]:
    """同步接口：获取盈利能力数据"""
    fetcher = FinancialFetcher()
    return run_async(fetcher.fetch_profit_data(code, year, quarter))


def fetch_growth_data(code: str, year: int, quarter: int) -> Optional[GrowthData]:
    """同步接口：获取成长能力数据"""
    fetcher = FinancialFetcher()
    return run_async(fetcher.fetch_growth_data(code, year, quarter))


if __name__ == "__main__":
    # 测试
    import json
    result = fetch_financial_data("000001", 2024, 3)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
