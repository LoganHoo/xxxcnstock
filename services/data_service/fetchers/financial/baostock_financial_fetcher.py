#!/usr/bin/env python3
"""
Baostock 财务数据获取器

使用 Baostock 接口获取三大财务报表数据:
- 资产负债表: query_balance_data
- 利润表: query_profit_data
- 现金流量表: query_cash_flow_data
- 财务指标: query_dupont_data, query_growth_data, query_operation_data
"""
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
from pathlib import Path
import baostock as bs

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("baostock_financial", log_file="system/baostock_financial.log")


class BaostockFinancialFetcher:
    """Baostock 财务数据获取器"""
    
    def __init__(self):
        self._logged_in = False
    
    def _login(self) -> bool:
        """登录 Baostock"""
        if self._logged_in:
            return True
        try:
            result = bs.login()
            if result.error_code == '0':
                self._logged_in = True
                logger.info("Baostock 登录成功")
                return True
            else:
                logger.error(f"Baostock 登录失败: {result.error_msg}")
        except Exception as e:
            logger.error(f"Baostock 登录异常: {e}")
        return False
    
    def _logout(self):
        """登出 Baostock"""
        if self._logged_in:
            try:
                bs.logout()
                self._logged_in = False
            except:
                pass
    
    def _convert_code(self, code: str) -> str:
        """转换股票代码格式"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz.{code}"
        return code
    
    def fetch_balance_sheet(self, code: str, years: int = 3) -> pd.DataFrame:
        """
        获取资产负债表
        
        Args:
            code: 股票代码
            years: 获取年数
        
        Returns:
            资产负债表 DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        try:
            bs_code = self._convert_code(code)
            
            # 计算日期范围
            end_year = datetime.now().year
            start_year = end_year - years
            
            logger.info(f"获取 {code} 资产负债表 ({start_year}-{end_year})")
            
            # 查询资产负债表
            rs = bs.query_balance_data(
                code=bs_code,
                year=start_year,
                quarter=4  # 年报
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"{code} 未获取到资产负债表数据")
                return pd.DataFrame()
            
            # 创建 DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['currentAssets', 'totalAssets', 'currentLiability', 
                           'totalLiabilities', 'equity', 'totalEquity']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"{code} 获取到 {len(df)} 条资产负债表记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {code} 资产负债表失败: {e}")
            return pd.DataFrame()
    
    def fetch_income_statement(self, code: str, years: int = 3) -> pd.DataFrame:
        """
        获取利润表
        
        Args:
            code: 股票代码
            years: 获取年数
        
        Returns:
            利润表 DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        try:
            bs_code = self._convert_code(code)
            
            end_year = datetime.now().year
            start_year = end_year - years
            
            logger.info(f"获取 {code} 利润表 ({start_year}-{end_year})")
            
            # 查询利润表
            rs = bs.query_profit_data(
                code=bs_code,
                year=start_year,
                quarter=4
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"{code} 未获取到利润表数据")
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['revenue', 'operateProfit', 'totalProfit', 'netProfit']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"{code} 获取到 {len(df)} 条利润表记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {code} 利润表失败: {e}")
            return pd.DataFrame()
    
    def fetch_cash_flow(self, code: str, years: int = 3) -> pd.DataFrame:
        """
        获取现金流量表
        
        Args:
            code: 股票代码
            years: 获取年数
        
        Returns:
            现金流量表 DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        try:
            bs_code = self._convert_code(code)
            
            end_year = datetime.now().year
            start_year = end_year - years
            
            logger.info(f"获取 {code} 现金流量表 ({start_year}-{end_year})")
            
            # 查询现金流量表
            rs = bs.query_cash_flow_data(
                code=bs_code,
                year=start_year,
                quarter=4
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"{code} 未获取到现金流量表数据")
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['netOperateCashFlow', 'netInvestCashFlow', 'netFinanceCashFlow']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"{code} 获取到 {len(df)} 条现金流量表记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {code} 现金流量表失败: {e}")
            return pd.DataFrame()
    
    def fetch_financial_indicators(self, code: str, years: int = 3) -> pd.DataFrame:
        """
        获取财务指标 (杜邦分析)
        
        Args:
            code: 股票代码
            years: 获取年数
        
        Returns:
            财务指标 DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        try:
            bs_code = self._convert_code(code)
            
            end_year = datetime.now().year
            start_year = end_year - years
            
            logger.info(f"获取 {code} 财务指标 ({start_year}-{end_year})")
            
            # 查询杜邦分析数据
            rs = bs.query_dupont_data(
                code=bs_code,
                year=start_year,
                quarter=4
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"{code} 未获取到财务指标数据")
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['dupontROE', 'totalAssetTurnover', 'profitMargin', 'equityMultiplier']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"{code} 获取到 {len(df)} 条财务指标记录")
            return df
            
        except Exception as e:
            logger.error(f"获取 {code} 财务指标失败: {e}")
            return pd.DataFrame()


# ==================== 便捷函数 ====================

def fetch_balance_sheet_baostock(code: str, years: int = 3) -> pd.DataFrame:
    """获取资产负债表 (便捷函数)"""
    fetcher = BaostockFinancialFetcher()
    try:
        return fetcher.fetch_balance_sheet(code, years)
    finally:
        fetcher._logout()


def fetch_income_statement_baostock(code: str, years: int = 3) -> pd.DataFrame:
    """获取利润表 (便捷函数)"""
    fetcher = BaostockFinancialFetcher()
    try:
        return fetcher.fetch_income_statement(code, years)
    finally:
        fetcher._logout()


def fetch_cash_flow_baostock(code: str, years: int = 3) -> pd.DataFrame:
    """获取现金流量表 (便捷函数)"""
    fetcher = BaostockFinancialFetcher()
    try:
        return fetcher.fetch_cash_flow(code, years)
    finally:
        fetcher._logout()


def fetch_financial_indicators_baostock(code: str, years: int = 3) -> pd.DataFrame:
    """获取财务指标 (便捷函数)"""
    fetcher = BaostockFinancialFetcher()
    try:
        return fetcher.fetch_financial_indicators(code, years)
    finally:
        fetcher._logout()
