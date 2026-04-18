#!/usr/bin/env python3
"""
基本面数据获取器
使用Baostock获取PE、PB、PS等估值指标
"""
import importlib
import pandas as pd
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from core.logger import setup_logger

logger = setup_logger("fundamental_fetcher", log_file="system/fundamental.log")


def _get_baostock_client():
    """延迟加载baostock"""
    return importlib.import_module("baostock")


@dataclass
class StockFundamental:
    """股票基本面数据"""
    code: str
    name: str
    pe_ttm: Optional[float]  # 市盈率TTM
    pb: Optional[float]      # 市净率
    ps_ttm: Optional[float]  # 市销率TTM
    pcf: Optional[float]     # 市现率
    total_mv: Optional[float]  # 总市值(亿)
    float_mv: Optional[float]  # 流通市值(亿)
    turnover: Optional[float]  # 换手率
    date: str


class FundamentalFetcher:
    """基本面数据获取器"""
    
    def __init__(self):
        self._bs = None
        self._logged_in = False
    
    def _login(self) -> bool:
        """登录Baostock"""
        if self._logged_in:
            return True
        
        try:
            bs = _get_baostock_client()
            lg = bs.login()
            if lg.error_code == '0':
                self._bs = bs
                self._logged_in = True
                logger.info("Baostock登录成功")
                return True
            else:
                logger.error(f"Baostock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock登录异常: {e}")
            return False
    
    def _logout(self):
        """登出Baostock"""
        if self._logged_in and self._bs:
            try:
                self._bs.logout()
                logger.info("Baostock登出成功")
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
    
    async def fetch_fundamental(self, code: str) -> Optional[StockFundamental]:
        """
        获取单只股票基本面数据
        
        Args:
            code: 股票代码(6位数字)
        Returns:
            StockFundamental或None
        """
        if not self._login():
            return None
        
        try:
            code_bs = self._convert_code(code)
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            # 查询历史K线获取估值指标
            rs = self._bs.query_history_k_data_plus(
                code_bs,
                "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                start_date=date_str,
                end_date=date_str,
                frequency="d"
            )
            
            if rs.error_code != '0':
                logger.warning(f"{code} 查询失败: {rs.error_msg}")
                return None
            
            # 获取数据
            data = None
            while rs.next():
                row = rs.get_row_data()
                data = {
                    'code': code,
                    'name': '',  # 需要另外获取
                    'pe_ttm': float(row[2]) if row[2] else None,
                    'pb': float(row[3]) if row[3] else None,
                    'ps_ttm': float(row[4]) if row[4] else None,
                    'pcf': float(row[5]) if row[5] else None,
                    'total_mv': None,
                    'float_mv': None,
                    'turnover': None,
                    'date': row[0]
                }
            
            if data:
                return StockFundamental(**data)
            return None
            
        except Exception as e:
            logger.error(f"{code} 采集异常: {e}")
            return None
    
    async def fetch_all_fundamentals(self, stock_list: List[Dict]) -> List[StockFundamental]:
        """
        批量获取基本面数据
        
        Args:
            stock_list: 股票列表 [{'code': '000001', 'name': '平安银行'}, ...]
        Returns:
            StockFundamental列表
        """
        if not self._login():
            return []
        
        results = []
        total = len(stock_list)
        
        logger.info(f"开始采集 {total} 只股票的基本面数据")
        
        for i, stock in enumerate(stock_list, 1):
            try:
                fundamental = await self.fetch_fundamental(stock['code'])
                if fundamental:
                    fundamental.name = stock.get('name', '')
                    results.append(fundamental)
                
                if i % 100 == 0:
                    logger.info(f"进度: {i}/{total} ({i/total*100:.1f}%)")
                    
            except Exception as e:
                logger.warning(f"{stock['code']} 处理失败: {e}")
                continue
        
        self._logout()
        logger.info(f"采集完成: 成功 {len(results)}/{total}")
        return results
    
    async def fetch_from_parquet(self, stock_list_path: str) -> pd.DataFrame:
        """
        从股票列表文件获取所有基本面数据
        
        Args:
            stock_list_path: 股票列表parquet文件路径
        Returns:
            DataFrame
        """
        import polars as pl
        
        try:
            # 读取股票列表
            df_stocks = pl.read_parquet(stock_list_path)
            stock_list = [
                {'code': row['code'], 'name': row['name']}
                for row in df_stocks.iter_rows(named=True)
            ]
            
            logger.info(f"从 {stock_list_path} 读取了 {len(stock_list)} 只股票")
            
            # 采集基本面数据
            fundamentals = await self.fetch_all_fundamentals(stock_list)
            
            # 转换为DataFrame
            if fundamentals:
                data = [f.__dict__ for f in fundamentals]
                return pd.DataFrame(data)
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"从parquet采集失败: {e}")
            return pd.DataFrame()
