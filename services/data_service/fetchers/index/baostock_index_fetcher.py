#!/usr/bin/env python3
"""
Baostock 大盘指数数据获取器

使用 Baostock 接口获取大盘指数数据:
- 上证指数 (sh.000001)
- 深证成指 (sz.399001)
- 创业板指 (sz.399006)
- 沪深300 (sh.000300)
- 中证500 (sh.000905)
- 科创50 (sh.000688)
- 上证50 (sh.000016)
"""
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from pathlib import Path
import baostock as bs

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("baostock_index", log_file="system/baostock_index.log")


# 大盘指数代码映射
INDEX_CODES = {
    "上证指数": "sh.000001",
    "深证成指": "sz.399001",
    "创业板指": "sz.399006",
    "沪深300": "sh.000300",
    "中证500": "sh.000905",
    "科创50": "sh.000688",
    "上证50": "sh.000016",
    "深证100": "sz.399330",
    "中证1000": "sh.000852",
    "国证2000": "sz.399303"
}


class BaostockIndexFetcher:
    """Baostock 大盘指数数据获取器"""
    
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
                logger.info("Baostock 登出成功")
            except Exception as e:
                logger.error(f"Baostock 登出异常: {e}")
    
    def fetch_index_kline(self, index_code: str, start_date: str, end_date: str,
                          fields: str = None) -> pd.DataFrame:
        """
        获取大盘指数K线数据
        
        Args:
            index_code: 指数代码 (如 sh.000001)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            fields: 需要获取的字段，默认获取全部
        
        Returns:
            指数K线数据 DataFrame
        """
        if not self._login():
            return pd.DataFrame()
        
        try:
            logger.info(f"获取 {index_code} 指数数据: {start_date} 至 {end_date}")
            
            # 默认字段
            if fields is None:
                fields = "date,code,open,high,low,close,preclose,volume,amount,pctChg"
            
            # 查询指数K线数据
            rs = bs.query_history_k_data_plus(
                index_code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency="d"
            )
            
            if rs.error_code != '0':
                logger.error(f"查询失败: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                logger.warning(f"{index_code} 未获取到数据")
                return pd.DataFrame()
            
            # 创建 DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'pctChg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"{index_code} 获取到 {len(df)} 条数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {index_code} 数据失败: {e}")
            return pd.DataFrame()
    
    def fetch_sh_index(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取上证指数"""
        return self.fetch_index_kline(INDEX_CODES["上证指数"], start_date, end_date)
    
    def fetch_sz_index(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取深证成指"""
        return self.fetch_index_kline(INDEX_CODES["深证成指"], start_date, end_date)
    
    def fetch_cy_index(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取创业板指"""
        return self.fetch_index_kline(INDEX_CODES["创业板指"], start_date, end_date)
    
    def fetch_hs300(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取沪深300指数"""
        return self.fetch_index_kline(INDEX_CODES["沪深300"], start_date, end_date)
    
    def fetch_zz500(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取中证500指数"""
        return self.fetch_index_kline(INDEX_CODES["中证500"], start_date, end_date)
    
    def fetch_kc50(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取科创50指数"""
        return self.fetch_index_kline(INDEX_CODES["科创50"], start_date, end_date)
    
    def fetch_sz50(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取上证50指数"""
        return self.fetch_index_kline(INDEX_CODES["上证50"], start_date, end_date)
    
    def fetch_all_major_indices(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        获取所有主要大盘指数
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            指数名称 -> DataFrame 的字典
        """
        major_indices = ["上证指数", "深证成指", "创业板指", "沪深300", "中证500", "科创50", "上证50"]
        results = {}
        
        for name in major_indices:
            code = INDEX_CODES[name]
            df = self.fetch_index_kline(code, start_date, end_date)
            if not df.empty:
                results[name] = df
        
        return results
    
    def save_index_data(self, df: pd.DataFrame, index_name: str, data_dir: Path = None):
        """
        保存指数数据到文件
        
        Args:
            df: 指数数据 DataFrame
            index_name: 指数名称
            data_dir: 数据目录
        """
        if df.empty:
            logger.warning(f"{index_name} 数据为空，跳过保存")
            return
        
        if data_dir is None:
            data_dir = get_data_path() / "index"
        
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # 构建文件名
        file_path = data_dir / f"{index_name}.parquet"
        
        try:
            df.to_parquet(file_path, index=False)
            logger.info(f"{index_name} 数据已保存: {file_path}")
        except Exception as e:
            logger.error(f"保存 {index_name} 数据失败: {e}")


# ==================== 便捷函数 ====================

def fetch_index_kline(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取指数K线数据 (便捷函数)"""
    fetcher = BaostockIndexFetcher()
    try:
        return fetcher.fetch_index_kline(index_code, start_date, end_date)
    finally:
        fetcher._logout()


def fetch_sh_index(start_date: str, end_date: str) -> pd.DataFrame:
    """获取上证指数 (便捷函数)"""
    return fetch_index_kline(INDEX_CODES["上证指数"], start_date, end_date)


def fetch_sz_index(start_date: str, end_date: str) -> pd.DataFrame:
    """获取深证成指 (便捷函数)"""
    return fetch_index_kline(INDEX_CODES["深证成指"], start_date, end_date)


def fetch_cy_index(start_date: str, end_date: str) -> pd.DataFrame:
    """获取创业板指 (便捷函数)"""
    return fetch_index_kline(INDEX_CODES["创业板指"], start_date, end_date)


def fetch_hs300(start_date: str, end_date: str) -> pd.DataFrame:
    """获取沪深300 (便捷函数)"""
    return fetch_index_kline(INDEX_CODES["沪深300"], start_date, end_date)


def fetch_zz500(start_date: str, end_date: str) -> pd.DataFrame:
    """获取中证500 (便捷函数)"""
    return fetch_index_kline(INDEX_CODES["中证500"], start_date, end_date)


def fetch_all_major_indices(start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """获取所有主要大盘指数 (便捷函数)"""
    fetcher = BaostockIndexFetcher()
    try:
        return fetcher.fetch_all_major_indices(start_date, end_date)
    finally:
        fetcher._logout()
