#!/usr/bin/env python3
"""
历史数据加载器

功能：
- 加载股票历史K线数据
- 加载指数历史数据
- 支持时间范围查询
- 支持复权处理
- 数据缓存
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import polars as pl
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path


class DataFrequency(Enum):
    """数据频率"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class HistoricalDataRequest:
    """历史数据请求"""
    code: str
    start_date: str
    end_date: str
    frequency: DataFrequency = DataFrequency.DAILY
    adjust: bool = True  # 是否复权
    fields: Optional[List[str]] = None


class HistoricalDataLoader:
    """历史数据加载器"""
    
    def __init__(self):
        self.logger = setup_logger("historical_data_loader")
        self.data_dir = get_data_path()
        self.kline_dir = self.data_dir / "kline"
        self.market_dir = self.data_dir / "market"
        
        # 缓存
        self._cache = {}
        self._cache_ttl = 300  # 缓存5分钟
    
    def load_stock_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        adjust: bool = True
    ) -> Optional[pl.DataFrame]:
        """
        加载股票历史数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjust: 是否应用前复权
        
        Returns:
            K线数据DataFrame
        """
        cache_key = f"{code}_{start_date}_{end_date}_{adjust}"
        
        # 检查缓存
        if cache_key in self._cache:
            cache_time, data = self._cache[cache_key]
            if time.time() - cache_time < self._cache_ttl:
                self.logger.debug(f"使用缓存: {code}")
                return data
        
        # 从本地加载
        file_path = self.kline_dir / f"{code}.parquet"
        
        if not file_path.exists():
            self.logger.warning(f"本地数据不存在: {code}")
            return None
        
        try:
            df = pl.read_parquet(file_path)
            
            # 过滤日期范围
            df = df.filter(
                (pl.col('trade_date') >= start_date) &
                (pl.col('trade_date') <= end_date)
            )
            
            if len(df) == 0:
                self.logger.warning(f"日期范围内无数据: {code} ({start_date} ~ {end_date})")
                return None
            
            # 应用复权
            if adjust and 'adj_factor' not in df.columns:
                df = self._apply_adjustment(df, code)
            
            # 更新缓存
            self._cache[cache_key] = (time.time(), df)
            
            self.logger.info(f"加载股票数据: {code}, {len(df)} 条记录")
            
            return df
            
        except Exception as e:
            self.logger.error(f"加载股票数据失败: {code}, {e}")
            return None
    
    def load_index_data(
        self,
        index_code: str,
        start_date: str,
        end_date: str
    ) -> Optional[pl.DataFrame]:
        """
        加载指数历史数据
        
        Args:
            index_code: 指数代码 (如 '000001')
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            指数数据DataFrame
        """
        file_path = self.market_dir / f"index_{index_code}.parquet"
        
        if not file_path.exists():
            self.logger.warning(f"指数数据不存在: {index_code}")
            return None
        
        try:
            df = pl.read_parquet(file_path)
            
            # 过滤日期范围
            df = df.filter(
                (pl.col('trade_date') >= start_date) &
                (pl.col('trade_date') <= end_date)
            )
            
            self.logger.info(f"加载指数数据: {index_code}, {len(df)} 条记录")
            
            return df
            
        except Exception as e:
            self.logger.error(f"加载指数数据失败: {index_code}, {e}")
            return None
    
    def load_batch(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        adjust: bool = True
    ) -> Dict[str, pl.DataFrame]:
        """
        批量加载股票数据
        
        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            adjust: 是否复权
        
        Returns:
            字典 {code: DataFrame}
        """
        results = {}
        
        self.logger.info(f"批量加载数据: {len(codes)} 只股票")
        
        for i, code in enumerate(codes):
            df = self.load_stock_data(code, start_date, end_date, adjust)
            if df is not None:
                results[code] = df
            
            if (i + 1) % 100 == 0:
                self.logger.info(f"   进度: {i+1}/{len(codes)}")
        
        self.logger.info(f"批量加载完成: {len(results)}/{len(codes)} 成功")
        
        return results
    
    def load_universe(
        self,
        universe_type: str = "all",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        加载整个股票池的数据
        
        Args:
            universe_type: 股票池类型 ('all', 'hs300', 'zz500')
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            字典 {code: DataFrame}
        """
        # 获取股票列表
        codes = self._get_universe_codes(universe_type)
        
        # 默认日期范围
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        return self.load_batch(codes, start_date, end_date)
    
    def get_price_series(
        self,
        code: str,
        field: str = 'close',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust: bool = True
    ) -> Optional[pd.Series]:
        """
        获取价格序列
        
        Args:
            code: 股票代码
            field: 价格字段 ('open', 'high', 'low', 'close')
            start_date: 开始日期
            end_date: 结束日期
            adjust: 是否复权
        
        Returns:
            价格序列
        """
        df = self.load_stock_data(code, start_date or '2020-01-01', end_date or datetime.now().strftime('%Y-%m-%d'), adjust)
        
        if df is None:
            return None
        
        # 选择复权后的字段
        if adjust and f'{field}_adj' in df.columns:
            field = f'{field}_adj'
        
        # 转换为pandas Series
        series = df.select(['trade_date', field]).to_pandas()
        series.set_index('trade_date', inplace=True)
        series = series[field]
        
        return series
    
    def get_returns(
        self,
        code: str,
        periods: int = 1,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.Series]:
        """
        获取收益率序列
        
        Args:
            code: 股票代码
            periods: 计算周期
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            收益率序列
        """
        prices = self.get_price_series(code, 'close', start_date, end_date)
        
        if prices is None:
            return None
        
        returns = prices.pct_change(periods=periods).dropna()
        
        return returns
    
    def get_panel_data(
        self,
        codes: List[str],
        field: str = 'close',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取面板数据（多股票时间序列）
        
        Args:
            codes: 股票代码列表
            field: 字段
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            DataFrame (index=date, columns=codes)
        """
        data_dict = {}
        
        for code in codes:
            series = self.get_price_series(code, field, start_date, end_date)
            if series is not None:
                data_dict[code] = series
        
        if not data_dict:
            return None
        
        # 合并为DataFrame
        panel_df = pd.DataFrame(data_dict)
        
        return panel_df
    
    def _apply_adjustment(self, df: pl.DataFrame, code: str) -> pl.DataFrame:
        """应用复权"""
        try:
            from services.data_service.fetchers.adjustment_factor_fetcher import get_adj_factor_fetcher
            
            fetcher = get_adj_factor_fetcher()
            
            # 获取日期范围
            dates = df['trade_date'].to_list()
            start_date = min(dates)
            end_date = max(dates)
            
            # 获取复权因子
            adj_df = fetcher.fetch_adj_factor(code, start_date, end_date)
            
            if adj_df is not None:
                # 重命名列以便合并
                adj_df = adj_df.rename({'date': 'trade_date'})
                
                # 合并
                df = df.join(adj_df.select(['trade_date', 'adj_factor']), on='trade_date', how='left')
                
                # 应用前复权
                df = fetcher.apply_forward_adjustment(df, adj_df)
            
            return df
            
        except Exception as e:
            self.logger.warning(f"应用复权失败: {code}, {e}")
            return df
    
    def _get_universe_codes(self, universe_type: str) -> List[str]:
        """获取股票池代码列表"""
        # 从股票列表文件加载
        stock_list_path = self.data_dir / "stock_list.parquet"
        
        if stock_list_path.exists():
            try:
                df = pl.read_parquet(stock_list_path)
                return df['code'].to_list()
            except Exception as e:
                self.logger.error(f"加载股票列表失败: {e}")
        
        # 返回空列表
        return []
    
    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        self.logger.info("历史数据缓存已清除")
    
    def get_data_info(self, code: str) -> Dict:
        """
        获取数据信息
        
        Args:
            code: 股票代码
        
        Returns:
            数据信息字典
        """
        file_path = self.kline_dir / f"{code}.parquet"
        
        if not file_path.exists():
            return {'exists': False}
        
        try:
            df = pl.read_parquet(file_path)
            
            dates = df['trade_date'].to_list()
            
            return {
                'exists': True,
                'code': code,
                'records': len(df),
                'start_date': min(dates),
                'end_date': max(dates),
                'fields': df.columns
            }
            
        except Exception as e:
            return {'exists': False, 'error': str(e)}


# 全局单例
_historical_loader = None


def get_historical_loader() -> HistoricalDataLoader:
    """获取历史数据加载器单例"""
    global _historical_loader
    
    if _historical_loader is None:
        _historical_loader = HistoricalDataLoader()
    
    return _historical_loader


# 便捷函数
def load_stock_data(code: str, start_date: str, end_date: str, adjust: bool = True) -> Optional[pl.DataFrame]:
    """便捷函数：加载股票数据"""
    loader = get_historical_loader()
    return loader.load_stock_data(code, start_date, end_date, adjust)


def load_index_data(index_code: str, start_date: str, end_date: str) -> Optional[pl.DataFrame]:
    """便捷函数：加载指数数据"""
    loader = get_historical_loader()
    return loader.load_index_data(index_code, start_date, end_date)


def get_price_series(code: str, field: str = 'close', start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[pd.Series]:
    """便捷函数：获取价格序列"""
    loader = get_historical_loader()
    return loader.get_price_series(code, field, start_date, end_date)


if __name__ == "__main__":
    # 测试
    loader = HistoricalDataLoader()
    
    # 加载单只股票
    df = loader.load_stock_data("000001", "2024-01-01", "2024-12-31")
    
    if df is not None:
        print(f"加载到 {len(df)} 条记录")
        print(df.head())
    
    # 获取价格序列
    prices = loader.get_price_series("000001", "close", "2024-01-01", "2024-12-31")
    
    if prices is not None:
        print(f"\n价格序列: {len(prices)} 条")
        print(prices.head())
