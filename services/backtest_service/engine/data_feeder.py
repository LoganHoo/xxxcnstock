#!/usr/bin/env python3
"""
数据供给器
为Backtrader准备数据
"""
import pandas as pd
import backtrader as bt
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class PandasData(bt.feeds.PandasData):
    """Pandas数据源适配器"""
    
    params = (
        ('datetime', None),  # 使用索引作为日期时间
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1),
    )


class DataFeeder:
    """
    数据供给器
    
    将DataFrame转换为Backtrader数据格式
    """
    
    def prepare_data(self, df: pd.DataFrame, name: Optional[str] = None) -> PandasData:
        """
        准备Backtrader数据
        
        Args:
            df: DataFrame with columns: open, high, low, close, volume
            name: 数据名称
        
        Returns:
            Backtrader数据对象
        """
        # 确保列名正确
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # 确保索引是日期时间
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'date' in df.columns:
                df.index = pd.to_datetime(df['date'])
            elif 'trade_date' in df.columns:
                df.index = pd.to_datetime(df['trade_date'])
            else:
                raise ValueError("DataFrame must have DatetimeIndex or date/trade_date column")
        
        # 按日期排序
        df = df.sort_index()
        
        data = PandasData(dataname=df)
        if name:
            data._name = name
        
        return data
    
    def prepare_multi_stock_data(
        self,
        data_dict: Dict[str, pd.DataFrame]
    ) -> List[PandasData]:
        """
        准备多只股票数据
        
        Args:
            data_dict: {code: DataFrame}
        
        Returns:
            Backtrader数据对象列表
        """
        data_list = []
        for code, df in data_dict.items():
            try:
                data = self.prepare_data(df, name=code)
                data_list.append(data)
                logger.debug(f"Prepared data for {code}: {len(df)} rows")
            except Exception as e:
                logger.warning(f"Failed to prepare data for {code}: {e}")
        
        logger.info(f"Prepared {len(data_list)} data feeds")
        return data_list
    
    def resample_data(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        重采样数据
        
        Args:
            df: 原始数据
            timeframe: 目标周期 ('W'=周, 'M'=月)
        
        Returns:
            重采样后的数据
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have DatetimeIndex")
        
        resampled = df.resample(timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        return resampled
