#!/usr/bin/env python3
"""
数据源提供者抽象基类

定义所有数据源提供者必须实现的接口
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
import pandas as pd
from datetime import datetime


class DataSourceProvider(ABC):
    """数据源提供者抽象基类"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self._is_connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """连接数据源"""
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """断开数据源连接"""
        pass
    
    @abstractmethod
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 频率 (d=日线, w=周线, m=月线)
        
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """健康检查"""
        pass
    
    @property
    def is_connected(self) -> bool:
        return self._is_connected
