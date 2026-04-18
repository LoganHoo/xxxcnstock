#!/usr/bin/env python3
"""
Tushare 数据提供者实现

使用 Tushare Pro API 获取股票数据
"""
import os
import logging
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime

from .base import DataSourceProvider

logger = logging.getLogger(__name__)


class TushareProvider(DataSourceProvider):
    """Tushare Pro 数据提供者"""
    
    def __init__(self, token: Optional[str] = None, config: Dict[str, Any] = None):
        super().__init__('tushare', config)
        self.token = token or os.getenv('TUSHARE_TOKEN')
        self.pro = None
        
        if not self.token:
            raise ValueError("Tushare token is required. Set TUSHARE_TOKEN env var or pass token parameter.")
    
    def connect(self) -> bool:
        """连接 Tushare"""
        try:
            import tushare as ts
            self.pro = ts.pro_api(self.token)
            self._is_connected = True
            logger.info("Tushare connected successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect Tushare: {e}")
            return False
    
    def disconnect(self) -> bool:
        """断开连接"""
        self.pro = None
        self._is_connected = False
        return True
    
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """获取K线数据"""
        if not self.is_connected:
            self.connect()
        
        try:
            # 转换日期格式
            start = start_date.replace('-', '')
            end = end_date.replace('-', '')
            
            # 调用 Tushare API
            df = self.pro.daily(
                ts_code=code,
                start_date=start,
                end_date=end
            )
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 标准化列名
            df = df.rename(columns={
                'trade_date': 'date',
                'vol': 'volume',
                'amount': 'turnover'
            })
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date')
            
            return df[['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
            
        except Exception as e:
            logger.error(f"Failed to fetch kline for {code}: {e}")
            return pd.DataFrame()
    
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.is_connected:
            self.connect()
        
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            return df
        except Exception as e:
            logger.error(f"Failed to fetch stock list: {e}")
            return pd.DataFrame()
    
    def health_check(self) -> bool:
        """健康检查"""
        if not self.is_connected:
            return self.connect()
        
        try:
            # 尝试获取交易日历
            df = self.pro.trade_cal(start_date='20240101', end_date='20240101')
            return df is not None and not df.empty
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
