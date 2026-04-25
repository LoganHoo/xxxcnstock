#!/usr/bin/env python3
"""
真实数据源适配器

集成Tushare和Baostock获取实时数据
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class DataSourceAdapter:
    """数据源适配器基类"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.name = "base"
        
    def connect(self) -> bool:
        """连接数据源"""
        raise NotImplementedError
    
    def get_daily_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        raise NotImplementedError
    
    def get_realtime_data(self, codes: List[str]) -> pd.DataFrame:
        """获取实时数据"""
        raise NotImplementedError
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        raise NotImplementedError


class TushareAdapter(DataSourceAdapter):
    """Tushare数据源适配器"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.name = "tushare"
        self.pro = None
        
    def connect(self) -> bool:
        """连接Tushare"""
        try:
            import tushare as ts
            token = self.config.get('token') or os.getenv('TUSHARE_TOKEN')
            if not token:
                logger.error("Tushare token未配置")
                return False
            
            self.pro = ts.pro_api(token)
            logger.info("Tushare连接成功")
            return True
        except Exception as e:
            logger.error(f"Tushare连接失败: {e}")
            return False
    
    def get_daily_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # 转换代码格式
            ts_code = self._convert_code(code)
            
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', '')
            )
            
            if df.empty:
                return df
            
            # 标准化列名
            df = df.rename(columns={
                'ts_code': 'code',
                'trade_date': 'trade_date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'volume',
                'amount': 'amount'
            })
            
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['code'] = code
            
            return df.sort_values('trade_date')
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            return pd.DataFrame()
    
    def get_realtime_data(self, codes: List[str]) -> pd.DataFrame:
        """获取实时数据"""
        try:
            import tushare as ts
            
            data_list = []
            for code in codes:
                ts_code = self._convert_code(code)
                df = ts.realtime_quote(ts_code)
                if not df.empty:
                    data_list.append(df)
            
            if data_list:
                return pd.concat(data_list, ignore_index=True)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取实时数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.pro:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            df = df.rename(columns={
                'ts_code': 'ts_code',
                'symbol': 'code',
                'name': 'name'
            })
            
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def _convert_code(self, code: str) -> str:
        """转换代码格式"""
        if code.startswith('6'):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"


class BaostockAdapter(DataSourceAdapter):
    """Baostock数据源适配器"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.name = "baostock"
        self.connected = False
        
    def connect(self) -> bool:
        """连接Baostock"""
        try:
            import baostock as bs
            
            lg = bs.login()
            if lg.error_code == '0':
                self.connected = True
                logger.info("Baostock连接成功")
                return True
            else:
                logger.error(f"Baostock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock连接失败: {e}")
            return False
    
    def get_daily_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        if not self.connected:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            import baostock as bs
            
            # 转换代码格式
            bs_code = self._convert_code(code)
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"  # 后复权
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 转换数据类型
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.rename(columns={'date': 'trade_date'})
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            return df.sort_values('trade_date')
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            return pd.DataFrame()
    
    def get_realtime_data(self, codes: List[str]) -> pd.DataFrame:
        """Baostock不支持实时数据"""
        logger.warning("Baostock不支持实时数据获取")
        return pd.DataFrame()
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.connected:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            import baostock as bs
            
            # 获取沪深A股
            rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            df = pd.DataFrame(data_list, columns=['code', 'name'])
            
            # 过滤出A股
            df = df[df['code'].str.match(r'(sh\.60|sz\.00|sz\.30)')]
            
            # 转换代码格式
            df['code'] = df['code'].str.replace(r'(sh\.|sz\.)', '', regex=True)
            
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def _convert_code(self, code: str) -> str:
        """转换代码格式"""
        if code.startswith('6'):
            return f"sh.{code}"
        else:
            return f"sz.{code}"


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self):
        self.adapters: Dict[str, DataSourceAdapter] = {}
        self.primary_adapter: Optional[DataSourceAdapter] = None
        
    def register_adapter(self, adapter: DataSourceAdapter, primary: bool = False):
        """注册数据源适配器"""
        self.adapters[adapter.name] = adapter
        if primary or not self.primary_adapter:
            self.primary_adapter = adapter
        logger.info(f"注册数据源: {adapter.name}")
    
    def get_adapter(self, name: str = None) -> DataSourceAdapter:
        """获取数据源适配器"""
        if name:
            return self.adapters.get(name)
        return self.primary_adapter
    
    def get_daily_data(self, code: str, start_date: str, end_date: str, 
                       source: str = None) -> pd.DataFrame:
        """获取日线数据（支持多源回退）"""
        adapter = self.get_adapter(source)
        
        if adapter:
            df = adapter.get_daily_data(code, start_date, end_date)
            if not df.empty:
                return df
        
        # 尝试其他数据源
        for name, adapter in self.adapters.items():
            if name != source:
                df = adapter.get_daily_data(code, start_date, end_date)
                if not df.empty:
                    logger.info(f"使用备用数据源: {name}")
                    return df
        
        logger.error(f"所有数据源都无法获取 {code} 的数据")
        return pd.DataFrame()
    
    def get_realtime_data(self, codes: List[str]) -> pd.DataFrame:
        """获取实时数据"""
        if self.primary_adapter:
            return self.primary_adapter.get_realtime_data(codes)
        return pd.DataFrame()
    
    def get_stock_list(self, source: str = None) -> pd.DataFrame:
        """获取股票列表"""
        adapter = self.get_adapter(source)
        if adapter:
            return adapter.get_stock_list()
        return pd.DataFrame()


# 全局数据源管理器
data_source_manager = DataSourceManager()


def init_data_sources(config: Dict = None):
    """初始化数据源"""
    config = config or {}
    
    # 注册Tushare
    tushare_config = config.get('tushare', {})
    if tushare_config.get('enabled', True):
        tushare = TushareAdapter(tushare_config)
        data_source_manager.register_adapter(tushare, primary=True)
    
    # 注册Baostock
    baostock_config = config.get('baostock', {})
    if baostock_config.get('enabled', True):
        baostock = BaostockAdapter(baostock_config)
        data_source_manager.register_adapter(baostock)
    
    logger.info("数据源初始化完成")


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 初始化
    init_data_sources({
        'tushare': {'enabled': True},
        'baostock': {'enabled': True}
    })
    
    # 获取数据
    df = data_source_manager.get_daily_data('000001', '2024-01-01', '2024-01-31')
    print(df.head())
