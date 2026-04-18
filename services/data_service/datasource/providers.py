#!/usr/bin/env python3
"""
数据源提供商实现
支持Baostock、AKShare、腾讯等多个数据源
"""
import importlib
import pandas as pd
from typing import Optional, List, Dict
from datetime import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass

from core.logger import setup_logger

logger = setup_logger("data_providers", log_file="system/providers.log")


@dataclass
class DataSourceStatus:
    """数据源状态"""
    name: str
    available: bool
    latency_ms: float
    last_error: str = ""
    last_check: datetime = None


class DataProvider(ABC):
    """数据源基类"""
    
    def __init__(self, name: str):
        self.name = name
        self.status = DataSourceStatus(
            name=name,
            available=False,
            latency_ms=0,
            last_check=datetime.now()
        )
    
    @abstractmethod
    async def health_check(self) -> bool:
        """健康检查"""
        pass
    
    @abstractmethod
    async def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    async def fetch_kline(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取K线数据"""
        pass
    
    @abstractmethod
    async def fetch_fundamental(self, code: str) -> Dict:
        """获取基本面数据"""
        pass
    
    @abstractmethod
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """获取实时行情"""
        pass


class BaostockProvider(DataProvider):
    """Baostock数据源"""
    
    def __init__(self):
        super().__init__("Baostock")
        self._bs = None
        self._logged_in = False
    
    def _login(self) -> bool:
        """登录"""
        if self._logged_in:
            return True
        try:
            bs = importlib.import_module("baostock")
            lg = bs.login()
            if lg.error_code == '0':
                self._bs = bs
                self._logged_in = True
                return True
        except Exception as e:
            logger.error(f"Baostock登录失败: {e}")
        return False
    
    def _logout(self):
        """登出"""
        if self._logged_in and self._bs:
            try:
                self._bs.logout()
            except:
                pass
            self._logged_in = False
            self._bs = None
    
    def _convert_code(self, code: str) -> str:
        """转换代码格式"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz.{code}"
        return code
    
    async def health_check(self) -> bool:
        """健康检查"""
        import time
        start = time.time()
        try:
            if self._login():
                # 查询一个已知股票测试
                rs = self._bs.query_history_k_data_plus(
                    "sh.600000",
                    "date",
                    start_date=datetime.now().strftime('%Y-%m-%d'),
                    end_date=datetime.now().strftime('%Y-%m-%d')
                )
                self.status.latency_ms = (time.time() - start) * 1000
                self.status.available = rs.error_code == '0'
                self.status.last_check = datetime.now()
                return self.status.available
        except Exception as e:
            self.status.last_error = str(e)
            self.status.available = False
        return False
    
    async def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self._login():
            return pd.DataFrame()
        
        try:
            stocks = []
            rs = self._bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            while rs.next():
                row = rs.get_row_data()
                code = row[0].split('.')[-1]
                stocks.append({
                    'code': code,
                    'name': row[1],
                    'industry': row[2] if len(row) > 2 else '',
                    'exchange': 'sh' if row[0].startswith('sh') else 'sz'
                })
            return pd.DataFrame(stocks)
        except Exception as e:
            logger.error(f"Baostock获取股票列表失败: {e}")
            return pd.DataFrame()
        finally:
            self._logout()
    
    async def fetch_kline(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取K线"""
        if not self._login():
            return pd.DataFrame()
        
        try:
            code_bs = self._convert_code(code)
            rs = self._bs.query_history_k_data_plus(
                code_bs,
                "date,code,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d"
            )
            
            data = []
            while rs.next():
                row = rs.get_row_data()
                data.append({
                    'code': code,
                    'date': row[0],
                    'open': float(row[2]) if row[2] else 0,
                    'high': float(row[3]) if row[3] else 0,
                    'low': float(row[4]) if row[4] else 0,
                    'close': float(row[5]) if row[5] else 0,
                    'volume': int(row[6]) if row[6] else 0,
                    'amount': float(row[7]) if row[7] else 0
                })
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Baostock获取K线失败: {e}")
            return pd.DataFrame()
    
    async def fetch_fundamental(self, code: str) -> Dict:
        """获取基本面"""
        if not self._login():
            return {}
        
        try:
            code_bs = self._convert_code(code)
            date_str = datetime.now().strftime('%Y-%m-%d')
            rs = self._bs.query_history_k_data_plus(
                code_bs,
                "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                start_date=date_str,
                end_date=date_str,
                frequency="d"
            )
            
            while rs.next():
                row = rs.get_row_data()
                return {
                    'code': code,
                    'pe_ttm': float(row[2]) if row[2] else None,
                    'pb': float(row[3]) if row[3] else None,
                    'ps_ttm': float(row[4]) if row[4] else None,
                    'pcf': float(row[5]) if row[5] else None,
                    'date': row[0]
                }
            return {}
        except Exception as e:
            logger.error(f"Baostock获取基本面失败: {e}")
            return {}
    
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """Baostock不支持实时行情"""
        return pd.DataFrame()


class AKShareProvider(DataProvider):
    """AKShare数据源"""
    
    def __init__(self):
        super().__init__("AKShare")
        self._ak = None
    
    def _get_ak(self):
        """延迟加载"""
        if self._ak is None:
            self._ak = importlib.import_module("akshare")
        return self._ak
    
    async def health_check(self) -> bool:
        """健康检查"""
        import time
        start = time.time()
        try:
            ak = self._get_ak()
            # 测试获取一只股票的实时行情
            df = ak.stock_zh_a_spot_em()
            self.status.latency_ms = (time.time() - start) * 1000
            self.status.available = not df.empty
            self.status.last_check = datetime.now()
            return self.status.available
        except Exception as e:
            self.status.last_error = str(e)
            self.status.available = False
            return False
    
    async def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        try:
            ak = self._get_ak()
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                return pd.DataFrame({
                    'code': df['代码'].astype(str),
                    'name': df['名称'],
                    'industry': df.get('所属行业', ''),
                    'exchange': df['代码'].apply(lambda x: 'sh' if str(x).startswith('6') else 'sz')
                })
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"AKShare获取股票列表失败: {e}")
            return pd.DataFrame()
    
    async def fetch_kline(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取K线"""
        try:
            ak = self._get_ak()
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )
            if not df.empty:
                df = df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount'
                })
                df['code'] = code
            return df
        except Exception as e:
            logger.error(f"AKShare获取K线失败: {e}")
            return pd.DataFrame()
    
    async def fetch_fundamental(self, code: str) -> Dict:
        """获取基本面"""
        try:
            ak = self._get_ak()
            # 从实时行情获取估值数据
            df = ak.stock_zh_a_spot_em()
            row = df[df['代码'] == code]
            if not row.empty:
                return {
                    'code': code,
                    'pe_ttm': float(row['市盈率-动态'].values[0]) if '市盈率-动态' in row else None,
                    'pb': float(row['市净率'].values[0]) if '市净率' in row else None,
                    'ps_ttm': None,  # AKShare可能不提供
                    'pcf': None,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
            return {}
        except Exception as e:
            logger.error(f"AKShare获取基本面失败: {e}")
            return {}
    
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """获取实时行情"""
        try:
            ak = self._get_ak()
            df = ak.stock_zh_a_spot_em()
            return df
        except Exception as e:
            logger.error(f"AKShare获取实时行情失败: {e}")
            return pd.DataFrame()


class TencentProvider(DataProvider):
    """腾讯数据源"""
    
    def __init__(self):
        super().__init__("Tencent")
    
    async def health_check(self) -> bool:
        """健康检查"""
        import time
        import requests
        start = time.time()
        try:
            url = 'https://qt.gtimg.cn/q=sh600000'
            response = requests.get(url, timeout=5)
            self.status.latency_ms = (time.time() - start) * 1000
            self.status.available = response.status_code == 200
            self.status.last_check = datetime.now()
            return self.status.available
        except Exception as e:
            self.status.last_error = str(e)
            self.status.available = False
            return False
    
    async def fetch_stock_list(self) -> pd.DataFrame:
        """腾讯不提供股票列表API"""
        return pd.DataFrame()
    
    async def fetch_kline(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取K线"""
        try:
            import requests
            import json
            import re
            
            symbol = f"sh{code}" if code.startswith('6') else f"sz{code}"
            url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
            params = {
                '_var': f'kline_dayqfq_{symbol}',
                'param': f'{symbol},day,,,500,qfq'
            }
            
            response = requests.get(url, params=params, timeout=30)
            text = response.text
            match = re.match(r'kline_dayqfq_\w+=(.*)', text)
            
            if match:
                data = json.loads(match.group(1))
                if data.get('code') == 0:
                    klines = data['data'][symbol].get('qfqday', [])
                    records = []
                    for k in klines:
                        records.append({
                            'code': code,
                            'date': k[0],
                            'open': float(k[1]),
                            'close': float(k[2]),
                            'high': float(k[3]),
                            'low': float(k[4]),
                            'volume': int(k[5])
                        })
                    return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Tencent获取K线失败: {e}")
            return pd.DataFrame()
    
    async def fetch_fundamental(self, code: str) -> Dict:
        """腾讯不提供基本面API"""
        return {}
    
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """获取实时行情"""
        # 腾讯实时行情需要批量查询，这里简化处理
        return pd.DataFrame()


# 别名导出，兼容测试代码
TushareProvider = BaostockProvider
AkShareProvider = AKShareProvider
BaoStockProvider = BaostockProvider
