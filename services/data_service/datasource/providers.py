#!/usr/bin/env python3
"""
数据源提供商实现
支持Baostock、AKShare、腾讯等多个数据源
"""
import importlib
import time
import asyncio
import functools
import pandas as pd
from typing import Optional, List, Dict, Callable, Any
from datetime import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass

from core.logger import setup_logger
from core.network_config import get_network_config

logger = setup_logger("data_providers", log_file="system/providers.log")

# 初始化网络配置（禁用代理）
_network_config = get_network_config()
if _network_config.is_proxy_enabled():
    logger.info("检测到代理配置，已禁用代理以确保数据源连接稳定")
    _network_config.disable_proxy()


def retry_on_network_error(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    网络错误重试装饰器

    Args:
        max_retries: 最大重试次数
        delay: 初始延迟（秒）
        backoff: 延迟增长倍数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_msg = str(e).lower()

                    # 检查是否是网络相关错误
                    is_network_error = any(
                        keyword in error_msg
                        for keyword in ['connection', 'timeout', 'remote', 'reset', 'aborted', 'refused']
                    )

                    if not is_network_error or attempt >= max_retries:
                        raise

                    logger.warning(f"{func.__name__} 网络错误 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff

            raise last_exception

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_msg = str(e).lower()

                    # 检查是否是网络相关错误
                    is_network_error = any(
                        keyword in error_msg
                        for keyword in ['connection', 'timeout', 'remote', 'reset', 'aborted', 'refused']
                    )

                    if not is_network_error or attempt >= max_retries:
                        raise

                    logger.warning(f"{func.__name__} 网络错误 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(current_delay)
                    current_delay *= backoff

            raise last_exception

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


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
    async def fetch_kline(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
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
        """获取股票列表（自动过滤退市股票和指数）"""
        if not self._login():
            return pd.DataFrame()

        try:
            stocks = []
            # 使用最近的交易日
            query_date = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            rs = self._bs.query_all_stock(day=query_date)

            while rs.next():
                row = rs.get_row_data()
                # row: [code, tradeStatus, code_name]
                full_code = row[0]  # e.g., 'sh.000001'
                trade_status = row[1] if len(row) > 1 else '0'  # '1' = 正常交易
                name = row[2] if len(row) > 2 else ''

                code = full_code.split('.')[-1]
                exchange = 'sh' if full_code.startswith('sh') else 'sz'

                # 过滤指数和ETF基金
                # 指数：000/880/999 开头（上证）, 399 开头（深证）
                if code.startswith(('000', '880', '999')) and exchange == 'sh':
                    continue
                if code.startswith('399') and exchange == 'sz':
                    continue
                # ETF基金：51/52/56/58/59 开头（上海）, 15/16/18 开头（深圳）
                if code.startswith(('51', '52', '56', '58', '59')) and exchange == 'sh':
                    continue
                if code.startswith(('15', '16', '18')) and exchange == 'sz':
                    continue
                # 可转债：11 开头（上海）, 12 开头（深圳）
                if code.startswith('11') and exchange == 'sh':
                    continue
                if code.startswith('12') and exchange == 'sz':
                    continue
                # B股：90 开头（上海）, 20 开头（深圳）
                if code.startswith('90') and exchange == 'sh':
                    continue
                if code.startswith('20') and exchange == 'sz':
                    continue

                # 过滤非交易状态的股票
                if trade_status != '1':
                    logger.debug(f"过滤非交易状态股票: {code} {name} (状态: {trade_status})")
                    continue

                # 过滤退市股票
                if self._is_delisted(name):
                    logger.debug(f"过滤退市股票: {code} {name}")
                    continue

                stocks.append({
                    'code': code,
                    'name': name,
                    'trade_status': trade_status,
                    'exchange': exchange
                })

            df = pd.DataFrame(stocks)
            if not df.empty:
                logger.info(f"Baostock获取股票列表: {len(df)} 只（已过滤退市/指数）")
            return df
        except Exception as e:
            logger.error(f"Baostock获取股票列表失败: {e}")
            return pd.DataFrame()
        finally:
            self._logout()

    def _is_delisted(self, name: str) -> bool:
        """
        检查股票是否已退市

        Args:
            name: 股票名称

        Returns:
            True 如果股票已退市
        """
        if not name:
            return False  # 空名称不排除，可能数据问题

        # 退市关键词列表
        delisting_keywords = [
            '退市', '退', '*ST', 'ST', 'PT', '终止上市',
            '摘牌', '作废', '注销', '解散', '破产'
        ]

        name_upper = str(name).upper()
        return any(keyword.upper() in name_upper for keyword in delisting_keywords)
    
    @retry_on_network_error(max_retries=3, delay=0.5, backoff=2.0)
    async def fetch_kline(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
        """获取K线数据"""
        if not self._login():
            return pd.DataFrame()

        try:
            code_bs = self._convert_code(code)
            bs_frequency = "d" if frequency == 'd' else frequency  # 日线
            # 简化字段，避免索引混乱
            fields = "date,open,high,low,close,volume"

            rs = self._bs.query_history_k_data_plus(
                code_bs,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=bs_frequency,
                adjustflag="3"  # 后复权
            )

            if rs.error_code != '0':
                logger.warning(f"Baostock查询失败: {rs.error_msg}")
                return pd.DataFrame()

            data = []
            while rs.next():
                row = rs.get_row_data()
                data.append({
                    'code': code,
                    'date': row[0],
                    'open': float(row[1]) if row[1] else 0,
                    'high': float(row[2]) if row[2] else 0,
                    'low': float(row[3]) if row[3] else 0,
                    'close': float(row[4]) if row[4] else 0,
                    'volume': int(float(row[5])) if row[5] else 0
                })

            df = pd.DataFrame(data)
            if not df.empty:
                logger.debug(f"Baostock获取 {code} 成功: {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"Baostock获取K线失败 {code}: {e}")
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

    def fetch_kline_sync(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
        """同步获取K线数据"""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.fetch_kline(code, start_date, end_date, frequency))


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
    
    @retry_on_network_error(max_retries=3, delay=0.5, backoff=2.0)
    async def fetch_kline(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
        """获取K线"""
        try:
            ak = self._get_ak()
            period = "daily" if frequency == 'd' else frequency
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
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
        import os

        # 临时清除代理环境变量
        old_env = {k: os.environ.pop(k, None) for k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']}

        start = time.time()
        try:
            url = 'https://qt.gtimg.cn/q=sh600000'
            response = requests.get(url, timeout=5, proxies={})
            self.status.latency_ms = (time.time() - start) * 1000
            self.status.available = response.status_code == 200
            self.status.last_check = datetime.now()
            return self.status.available
        except Exception as e:
            self.status.last_error = str(e)
            self.status.available = False
            return False
        finally:
            # 恢复环境变量
            for k, v in old_env.items():
                if v is not None:
                    os.environ[k] = v
    
    async def fetch_stock_list(self) -> pd.DataFrame:
        """腾讯不提供股票列表API"""
        return pd.DataFrame()
    
    @retry_on_network_error(max_retries=3, delay=0.5, backoff=2.0)
    async def fetch_kline(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
        """获取K线"""
        try:
            import requests
            import json
            import re
            import time
            import os

            # 临时清除代理环境变量，避免 SOCKS 依赖问题
            old_env = {k: os.environ.pop(k, None) for k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']}

            symbol = f"sh{code}" if code.startswith('6') else f"sz{code}"
            url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'

            # 根据frequency参数调整
            freq_map = {'d': 'day', 'w': 'week', 'm': 'month'}
            period = freq_map.get(frequency, 'day')

            # 使用与原脚本相同的参数格式
            params = {
                '_var': f'kline_dayqfq_{symbol}',
                'param': f'{symbol},day,,,500,qfq',
                'r': str(int(time.time() * 1000))
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://gu.qq.com/'
            }

            # 显式禁用代理，避免 SOCKS 依赖问题
            response = requests.get(url, params=params, headers=headers, timeout=30, proxies={})
            text = response.text
            match = re.match(rf'kline_dayqfq_\w+=(.*)', text)

            if match:
                data = json.loads(match.group(1))
                if data.get('code') == 0:
                    # 使用 qfqday 字段（与原脚本一致）
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
                            'volume': int(float(k[5]))
                        })
                    df = pd.DataFrame(records)
                    if not df.empty:
                        logger.debug(f"Tencent获取 {code} 成功: {len(df)} 条")
                    return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Tencent获取K线失败 {code}: {e}")
            return pd.DataFrame()
        finally:
            # 恢复环境变量
            for k, v in old_env.items():
                if v is not None:
                    os.environ[k] = v
    
    async def fetch_fundamental(self, code: str) -> Dict:
        """腾讯不提供基本面API"""
        return {}
    
    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """获取实时行情"""
        # 腾讯实时行情需要批量查询，这里简化处理
        return pd.DataFrame()

    def fetch_kline_sync(self, code: str, start_date: str, end_date: str, frequency: str = 'd') -> pd.DataFrame:
        """同步获取K线数据"""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.fetch_kline(code, start_date, end_date, frequency))


# 别名导出，兼容测试代码
TushareProvider = BaostockProvider
AkShareProvider = AKShareProvider
BaoStockProvider = BaostockProvider
