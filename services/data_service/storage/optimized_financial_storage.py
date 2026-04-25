#!/usr/bin/env python3
"""
优化的财务数据存储管理器

性能优化:
- 内存缓存: 热点数据常驻内存
- 批量加载: 支持多股票批量查询
- 索引优化: 按日期和代码建立索引
- 延迟加载: 按需加载数据

存储格式: Parquet (按股票代码分片)
存储路径: data/financial/
"""
import pandas as pd
import polars as pl
from typing import List, Dict, Optional, Union, Set
from pathlib import Path
from datetime import datetime, timedelta
from functools import lru_cache
import json
import hashlib
import threading

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.storage.financial_storage import FinancialStorageManager

logger = setup_logger("optimized_financial_storage", log_file="system/optimized_financial_storage.log")


class MemoryCache:
    """简单的内存缓存"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        self.max_size = max_size
        self.ttl = ttl  # 缓存过期时间(秒)
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.RLock()
    
    def _make_key(self, *args, **kwargs) -> str:
        """生成缓存键"""
        key_str = str(args) + str(sorted(kwargs.items()))
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, *args, **kwargs) -> Optional[Any]:
        """获取缓存"""
        key = self._make_key(*args, **kwargs)
        
        with self._lock:
            if key in self._cache:
                item = self._cache[key]
                # 检查是否过期
                if datetime.now().timestamp() - item['timestamp'] < self.ttl:
                    return item['data']
                else:
                    del self._cache[key]
        
        return None
    
    def set(self, data: Any, *args, **kwargs):
        """设置缓存"""
        key = self._make_key(*args, **kwargs)
        
        with self._lock:
            # 清理过期数据
            self._cleanup()
            
            # 如果缓存已满,删除最旧的
            if len(self._cache) >= self.max_size:
                oldest_key = min(self._cache.keys(), 
                               key=lambda k: self._cache[k]['timestamp'])
                del self._cache[oldest_key]
            
            self._cache[key] = {
                'data': data,
                'timestamp': datetime.now().timestamp()
            }
    
    def _cleanup(self):
        """清理过期缓存"""
        now = datetime.now().timestamp()
        expired_keys = [
            k for k, v in self._cache.items() 
            if now - v['timestamp'] > self.ttl
        ]
        for k in expired_keys:
            del self._cache[k]
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()
    
    def get_stats(self) -> Dict:
        """获取缓存统计"""
        with self._lock:
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'ttl': self.ttl
            }


class OptimizedFinancialStorageManager(FinancialStorageManager):
    """优化的财务数据存储管理器"""
    
    def __init__(self, data_dir: Optional[str] = None, enable_cache: bool = True):
        super().__init__(data_dir)
        
        # 内存缓存
        self._cache_enabled = enable_cache
        self._cache = MemoryCache(max_size=1000, ttl=3600) if enable_cache else None
        
        # 索引缓存
        self._code_index: Optional[Set[str]] = None
        self._index_last_update: Optional[datetime] = None
        
        self.logger.info(f"优化版财务存储管理器初始化完成 (缓存: {enable_cache})")
    
    def _get_cache_key(self, code: str, data_type: str) -> str:
        """生成缓存键"""
        return f"{data_type}:{code}"
    
    def load_balance_sheet(
        self, 
        code: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        加载资产负债表 (带缓存)
        
        Args:
            code: 股票代码
            use_cache: 是否使用缓存
        
        Returns:
            资产负债表DataFrame
        """
        cache_key = self._get_cache_key(code, 'balance_sheet')
        
        # 尝试从缓存读取
        if use_cache and self._cache_enabled:
            cached = self._cache.get('balance_sheet', code)
            if cached is not None:
                self.logger.debug(f"{code} 资产负债表缓存命中")
                return cached
        
        # 从文件加载
        df = super().load_balance_sheet(code)
        
        # 写入缓存
        if use_cache and self._cache_enabled and not df.empty:
            self._cache.set(df.copy(), 'balance_sheet', code)
        
        return df
    
    def load_income_statement(
        self, 
        code: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """加载利润表 (带缓存)"""
        cache_key = self._get_cache_key(code, 'income_statement')
        
        if use_cache and self._cache_enabled:
            cached = self._cache.get('income_statement', code)
            if cached is not None:
                return cached
        
        df = super().load_income_statement(code)
        
        if use_cache and self._cache_enabled and not df.empty:
            self._cache.set(df.copy(), 'income_statement', code)
        
        return df
    
    def load_cash_flow(
        self, 
        code: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """加载现金流量表 (带缓存)"""
        cache_key = self._get_cache_key(code, 'cash_flow')
        
        if use_cache and self._cache_enabled:
            cached = self._cache.get('cash_flow', code)
            if cached is not None:
                return cached
        
        df = super().load_cash_flow(code)
        
        if use_cache and self._cache_enabled and not df.empty:
            self._cache.set(df.copy(), 'cash_flow', code)
        
        return df
    
    def load_indicators(
        self, 
        code: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """加载财务指标 (带缓存)"""
        cache_key = self._get_cache_key(code, 'indicators')
        
        if use_cache and self._cache_enabled:
            cached = self._cache.get('indicators', code)
            if cached is not None:
                return cached
        
        df = super().load_indicators(code)
        
        if use_cache and self._cache_enabled and not df.empty:
            self._cache.set(df.copy(), 'indicators', code)
        
        return df
    
    def batch_load_indicators(
        self,
        codes: List[str],
        as_of_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载财务指标
        
        Args:
            codes: 股票代码列表
            as_of_date: 指定日期 (格式: YYYY-MM-DD), None表示最新
        
        Returns:
            {code: indicators_df}
        """
        results = {}
        
        # 批量加载,减少IO次数
        for code in codes:
            try:
                df = self.load_indicators(code)
                
                if not df.empty and as_of_date:
                    # 筛选指定日期之前的数据
                    df = df[df['report_date'] <= as_of_date]
                
                if not df.empty:
                    results[code] = df
                    
            except Exception as e:
                self.logger.debug(f"{code} 批量加载失败: {e}")
        
        self.logger.info(f"批量加载完成: {len(results)}/{len(codes)} 只股票")
        return results
    
    def get_available_codes(self, data_type: str = 'balance_sheet') -> List[str]:
        """
        获取可用的股票代码列表 (带索引缓存)
        
        Args:
            data_type: 数据类型
        
        Returns:
            股票代码列表
        """
        # 检查索引是否需要更新 (每5分钟更新一次)
        if (self._code_index is None or 
            self._index_last_update is None or
            datetime.now() - self._index_last_update > timedelta(minutes=5)):
            
            # 更新索引
            codes = super().get_available_codes(data_type)
            self._code_index = set(codes)
            self._index_last_update = datetime.now()
            
            self.logger.info(f"代码索引已更新: {len(codes)} 只股票")
        
        return list(self._code_index)
    
    def clear_cache(self):
        """清空缓存"""
        if self._cache:
            self._cache.clear()
            self.logger.info("缓存已清空")
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        if self._cache:
            return self._cache.get_stats()
        return {'enabled': False}
    
    def preload_hot_data(self, codes: List[str]):
        """
        预加载热点数据到缓存
        
        Args:
            codes: 热点股票代码列表
        """
        if not self._cache_enabled:
            return
        
        self.logger.info(f"开始预加载 {len(codes)} 只股票的财务数据")
        
        loaded = 0
        for code in codes:
            try:
                # 预加载指标数据
                df = super().load_indicators(code)
                if not df.empty:
                    self._cache.set(df, 'indicators', code)
                    loaded += 1
            except Exception as e:
                self.logger.debug(f"{code} 预加载失败: {e}")
        
        self.logger.info(f"预加载完成: {loaded}/{len(codes)} 只股票")


class FinancialDataIndex:
    """财务数据索引"""
    
    def __init__(self, storage_manager: OptimizedFinancialStorageManager):
        self.storage = storage_manager
        self.logger = logger
        
        # 索引数据
        self._date_index: Dict[str, List[str]] = {}  # {date: [codes]}
        self._indicator_index: Dict[str, Dict] = {}  # {code: latest_indicators}
    
    def build_date_index(self, indicator: str = 'roe', threshold: float = 15.0):
        """
        按日期建立索引
        
        Args:
            indicator: 指标名称
            threshold: 阈值
        
        Returns:
            符合条件的股票按日期分组
        """
        codes = self.storage.get_available_codes('indicators')
        
        for code in codes:
            try:
                df = self.storage.load_indicators(code)
                if df.empty:
                    continue
                
                for _, row in df.iterrows():
                    date = row['report_date']
                    value = row.get(indicator)
                    
                    if value and value >= threshold:
                        if date not in self._date_index:
                            self._date_index[date] = []
                        self._date_index[date].append(code)
                        
            except Exception as e:
                self.logger.debug(f"{code} 索引构建失败: {e}")
        
        self.logger.info(f"日期索引构建完成: {len(self._date_index)} 个日期")
        return self._date_index
    
    def get_codes_by_date(
        self,
        date: str,
        indicator: str = 'roe',
        min_value: float = 15.0
    ) -> List[str]:
        """获取某日期符合条件的股票"""
        if date in self._date_index:
            return self._date_index[date]
        
        # 如果没有索引,实时查询
        codes = self.storage.get_available_codes('indicators')
        result = []
        
        for code in codes:
            try:
                df = self.storage.load_indicators(code)
                if df.empty:
                    continue
                
                # 查找指定日期的数据
                date_data = df[df['report_date'] == date]
                if not date_data.empty:
                    value = date_data.iloc[0].get(indicator)
                    if value and value >= min_value:
                        result.append(code)
                        
            except Exception as e:
                self.logger.debug(f"{code} 查询失败: {e}")
        
        return result


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 优化版财务数据存储管理器")
    print("=" * 60)
    
    storage = OptimizedFinancialStorageManager(enable_cache=True)
    
    # 测试缓存
    print("\n1. 测试缓存功能:")
    import time
    
    code = "000001"
    
    # 第一次加载 (无缓存)
    start = time.time()
    df1 = storage.load_indicators(code)
    time1 = time.time() - start
    print(f"首次加载: {time1:.3f}s, 记录数: {len(df1)}")
    
    # 第二次加载 (有缓存)
    start = time.time()
    df2 = storage.load_indicators(code)
    time2 = time.time() - start
    print(f"缓存加载: {time2:.3f}s, 记录数: {len(df2)}")
    print(f"缓存加速: {time1/time2:.1f}x")
    
    # 测试批量加载
    print("\n2. 测试批量加载:")
    codes = ["000001", "000002", "600000"]
    results = storage.batch_load_indicators(codes)
    print(f"批量加载完成: {len(results)} 只股票")
    
    # 测试缓存统计
    print("\n3. 缓存统计:")
    stats = storage.get_cache_stats()
    print(f"缓存大小: {stats['size']}/{stats['max_size']}")
    
    # 测试索引
    print("\n4. 测试日期索引:")
    index = FinancialDataIndex(storage)
    codes = index.get_codes_by_date("2024-03-31", indicator="roe", min_value=15.0)
    print(f"2024-03-31 ROE>=15% 的股票: {len(codes)} 只")
