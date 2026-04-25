#!/usr/bin/env python3
"""
复权因子获取接口

支持：
- 前复权因子获取
- 后复权因子获取
- 复权因子缓存
- 批量获取

数据来源：Tushare pro.adj_factor 接口
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
from dataclasses import dataclass

import pandas as pd
import polars as pl

from core.logger import setup_logger
from core.paths import get_data_path


@dataclass
class AdjustmentFactor:
    """复权因子数据"""
    code: str
    trade_date: str
    adj_factor: float
    
    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'trade_date': self.trade_date,
            'adj_factor': self.adj_factor
        }


class AdjustmentFactorFetcher:
    """复权因子获取器"""
    
    def __init__(self, tushare_token: Optional[str] = None):
        self.logger = setup_logger("adj_factor_fetcher")
        self.data_dir = get_data_path() / "adjustment"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化Tushare
        self._init_tushare(tushare_token)
        
        # 缓存
        self._cache = {}
        self._cache_ttl = 3600  # 缓存1小时
    
    def _init_tushare(self, token: Optional[str] = None):
        """初始化Tushare"""
        try:
            import tushare as ts
            
            if token is None:
                # 从环境变量获取
                import os
                token = os.getenv('TUSHARE_TOKEN')
            
            if token:
                self.pro = ts.pro_api(token)
                self.logger.info("Tushare初始化成功")
            else:
                self.pro = None
                self.logger.warning("未配置Tushare Token")
                
        except ImportError:
            self.pro = None
            self.logger.error("tushare包未安装")
    
    def fetch_adj_factor(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_cache: bool = True
    ) -> Optional[pl.DataFrame]:
        """
        获取复权因子
        
        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            use_cache: 是否使用缓存
        
        Returns:
            复权因子DataFrame
        """
        # 标准化代码
        ts_code = self._standardize_code(code)
        
        # 检查缓存
        cache_key = f"{ts_code}_{start_date}_{end_date}"
        if use_cache and cache_key in self._cache:
            cache_time, data = self._cache[cache_key]
            if time.time() - cache_time < self._cache_ttl:
                self.logger.debug(f"使用缓存: {ts_code}")
                return data
        
        # 检查本地存储
        local_data = self._load_from_local(ts_code, start_date, end_date)
        if local_data is not None and len(local_data) > 0:
            self.logger.debug(f"从本地加载: {ts_code}")
            if use_cache:
                self._cache[cache_key] = (time.time(), local_data)
            return local_data
        
        # 从API获取
        if self.pro is None:
            self.logger.error("Tushare未初始化，无法获取复权因子")
            return None
        
        try:
            self.logger.info(f"从API获取复权因子: {ts_code}")
            
            # 转换日期格式
            start = start_date.replace('-', '') if start_date else None
            end = end_date.replace('-', '') if end_date else None
            
            # 调用Tushare接口
            df = self.pro.adj_factor(
                ts_code=ts_code,
                start_date=start,
                end_date=end
            )
            
            if df is None or len(df) == 0:
                self.logger.warning(f"未获取到复权因子: {ts_code}")
                return None
            
            # 转换为polars
            pl_df = pl.from_pandas(df)
            
            # 标准化列名
            pl_df = pl_df.rename({
                'ts_code': 'code',
                'trade_date': 'date'
            })
            
            # 转换日期格式
            pl_df = pl_df.with_columns([
                pl.col('date').str.to_date(format='%Y%m%d').cast(pl.Utf8).alias('date')
            ])
            
            # 保存到本地
            self._save_to_local(ts_code, pl_df)
            
            # 更新缓存
            if use_cache:
                self._cache[cache_key] = (time.time(), pl_df)
            
            self.logger.info(f"获取复权因子成功: {ts_code}, {len(pl_df)} 条记录")
            
            return pl_df
            
        except Exception as e:
            self.logger.error(f"获取复权因子失败: {ts_code}, {e}")
            return None
    
    def fetch_adj_factor_batch(
        self,
        codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        批量获取复权因子
        
        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            字典 {code: DataFrame}
        """
        results = {}
        
        self.logger.info(f"批量获取复权因子: {len(codes)} 只股票")
        
        for i, code in enumerate(codes):
            result = self.fetch_adj_factor(code, start_date, end_date)
            if result is not None:
                results[code] = result
            
            # 每100只报告一次
            if (i + 1) % 100 == 0:
                self.logger.info(f"   进度: {i+1}/{len(codes)}")
            
            # 限速：每秒最多10次请求
            time.sleep(0.1)
        
        self.logger.info(f"批量获取完成: {len(results)}/{len(codes)} 成功")
        
        return results
    
    def get_latest_adj_factor(self, code: str) -> Optional[float]:
        """
        获取最新复权因子
        
        Args:
            code: 股票代码
        
        Returns:
            最新复权因子值
        """
        # 获取最近30天的数据
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = self.fetch_adj_factor(code, start_date, end_date)
        
        if df is not None and len(df) > 0:
            # 获取最新值
            latest = df.sort('date').tail(1)
            return latest['adj_factor'].item()
        
        return None
    
    def apply_forward_adjustment(
        self,
        kline_df: pl.DataFrame,
        adj_factor_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        应用前复权
        
        Args:
            kline_df: K线数据
            adj_factor_df: 复权因子数据
        
        Returns:
            前复权后的K线数据
        """
        # 合并复权因子
        df = kline_df.join(
            adj_factor_df.select(['date', 'adj_factor']),
            on='date',
            how='left'
        )
        
        # 填充缺失的复权因子（使用前向填充）
        df = df.with_columns([
            pl.col('adj_factor').forward_fill()
        ])
        
        # 获取最新复权因子
        latest_factor = df['adj_factor'].drop_nulls().tail(1).item()
        
        # 应用前复权
        df = df.with_columns([
            (pl.col('open') * pl.col('adj_factor') / latest_factor).alias('open_adj'),
            (pl.col('close') * pl.col('adj_factor') / latest_factor).alias('close_adj'),
            (pl.col('high') * pl.col('adj_factor') / latest_factor).alias('high_adj'),
            (pl.col('low') * pl.col('adj_factor') / latest_factor).alias('low_adj'),
        ])
        
        return df
    
    def apply_backward_adjustment(
        self,
        kline_df: pl.DataFrame,
        adj_factor_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        应用后复权
        
        Args:
            kline_df: K线数据
            adj_factor_df: 复权因子数据
        
        Returns:
            后复权后的K线数据
        """
        # 合并复权因子
        df = kline_df.join(
            adj_factor_df.select(['date', 'adj_factor']),
            on='date',
            how='left'
        )
        
        # 填充缺失的复权因子
        df = df.with_columns([
            pl.col('adj_factor').forward_fill()
        ])
        
        # 应用后复权（直接乘以复权因子）
        df = df.with_columns([
            (pl.col('open') * pl.col('adj_factor')).alias('open_adj'),
            (pl.col('close') * pl.col('adj_factor')).alias('close_adj'),
            (pl.col('high') * pl.col('adj_factor')).alias('high_adj'),
            (pl.col('low') * pl.col('adj_factor')).alias('low_adj'),
        ])
        
        return df
    
    def _standardize_code(self, code: str) -> str:
        """标准化股票代码"""
        code = code.strip()
        
        # 如果已经是Tushare格式，直接返回
        if '.' in code:
            return code
        
        # 判断交易所
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('8') or code.startswith('4'):
            return f"{code}.BJ"
        else:
            # 默认尝试上海
            return f"{code}.SH"
    
    def _load_from_local(
        self,
        ts_code: str,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Optional[pl.DataFrame]:
        """从本地加载复权因子"""
        file_path = self.data_dir / f"{ts_code.replace('.', '_')}.parquet"
        
        if not file_path.exists():
            return None
        
        try:
            df = pl.read_parquet(file_path)
            
            # 过滤日期范围
            if start_date:
                df = df.filter(pl.col('date') >= start_date)
            if end_date:
                df = df.filter(pl.col('date') <= end_date)
            
            return df
            
        except Exception as e:
            self.logger.error(f"加载本地复权因子失败: {ts_code}, {e}")
            return None
    
    def _save_to_local(self, ts_code: str, df: pl.DataFrame):
        """保存到本地"""
        file_path = self.data_dir / f"{ts_code.replace('.', '_')}.parquet"
        
        try:
            # 如果文件存在，合并数据
            if file_path.exists():
                existing = pl.read_parquet(file_path)
                combined = pl.concat([existing, df]).unique(subset=['date'])
                combined.write_parquet(file_path)
            else:
                df.write_parquet(file_path)
            
            self.logger.debug(f"保存复权因子到本地: {ts_code}")
            
        except Exception as e:
            self.logger.error(f"保存复权因子失败: {ts_code}, {e}")
    
    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        self.logger.info("复权因子缓存已清除")


# 全局单例
_adj_factor_fetcher = None


def get_adj_factor_fetcher(token: Optional[str] = None) -> AdjustmentFactorFetcher:
    """获取复权因子获取器单例"""
    global _adj_factor_fetcher
    
    if _adj_factor_fetcher is None:
        _adj_factor_fetcher = AdjustmentFactorFetcher(token)
    
    return _adj_factor_fetcher


# 便捷函数
def fetch_adj_factor(code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """便捷函数：获取复权因子"""
    fetcher = get_adj_factor_fetcher()
    return fetcher.fetch_adj_factor(code, start_date, end_date)


def apply_forward_adjustment(kline_df: pl.DataFrame, code: str) -> pl.DataFrame:
    """便捷函数：应用前复权"""
    fetcher = get_adj_factor_fetcher()
    
    # 获取日期范围
    dates = kline_df['date'].to_list()
    start_date = min(dates)
    end_date = max(dates)
    
    # 获取复权因子
    adj_factor_df = fetcher.fetch_adj_factor(code, start_date, end_date)
    
    if adj_factor_df is not None:
        return fetcher.apply_forward_adjustment(kline_df, adj_factor_df)
    
    return kline_df


if __name__ == "__main__":
    # 测试
    fetcher = AdjustmentFactorFetcher()
    
    # 获取单只股票复权因子
    result = fetcher.fetch_adj_factor("000001", "2024-01-01", "2024-12-31")
    
    if result is not None:
        print(f"获取到 {len(result)} 条复权因子记录")
        print(result.head())
        
        # 获取最新复权因子
        latest = fetcher.get_latest_adj_factor("000001")
        print(f"\n最新复权因子: {latest}")
