#!/usr/bin/env python3
"""
异步K线数据获取器

使用 asyncio 实现高并发采集，替代多进程方案
优势:
1. 避免多进程在异步环境中的问题
2. 更轻量的并发模型
3. 更好的资源控制
4. 适合IO密集型任务（网络请求）
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import polars as pl

from .unified_fetcher import get_unified_fetcher
from core.logger import setup_logger
from core.delisting_guard import get_delisting_guard

logger = setup_logger("async_kline_fetcher", log_file="system/async_kline_fetcher.log")


@dataclass
class FetchResult:
    """采集结果"""
    code: str
    success: bool
    rows: int = 0
    status: str = ""
    error: Optional[str] = None


@dataclass
class AsyncConfig:
    """异步采集配置"""
    # 并发控制 - 降低并发避免数据源限流
    max_concurrent: int = 3  # 最大并发数（Baostock限制较严）
    semaphore_value: int = 3  # 信号量限制

    # 批次控制
    batch_size: int = 30  # 每批处理的股票数
    batch_pause: float = 3.0  # 批次间暂停（秒）

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 2.0

    # 请求频率控制 - 增加间隔避免限流
    request_delay: float = 0.5  # 请求间隔（秒）

    # 数据配置
    kline_days: int = 365 * 3
    min_kline_rows: int = 50


class AsyncKlineFetcher:
    """异步K线数据获取器"""
    
    def __init__(self, config: Optional[AsyncConfig] = None):
        self.config = config or AsyncConfig()
        self.semaphore = asyncio.Semaphore(self.config.semaphore_value)
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_rows': 0
        }
    
    async def fetch_single_stock(
        self,
        code: str,
        kline_dir: Path,
        days: int
    ) -> FetchResult:
        """
        获取单只股票的K线数据
        
        Args:
            code: 股票代码
            kline_dir: K线数据保存目录
            days: 获取天数
        
        Returns:
            FetchResult: 采集结果
        """
        async with self.semaphore:  # 控制并发数
            try:
                # 获取增量日期范围
                start_date, end_date, is_incremental = self._get_date_range(
                    code, days, kline_dir
                )
                
                # 如果已经是最新数据，跳过
                if start_date > end_date:
                    self.stats['skipped'] += 1
                    return FetchResult(code=code, success=True, rows=0, status='skipped')
                
                # 获取统一获取器
                fetcher = await get_unified_fetcher()
                
                # 获取K线数据
                df_new = await fetcher.fetch_kline(code, start_date, end_date)
                
                if df_new is None or df_new.empty:
                    self.stats['failed'] += 1
                    return FetchResult(
                        code=code,
                        success=False,
                        rows=0,
                        status='no_data',
                        error='返回数据为空'
                    )
                
                # 验证数据完整性（增量更新时放宽要求）
                is_valid, msg = self._validate_data(df_new, code, is_incremental)
                if not is_valid:
                    self.stats['failed'] += 1
                    return FetchResult(
                        code=code,
                        success=False,
                        rows=0,
                        status='validation_failed',
                        error=msg
                    )
                
                # 合并现有数据（增量模式）
                output_file = kline_dir / f"{code}.parquet"
                if is_incremental and output_file.exists():
                    try:
                        df_existing = pl.read_parquet(output_file).to_pandas()
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        # 兼容两种列名
                        date_col = 'trade_date' if 'trade_date' in df_combined.columns else 'date'
                        df_combined = df_combined.drop_duplicates(subset=[date_col], keep='last')
                        df_combined = df_combined.sort_values(date_col)
                        df_new = df_combined
                    except Exception as e:
                        logger.warning(f"合并{code}数据失败，使用新数据: {e}")
                
                # 保存数据
                if self._save_data(df_new, output_file):
                    self.stats['success'] += 1
                    self.stats['total_rows'] += len(df_new)
                    return FetchResult(
                        code=code,
                        success=True,
                        rows=len(df_new),
                        status='success'
                    )
                else:
                    self.stats['failed'] += 1
                    return FetchResult(
                        code=code,
                        success=False,
                        rows=0,
                        status='save_failed',
                        error='保存失败'
                    )
                
            except Exception as e:
                logger.exception(f"处理{code}时错误")
                self.stats['failed'] += 1
                return FetchResult(
                    code=code,
                    success=False,
                    rows=0,
                    status='error',
                    error=str(e)
                )
            finally:
                # 请求间隔控制
                await asyncio.sleep(self.config.request_delay)
    
    def _get_date_range(
        self,
        code: str,
        days: int,
        kline_dir: Path
    ) -> Tuple[str, str, bool]:
        """
        获取日期范围

        Returns:
            (start_date, end_date, is_incremental)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        output_file = kline_dir / f"{code}.parquet"
        is_incremental = False

        if output_file.exists():
            try:
                df_existing = pl.read_parquet(output_file).to_pandas()
                # 兼容两种列名
                date_col = None
                if 'trade_date' in df_existing.columns:
                    date_col = 'trade_date'
                elif 'date' in df_existing.columns:
                    date_col = 'date'

                if date_col:
                    last_date = pd.to_datetime(df_existing[date_col].max())
                    # 检查数据是否最新（最近3天内）
                    days_since_last = (end_date - last_date).days

                    if days_since_last <= 3:
                        # 数据较新，只获取缺失的日期
                        # 从最后一天的下一天开始
                        start_date = last_date + timedelta(days=1)
                        is_incremental = True
                        logger.debug(f"{code}: 增量更新，从 {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
                    else:
                        # 数据较旧，重新获取完整历史
                        logger.info(f"{code}: 数据较旧（{days_since_last}天未更新），重新获取完整历史")
                        start_date = end_date - timedelta(days=days)
                        is_incremental = False
            except Exception as e:
                logger.warning(f"读取{code}现有数据失败: {e}")

        return (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            is_incremental
        )
    
    def _validate_data(self, df: pd.DataFrame, code: str, is_incremental: bool = False) -> Tuple[bool, str]:
        """验证数据完整性"""
        # 增量更新时放宽行数要求（至少1行）
        min_rows = 1 if is_incremental else self.config.min_kline_rows

        if len(df) < min_rows:
            return False, f"数据行数不足: {len(df)} < {min_rows}"

        # 检查必需列
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少列: {missing_cols}"

        # 检查OHLC逻辑
        ohlc_valid = (
            (df['high'] >= df[['open', 'close']].max(axis=1)) &
            (df[['open', 'close']].max(axis=1) >= df[['open', 'close']].min(axis=1)) &
            (df[['open', 'close']].min(axis=1) >= df['low'])
        ).all()

        if not ohlc_valid:
            return False, "OHLC逻辑错误"

        return True, "验证通过"
    
    def _save_data(self, df: pd.DataFrame, output_file: Path) -> bool:
        """保存数据到Parquet文件"""
        try:
            # 确保目录存在
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 转换为Polars并保存
            df_pl = pl.from_pandas(df)
            df_pl.write_parquet(output_file)
            
            return True
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
            return False
    
    async def fetch_batch(
        self,
        codes: List[str],
        kline_dir: Path,
        days: int
    ) -> List[FetchResult]:
        """
        批量获取K线数据
        
        Args:
            codes: 股票代码列表
            kline_dir: K线数据保存目录
            days: 获取天数
        
        Returns:
            List[FetchResult]: 采集结果列表
        """
        tasks = [
            self.fetch_single_stock(code, kline_dir, days)
            for code in codes
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def fetch_all(
        self,
        codes: List[str],
        kline_dir: Path,
        days: int = None,
        filter_delisted: bool = True
    ) -> Dict:
        """
        获取所有股票的K线数据
        
        Args:
            codes: 股票代码列表
            kline_dir: K线数据保存目录
            days: 获取天数
            filter_delisted: 是否过滤退市股票
        
        Returns:
            Dict: 统计结果
        """
        days = days or self.config.kline_days
        
        # 过滤退市股票
        if filter_delisted:
            delisting_guard = get_delisting_guard()
            original_count = len(codes)
            codes = [code for code in codes if not delisting_guard.is_delisted_by_code(code)]
            filtered_count = original_count - len(codes)
            if filtered_count > 0:
                logger.info(f"已过滤 {filtered_count} 只退市股票，剩余 {len(codes)} 只")
        
        logger.info(f"异步获取K线数据 (最近{days}天, 并发数: {self.config.max_concurrent})")
        
        kline_dir.mkdir(exist_ok=True)
        
        # 分批处理
        batches = [
            codes[i:i+self.config.batch_size]
            for i in range(0, len(codes), self.config.batch_size)
        ]
        
        all_results = []
        
        for i, batch in enumerate(batches, 1):
            logger.info(f"处理批次 {i}/{len(batches)} ({len(batch)} 只股票)")
            
            results = await self.fetch_batch(batch, kline_dir, days)
            all_results.extend(results)
            
            # 批次间暂停
            if i < len(batches):
                await asyncio.sleep(self.config.batch_pause)
            
            # 输出进度
            processed = min(i * self.config.batch_size, len(codes))
            logger.info(
                f"进度: {processed}/{len(codes)}, "
                f"成功 {self.stats['success']}, "
                f"跳过 {self.stats['skipped']}, "
                f"失败 {self.stats['failed']}"
            )
        
        logger.info(
            f"K线数据获取完成: {self.stats['success']}/{len(codes)} 只成功, "
            f"{self.stats['skipped']} 只跳过, {self.stats['failed']} 只失败, "
            f"累计 {self.stats['total_rows']} 行"
        )
        
        return {
            'success': self.stats['success'],
            'skipped': self.stats['skipped'],
            'failed': self.stats['failed'],
            'total_rows': self.stats['total_rows'],
            'results': all_results
        }


# 便捷函数
async def fetch_kline_data_async(
    codes: List[str],
    kline_dir: Path,
    days: int = None,
    max_concurrent: int = 10,
    filter_delisted: bool = True
) -> Dict:
    """
    异步获取K线数据的便捷函数
    
    Args:
        codes: 股票代码列表
        kline_dir: K线数据保存目录
        days: 获取天数
        max_concurrent: 最大并发数
        filter_delisted: 是否过滤退市股票
    
    Returns:
        Dict: 统计结果
    """
    config = AsyncConfig(max_concurrent=max_concurrent, semaphore_value=max_concurrent)
    fetcher = AsyncKlineFetcher(config)
    return await fetcher.fetch_all(codes, kline_dir, days, filter_delisted)
