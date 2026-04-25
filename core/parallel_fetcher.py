"""
并行数据采集引擎

提供高性能的批量数据采集能力，支持：
- 异步并发请求
- 流量控制
- 指数退避重试
- 批量任务管理
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from .async_http import (
    AsyncHTTPClient,
    BatchRequestManager,
    RateLimiter,
    ExponentialBackoff,
    RetryConfig,
    RateLimitConfig
)


@dataclass
class FetchTask:
    """采集任务"""
    identifier: str
    url: str
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    priority: int = 0
    retries: int = 0
    max_retries: int = 3


@dataclass
class FetchResult:
    """采集结果"""
    identifier: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    retries: int = 0


@dataclass
class ParallelFetcherConfig:
    """并行采集器配置"""
    # 并发控制
    max_concurrent: int = 50
    batch_size: int = 100
    
    # 流量控制
    calls_per_minute: int = 480
    burst_size: int = 50
    
    # 重试配置
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    
    # 超时配置
    request_timeout: int = 30
    
    # 连接池配置
    pool_size: int = 100
    
    # 批次间暂停（秒）
    batch_pause: float = 0.1


class ParallelDataFetcher:
    """
    并行数据采集器
    
    用于批量采集股票数据，支持高并发和自动重试。
    """
    
    def __init__(self, config: Optional[ParallelFetcherConfig] = None):
        self.config = config or ParallelFetcherConfig()
        self.http_client: Optional[AsyncHTTPClient] = None
        self.batch_manager: Optional[BatchRequestManager] = None
        self.rate_limiter: Optional[RateLimiter] = None
        
        # 统计信息
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'retried': 0,
            'total_duration_ms': 0,
        }
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def initialize(self):
        """初始化采集器"""
        # 初始化 HTTP 客户端
        retry_config = RetryConfig(
            max_retries=self.config.max_retries,
            base_delay=self.config.base_delay,
            max_delay=self.config.max_delay
        )
        
        self.http_client = AsyncHTTPClient(
            pool_size=self.config.pool_size,
            timeout=self.config.request_timeout,
            retry_config=retry_config
        )
        await self.http_client.open()
        
        # 初始化批量请求管理器
        self.batch_manager = BatchRequestManager(
            client=self.http_client,
            max_concurrency=self.config.max_concurrent,
            batch_size=self.config.batch_size
        )
        
        # 初始化流量限制器
        rate_config = RateLimitConfig(
            calls_per_minute=self.config.calls_per_minute,
            burst_size=self.config.burst_size
        )
        self.rate_limiter = RateLimiter(rate_config)
        
        logger.info(
            f"ParallelDataFetcher initialized: "
            f"max_concurrent={self.config.max_concurrent}, "
            f"batch_size={self.config.batch_size}"
        )
    
    async def close(self):
        """关闭采集器"""
        if self.http_client:
            await self.http_client.close()
            self.http_client = None
        logger.info("ParallelDataFetcher closed")
    
    async def fetch_single(
        self,
        identifier: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None
    ) -> FetchResult:
        """
        采集单个任务
        
        Args:
            identifier: 任务标识
            url: 请求 URL
            params: 请求参数
            headers: 请求头
            
        Returns:
            FetchResult: 采集结果
        """
        start_time = time.monotonic()
        retries = 0
        
        backoff = ExponentialBackoff(RetryConfig(
            max_retries=self.config.max_retries,
            base_delay=self.config.base_delay,
            max_delay=self.config.max_delay
        ))
        
        while True:
            try:
                # 流量控制
                await self.rate_limiter.acquire()
                
                # 执行请求
                data = await self.http_client.get(url, params=params, headers=headers)
                
                duration_ms = (time.monotonic() - start_time) * 1000
                
                self.stats['total'] += 1
                self.stats['success'] += 1
                self.stats['total_duration_ms'] += duration_ms
                
                return FetchResult(
                    identifier=identifier,
                    success=True,
                    data=data,
                    duration_ms=duration_ms,
                    retries=retries
                )
                
            except Exception as e:
                retries += 1
                
                if not backoff.should_retry():
                    duration_ms = (time.monotonic() - start_time) * 1000
                    
                    self.stats['total'] += 1
                    self.stats['failed'] += 1
                    self.stats['retried'] += retries - 1
                    
                    logger.error(f"Fetch failed after {retries} retries: {identifier} - {e}")
                    
                    return FetchResult(
                        identifier=identifier,
                        success=False,
                        error=str(e),
                        duration_ms=duration_ms,
                        retries=retries - 1
                    )
                
                # 等待后重试
                delay = backoff.next_delay()
                logger.warning(f"Fetch failed, retrying in {delay:.1f}s: {identifier} - {e}")
                await asyncio.sleep(delay)
    
    async def fetch_many(
        self,
        tasks: List[FetchTask]
    ) -> List[FetchResult]:
        """
        批量采集
        
        Args:
            tasks: 采集任务列表
            
        Returns:
            List[FetchResult]: 采集结果列表
        """
        # 按优先级排序
        sorted_tasks = sorted(tasks, key=lambda t: t.priority, reverse=True)
        
        # 分批处理
        results = []
        batches = [
            sorted_tasks[i:i + self.config.batch_size]
            for i in range(0, len(sorted_tasks), self.config.batch_size)
        ]
        
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} tasks)")
            
            # 创建并发任务
            batch_tasks = [
                self.fetch_single(
                    task.identifier,
                    task.url,
                    task.params,
                    task.headers
                )
                for task in batch
            ]
            
            # 并发执行
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 处理结果
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Unexpected error: {result}")
                    results.append(FetchResult(
                        identifier="unknown",
                        success=False,
                        error=str(result)
                    ))
                else:
                    results.append(result)
            
            # 批次间暂停
            if i < len(batches) - 1:
                await asyncio.sleep(self.config.batch_pause)
        
        return results
    
    async def fetch_with_callback(
        self,
        tasks: List[FetchTask],
        callback: Optional[Callable[[FetchResult], None]] = None,
        progress_interval: int = 100
    ) -> List[FetchResult]:
        """
        批量采集，支持回调和进度报告
        
        Args:
            tasks: 采集任务列表
            callback: 结果回调函数
            progress_interval: 进度报告间隔
            
        Returns:
            List[FetchResult]: 采集结果列表
        """
        results = []
        processed = 0
        
        batches = [
            tasks[i:i + self.config.batch_size]
            for i in range(0, len(tasks), self.config.batch_size)
        ]
        
        for i, batch in enumerate(batches):
            batch_results = await self.fetch_many(batch)
            results.extend(batch_results)
            
            # 执行回调
            if callback:
                for result in batch_results:
                    callback(result)
            
            processed += len(batch)
            
            # 进度报告
            if processed % progress_interval < self.config.batch_size:
                success_count = sum(1 for r in results if r.success)
                logger.info(
                    f"Progress: {processed}/{len(tasks)} "
                    f"({processed/len(tasks)*100:.1f}%), "
                    f"Success: {success_count}/{len(results)}"
                )
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        
        if stats['total'] > 0:
            stats['success_rate'] = stats['success'] / stats['total']
            stats['avg_duration_ms'] = stats['total_duration_ms'] / stats['total']
        else:
            stats['success_rate'] = 0.0
            stats['avg_duration_ms'] = 0.0
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'retried': 0,
            'total_duration_ms': 0,
        }


class StockDataFetcher(ParallelDataFetcher):
    """
    股票数据专用采集器
    
    针对股票数据采集场景优化，支持：
    - 股票代码批量采集
    - 自动日期范围计算
    - 数据合并与保存
    """
    
    def __init__(
        self,
        config: Optional[ParallelFetcherConfig] = None,
        kline_dir: Optional[Path] = None
    ):
        super().__init__(config)
        self.kline_dir = kline_dir
    
    async def fetch_stock_klines(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        api_url: str,
        api_token: str
    ) -> Dict[str, FetchResult]:
        """
        批量采集股票K线数据
        
        Args:
            codes: 股票代码列表
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            api_url: API 地址
            api_token: API Token
            
        Returns:
            Dict[str, FetchResult]: 采集结果字典
        """
        # 创建采集任务
        tasks = []
        for code in codes:
            params = {
                'code': code,
                'start_date': start_date.replace('-', ''),
                'end_date': end_date.replace('-', ''),
                'token': api_token
            }
            tasks.append(FetchTask(
                identifier=code,
                url=api_url,
                params=params
            ))
        
        # 执行采集
        results = await self.fetch_many(tasks)
        
        return {r.identifier: r for r in results}
    
    async def fetch_with_incremental(
        self,
        codes: List[str],
        days: int = 365 * 3,
        api_url: str = None,
        api_token: str = None
    ) -> Dict[str, FetchResult]:
        """
        增量采集股票数据
        
        自动检测本地数据，只采集缺失的部分。
        
        Args:
            codes: 股票代码列表
            days: 历史天数
            api_url: API 地址
            api_token: API Token
            
        Returns:
            Dict[str, FetchResult]: 采集结果字典
        """
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        tasks = []
        
        for code in codes:
            # 计算需要的日期范围
            start_date = end_date - timedelta(days=days)
            
            # 检查本地数据
            if self.kline_dir:
                kline_file = self.kline_dir / f"{code}.parquet"
                if kline_file.exists():
                    try:
                        import polars as pl
                        df = pl.read_parquet(kline_file)
                        date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
                        last_date = df[date_col].max()
                        last = datetime.strptime(str(last_date), "%Y-%m-%d")
                        
                        if last.date() >= end_date.date():
                            # 数据已是最新，跳过
                            continue
                        
                        # 从最后一天的下一天开始
                        start_date = last + timedelta(days=1)
                    except Exception as e:
                        logger.warning(f"Failed to check existing data for {code}: {e}")
            
            # 创建采集任务
            if start_date.date() <= end_date.date():
                params = {
                    'code': code,
                    'start_date': start_date.strftime('%Y%m%d'),
                    'end_date': end_date.strftime('%Y%m%d'),
                    'token': api_token
                }
                tasks.append(FetchTask(
                    identifier=code,
                    url=api_url,
                    params=params
                ))
        
        if not tasks:
            logger.info("All stocks are up to date, no fetch needed")
            return {}
        
        logger.info(f"Fetching {len(tasks)} stocks incrementally")
        
        # 执行采集
        results = await self.fetch_many(tasks)
        
        return {r.identifier: r for r in results}


# 便捷函数
async def fetch_parallel(
    tasks: List[Tuple[str, str, Optional[Dict]]],
    max_concurrent: int = 50,
    calls_per_minute: int = 480
) -> Dict[str, FetchResult]:
    """
    便捷的并行采集函数
    
    使用示例：
        tasks = [
            ("000001", "https://api.example.com/kline", {"code": "000001.SZ"}),
            ("600000", "https://api.example.com/kline", {"code": "600000.SH"}),
        ]
        results = await fetch_parallel(tasks, max_concurrent=50)
    """
    config = ParallelFetcherConfig(
        max_concurrent=max_concurrent,
        calls_per_minute=calls_per_minute
    )
    
    fetch_tasks = [
        FetchTask(identifier=ident, url=url, params=params)
        for ident, url, params in tasks
    ]
    
    async with ParallelDataFetcher(config) as fetcher:
        results = await fetcher.fetch_many(fetch_tasks)
        return {r.identifier: r for r in results}
