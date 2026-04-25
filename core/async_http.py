"""
异步 HTTP 客户端模块

提供高性能的异步 HTTP 请求能力，支持连接池、流量控制和重试机制。
用于并行数据采集，显著提升数据获取效率。
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from functools import wraps

import aiohttp
from loguru import logger


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0


@dataclass
class RateLimitConfig:
    """流量控制配置"""
    calls_per_minute: int = 480  # Tushare 限制 500，预留 20 缓冲
    burst_size: int = 50


class ExponentialBackoff:
    """指数退避重试机制"""
    
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self.attempt = 0
    
    def next_delay(self) -> float:
        """计算下一次重试的延迟时间"""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** self.attempt),
            self.config.max_delay
        )
        self.attempt += 1
        return delay
    
    def reset(self):
        """重置重试计数"""
        self.attempt = 0
    
    def should_retry(self) -> bool:
        """检查是否应该继续重试"""
        return self.attempt < self.config.max_retries


class RateLimiter:
    """异步流量限制器"""
    
    def __init__(self, config: RateLimitConfig = None):
        self.config = config or RateLimitConfig()
        self.tokens = config.burst_size
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """获取一个令牌，如果没有可用令牌则等待"""
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # 补充令牌
            self.tokens = min(
                self.config.burst_size,
                self.tokens + elapsed * (self.config.calls_per_minute / 60.0)
            )
            self.last_update = now
            
            if self.tokens < 1:
                # 计算需要等待的时间
                wait_time = (1 - self.tokens) / (self.config.calls_per_minute / 60.0)
                logger.debug(f"Rate limit hit, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


def rate_limit(calls_per_minute: int = 480, burst_size: int = 50):
    """流量控制装饰器"""
    limiter = RateLimiter(RateLimitConfig(
        calls_per_minute=calls_per_minute,
        burst_size=burst_size
    ))
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await limiter.acquire()
            return await func(*args, **kwargs)
        return wrapper
    return decorator


class AsyncHTTPClient:
    """
    异步 HTTP 客户端
    
    特性：
    - 连接池复用
    - 自动重试机制
    - 流量控制
    - 超时处理
    """
    
    def __init__(
        self,
        pool_size: int = 100,
        timeout: int = 30,
        retry_config: RetryConfig = None
    ):
        self.pool_size = pool_size
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retry_config = retry_config or RetryConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
    
    async def __aenter__(self):
        await self.open()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def open(self):
        """初始化连接池"""
        if self._session is None:
            self._connector = aiohttp.TCPConnector(
                limit=self.pool_size,
                limit_per_host=10,
                enable_cleanup_closed=True,
                force_close=False,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=self.timeout,
                headers={
                    "User-Agent": "XCNStock-DataFetcher/1.0",
                    "Accept": "application/json",
                }
            )
            logger.info(f"AsyncHTTPClient initialized with pool_size={self.pool_size}")
    
    async def close(self):
        """关闭连接池"""
        if self._session:
            await self._session.close()
            self._session = None
            logger.info("AsyncHTTPClient closed")
    
    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        发送 GET 请求（带重试）
        
        Args:
            url: 请求 URL
            params: URL 参数
            headers: 请求头
            
        Returns:
            JSON 响应数据
            
        Raises:
            aiohttp.ClientError: 请求失败且重试耗尽
        """
        backoff = ExponentialBackoff(self.retry_config)
        last_exception = None
        
        while backoff.should_retry():
            try:
                async with self._session.get(
                    url,
                    params=params,
                    headers=headers
                ) as response:
                    if response.status == 429:  # Rate limited
                        delay = backoff.next_delay()
                        logger.warning(f"Rate limited, retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                    
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                last_exception = e
                if not backoff.should_retry():
                    break
                    
                delay = backoff.next_delay()
                logger.warning(f"Request failed: {e}, retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
        
        logger.error(f"Request failed after {self.retry_config.max_retries} retries: {last_exception}")
        raise last_exception
    
    async def post(
        self,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        发送 POST 请求（带重试）
        
        Args:
            url: 请求 URL
            data: 表单数据
            json_data: JSON 数据
            headers: 请求头
            
        Returns:
            JSON 响应数据
        """
        backoff = ExponentialBackoff(self.retry_config)
        last_exception = None
        
        while backoff.should_retry():
            try:
                async with self._session.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=headers
                ) as response:
                    if response.status == 429:
                        delay = backoff.next_delay()
                        logger.warning(f"Rate limited, retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                    
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                last_exception = e
                if not backoff.should_retry():
                    break
                    
                delay = backoff.next_delay()
                logger.warning(f"Request failed: {e}, retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
        
        logger.error(f"Request failed after {self.retry_config.max_retries} retries: {last_exception}")
        raise last_exception


class BatchRequestManager:
    """
    批量请求管理器
    
    管理并发请求，控制并发数，收集结果
    """
    
    def __init__(
        self,
        client: AsyncHTTPClient,
        max_concurrency: int = 50,
        batch_size: int = 100
    ):
        self.client = client
        self.max_concurrency = max_concurrency
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(max_concurrency)
    
    async def fetch_one(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        identifier: str = None
    ) -> tuple:
        """
        获取单个请求（受信号量控制）
        
        Returns:
            (identifier, data) 或 (identifier, exception)
        """
        async with self.semaphore:
            try:
                data = await self.client.get(url, params=params)
                return (identifier or url, data)
            except Exception as e:
                logger.error(f"Failed to fetch {identifier}: {e}")
                return (identifier or url, e)
    
    async def fetch_many(
        self,
        requests: List[tuple]
    ) -> Dict[str, Any]:
        """
        批量获取多个请求
        
        Args:
            requests: 列表，每个元素为 (url, params, identifier)
            
        Returns:
            字典，key 为 identifier，value 为数据或异常
        """
        tasks = [
            self.fetch_one(url, params, identifier)
            for url, params, identifier in requests
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return dict(results)
    
    async def fetch_batches(
        self,
        requests: List[tuple]
    ) -> Dict[str, Any]:
        """
        分批获取请求，避免一次性创建过多任务
        
        Args:
            requests: 列表，每个元素为 (url, params, identifier)
            
        Returns:
            合并后的结果字典
        """
        all_results = {}
        
        for i in range(0, len(requests), self.batch_size):
            batch = requests[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1}/{(len(requests) - 1) // self.batch_size + 1}")
            
            batch_results = await self.fetch_many(batch)
            all_results.update(batch_results)
            
            # 短暂暂停，避免过载
            if i + self.batch_size < len(requests):
                await asyncio.sleep(0.1)
        
        return all_results


# 便捷函数
async def fetch_single(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    便捷的单个请求函数
    
    使用示例：
        data = await fetch_single("https://api.example.com/data", {"code": "000001.SZ"})
    """
    async with AsyncHTTPClient(timeout=timeout) as client:
        return await client.get(url, params=params)


async def fetch_multiple(
    urls: List[tuple],
    max_concurrency: int = 50,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    便捷的批量请求函数
    
    使用示例：
        requests = [
            ("https://api.example.com/data", {"code": "000001.SZ"}, "000001"),
            ("https://api.example.com/data", {"code": "600000.SH"}, "600000"),
        ]
        results = await fetch_multiple(requests)
    """
    async with AsyncHTTPClient(timeout=timeout) as client:
        manager = BatchRequestManager(client, max_concurrency=max_concurrency)
        return await manager.fetch_batches(urls)
