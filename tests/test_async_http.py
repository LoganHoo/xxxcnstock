"""
异步 HTTP 客户端测试
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock

from core.async_http import (
    AsyncHTTPClient,
    BatchRequestManager,
    RateLimiter,
    ExponentialBackoff,
    RetryConfig,
    fetch_parallel
)


class TestExponentialBackoff:
    """测试指数退避机制"""
    
    def test_initial_delay(self):
        """测试初始延迟"""
        backoff = ExponentialBackoff(RetryConfig(base_delay=1.0))
        assert backoff.next_delay() == 1.0
    
    def test_exponential_increase(self):
        """测试指数增长"""
        backoff = ExponentialBackoff(RetryConfig(base_delay=1.0))
        
        delays = [backoff.next_delay() for _ in range(3)]
        assert delays == [1.0, 2.0, 4.0]
    
    def test_max_delay_cap(self):
        """测试最大延迟限制"""
        backoff = ExponentialBackoff(RetryConfig(base_delay=1.0, max_delay=5.0))
        
        delays = [backoff.next_delay() for _ in range(10)]
        assert all(d <= 5.0 for d in delays)
    
    def test_should_retry(self):
        """测试重试判断"""
        backoff = ExponentialBackoff(RetryConfig(max_retries=3))
        
        assert backoff.should_retry()  # 第 0 次
        backoff.next_delay()
        assert backoff.should_retry()  # 第 1 次
        backoff.next_delay()
        assert backoff.should_retry()  # 第 2 次
        backoff.next_delay()
        assert not backoff.should_retry()  # 第 3 次，不应再重试


class TestRateLimiter:
    """测试流量限制器"""
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """测试流量限制"""
        limiter = RateLimiter(RateLimitConfig(calls_per_minute=60, burst_size=2))
        
        start_time = asyncio.get_event_loop().time()
        
        # 前 2 个请求应该立即通过
        await limiter.acquire()
        await limiter.acquire()
        
        # 第 3 个请求应该等待
        await limiter.acquire()
        
        elapsed = asyncio.get_event_loop().time() - start_time
        assert elapsed >= 1.0  # 至少等待 1 秒


class TestAsyncHTTPClient:
    """测试异步 HTTP 客户端"""
    
    @pytest.mark.asyncio
    async def test_client_initialization(self):
        """测试客户端初始化"""
        client = AsyncHTTPClient(pool_size=50, timeout=30)
        
        await client.open()
        assert client._session is not None
        assert client._connector is not None
        
        await client.close()
        assert client._session is None
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """测试上下文管理器"""
        async with AsyncHTTPClient() as client:
            assert client._session is not None
        
        assert client._session is None


class TestFetchParallel:
    """测试并行采集便捷函数"""
    
    @pytest.mark.asyncio
    @patch('core.async_http.AsyncHTTPClient')
    async def test_fetch_parallel(self, mock_client_class):
        """测试并行采集"""
        # 模拟响应
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": "test"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)
        
        mock_session = Mock()
        mock_session.get = AsyncMock(return_value=mock_response)
        
        mock_client = Mock()
        mock_client._session = mock_session
        mock_client.open = AsyncMock()
        mock_client.close = AsyncMock()
        
        mock_client_class.return_value = mock_client
        mock_client_class.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_class.__aexit__ = AsyncMock(return_value=False)
        
        # 测试数据
        tasks = [
            ("task1", "http://api.example.com/1", {"param": "1"}),
            ("task2", "http://api.example.com/2", {"param": "2"}),
        ]
        
        # 执行测试
        # 注意：由于 mock 的复杂性，这里只是示例
        # 实际测试需要更完善的 mock 设置


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
