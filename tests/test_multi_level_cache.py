"""
多级缓存系统测试
"""

import pytest
import time
from unittest.mock import Mock, patch

from core.cache.memory_cache import MemoryCache
from core.cache.multi_level_cache import MultiLevelCache


class TestMemoryCache:
    """测试内存缓存"""
    
    def test_basic_operations(self):
        """测试基本操作"""
        cache = MemoryCache(maxsize=100, default_ttl=3600)
        
        # 测试设置和获取
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # 测试获取不存在的键
        assert cache.get("nonexistent") is None
        assert cache.get("nonexistent", "default") == "default"
    
    def test_ttl_expiration(self):
        """测试 TTL 过期"""
        cache = MemoryCache(maxsize=100, default_ttl=1)
        
        cache.set("key1", "value1", ttl=0.1)
        assert cache.get("key1") == "value1"
        
        # 等待过期
        time.sleep(0.2)
        assert cache.get("key1") is None
    
    def test_delete(self):
        """测试删除"""
        cache = MemoryCache(maxsize=100)
        
        cache.set("key1", "value1")
        assert cache.delete("key1") is True
        assert cache.get("key1") is None
        assert cache.delete("key1") is False
    
    def test_clear(self):
        """测试清空"""
        cache = MemoryCache(maxsize=100)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        cache.clear()
        
        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert len(cache.keys()) == 0
    
    def test_lru_eviction(self):
        """测试 LRU 淘汰"""
        cache = MemoryCache(maxsize=2)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")  # 应该淘汰 key1
        
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"
    
    def test_info(self):
        """测试信息获取"""
        cache = MemoryCache(maxsize=100, default_ttl=3600)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        info = cache.info()
        assert info["size"] == 2
        assert info["maxsize"] == 100
        assert info["usage_percent"] == 2.0


class TestMultiLevelCache:
    """测试多级缓存"""
    
    def test_l1_cache_hit(self):
        """测试 L1 缓存命中"""
        # 使用 Mock 避免依赖 Redis
        with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
            mock_redis_instance = Mock()
            mock_redis_instance.get.return_value = None
            mock_redis.return_value = mock_redis_instance
            
            cache = MultiLevelCache()
            cache.l1_cache.set("key1", "value1")
            
            # 应该从 L1 获取
            value = cache.get("key1")
            assert value == "value1"
            
            # L2 不应该被访问
            mock_redis_instance.get.assert_not_called()
    
    def test_l2_cache_hit(self):
        """测试 L2 缓存命中"""
        with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
            mock_redis_instance = Mock()
            mock_redis_instance.get.return_value = "value_from_l2"
            mock_redis.return_value = mock_redis_instance
            
            cache = MultiLevelCache()
            
            # L1 未命中，从 L2 获取
            value = cache.get("key1")
            assert value == "value_from_l2"
            
            # 应该回填到 L1
            assert cache.l1_cache.get("key1") == "value_from_l2"
    
    def test_cache_miss(self):
        """测试缓存未命中"""
        with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
            mock_redis_instance = Mock()
            mock_redis_instance.get.return_value = None
            mock_redis.return_value = mock_redis_instance
            
            cache = MultiLevelCache()
            
            value = cache.get("nonexistent")
            assert value is None
    
    def test_set_both_levels(self):
        """测试同时设置两级缓存"""
        with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis.return_value = mock_redis_instance
            
            cache = MultiLevelCache()
            cache.set("key1", "value1", level="both")
            
            # L1 应该被设置
            assert cache.l1_cache.get("key1") == "value1"
            # L2 也应该被设置
            mock_redis_instance.set.assert_called_once()
    
    def test_get_with_loader(self):
        """测试带加载器的获取"""
        with patch('core.cache.multi_level_cache.RedisCache') as mock_redis:
            mock_redis_instance = Mock()
            mock_redis_instance.get.return_value = None
            mock_redis_instance.set.return_value = True
            mock_redis.return_value = mock_redis_instance
            
            cache = MultiLevelCache()
            
            def loader():
                return "loaded_value"
            
            value = cache.get_with_loader("key1", loader)
            assert value == "loaded_value"
            
            # 应该写入缓存
            assert cache.l1_cache.get("key1") == "loaded_value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
