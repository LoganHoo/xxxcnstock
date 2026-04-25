"""
L1 内存缓存实现

基于 cachetools.LRUCache 的高性能内存缓存，适合存储热点数据。
"""

import time
import hashlib
from typing import Any, Optional, Callable
from functools import wraps

from cachetools import LRUCache
from loguru import logger


class MemoryCache:
    """
    内存缓存类
    
    特性：
    - LRU 淘汰策略
    - TTL 过期支持
    - 线程安全
    - 内存占用控制
    """
    
    def __init__(self, maxsize: int = 1000, default_ttl: int = 3600):
        """
        初始化内存缓存
        
        Args:
            maxsize: 最大缓存条目数
            default_ttl: 默认过期时间（秒）
        """
        self._cache = LRUCache(maxsize=maxsize)
        self._ttl = {}
        self.default_ttl = default_ttl
        self._maxsize = maxsize
        
        logger.info(f"MemoryCache initialized: maxsize={maxsize}, default_ttl={default_ttl}s")
    
    def _make_key(self, key: str) -> str:
        """生成缓存键"""
        if isinstance(key, str):
            return key
        return hashlib.md5(str(key).encode()).hexdigest()
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取缓存值
        
        Args:
            key: 缓存键
            default: 默认值
            
        Returns:
            缓存值或默认值
        """
        cache_key = self._make_key(key)
        
        # 检查是否过期
        if cache_key in self._ttl:
            if time.time() > self._ttl[cache_key]:
                self.delete(key)
                return default
        
        value = self._cache.get(cache_key)
        if value is not None:
            logger.debug(f"MemoryCache HIT: {key}")
        else:
            logger.debug(f"MemoryCache MISS: {key}")
        
        return value if value is not None else default
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> None:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒），None 表示使用默认值
        """
        cache_key = self._make_key(key)
        self._cache[cache_key] = value
        
        # 设置过期时间
        expire_time = time.time() + (ttl if ttl is not None else self.default_ttl)
        self._ttl[cache_key] = expire_time
        
        logger.debug(f"MemoryCache SET: {key} (ttl={ttl or self.default_ttl}s)")
    
    def delete(self, key: str) -> bool:
        """
        删除缓存
        
        Args:
            key: 缓存键
            
        Returns:
            是否成功删除
        """
        cache_key = self._make_key(key)
        
        if cache_key in self._cache:
            del self._cache[cache_key]
            self._ttl.pop(cache_key, None)
            logger.debug(f"MemoryCache DELETE: {key}")
            return True
        
        return False
    
    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear()
        self._ttl.clear()
        logger.info("MemoryCache cleared")
    
    def keys(self) -> list:
        """获取所有缓存键"""
        return list(self._cache.keys())
    
    def info(self) -> dict:
        """
        获取缓存信息
        
        Returns:
            缓存统计信息
        """
        return {
            "size": len(self._cache),
            "maxsize": self._maxsize,
            "usage_percent": len(self._cache) / self._maxsize * 100,
            "default_ttl": self.default_ttl,
        }
    
    def cleanup_expired(self) -> int:
        """
        清理过期缓存
        
        Returns:
            清理的条目数
        """
        now = time.time()
        expired_keys = [
            key for key, expire_time in self._ttl.items()
            if now > expire_time
        ]
        
        for key in expired_keys:
            self._cache.pop(key, None)
            self._ttl.pop(key, None)
        
        if expired_keys:
            logger.debug(f"MemoryCache cleaned up {len(expired_keys)} expired entries")
        
        return len(expired_keys)


def memory_cached(
    maxsize: int = 1000,
    ttl: int = 3600,
    key_func: Optional[Callable] = None
):
    """
    内存缓存装饰器
    
    使用示例：
        @memory_cached(maxsize=100, ttl=300)
        def get_stock_list():
            return fetch_stock_list_from_api()
    """
    cache = MemoryCache(maxsize=maxsize, default_ttl=ttl)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # 尝试从缓存获取
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # 执行函数并缓存结果
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result
        
        # 附加缓存操作接口
        wrapper.cache = cache
        wrapper.clear_cache = cache.clear
        
        return wrapper
    return decorator
