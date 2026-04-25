"""
多级缓存协调器

整合 L1 内存缓存和 L2 Redis 缓存，提供统一的缓存接口。
"""

import time
from typing import Any, Optional, Callable
from functools import wraps

from loguru import logger

from .memory_cache import MemoryCache
from .redis_cache import RedisCache


class MultiLevelCache:
    """
    多级缓存类
    
    缓存层级：
    - L1: 内存缓存（最快，进程内）
    - L2: Redis 缓存（分布式，跨进程）
    
    读取顺序：L1 → L2 → 数据源
    写入顺序：数据源 → L2 → L1
    """
    
    def __init__(
        self,
        l1_maxsize: int = 1000,
        l1_ttl: int = 3600,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        redis_db: int = 0,
        l2_ttl: int = 86400,
        key_prefix: str = "xcnstock:"
    ):
        """
        初始化多级缓存
        
        Args:
            l1_maxsize: L1 缓存最大条目数
            l1_ttl: L1 缓存默认过期时间（秒）
            redis_host: Redis 主机地址
            redis_port: Redis 端口
            redis_password: Redis 密码
            redis_db: Redis 数据库编号
            l2_ttl: L2 缓存默认过期时间（秒）
            key_prefix: 键前缀
        """
        self.l1_cache = MemoryCache(maxsize=l1_maxsize, default_ttl=l1_ttl)
        self.l2_cache = RedisCache(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            default_ttl=l2_ttl,
            key_prefix=key_prefix,
            fallback_to_memory=False  # L2 不降级，由本类统一管理
        )
        
        self.key_prefix = key_prefix
        self.hit_stats = {"l1": 0, "l2": 0, "miss": 0}
        
        logger.info(f"MultiLevelCache initialized: L1(maxsize={l1_maxsize}), L2(redis={redis_host}:{redis_port})")
    
    def get(
        self,
        key: str,
        default: Any = None,
        l1_ttl: Optional[int] = None
    ) -> Any:
        """
        获取缓存值（多级缓存读取）
        
        Args:
            key: 缓存键
            default: 默认值
            l1_ttl: L1 缓存过期时间（从 L2 回填时使用）
            
        Returns:
            缓存值或默认值
        """
        # 1. 尝试从 L1 获取
        value = self.l1_cache.get(key)
        if value is not None:
            self.hit_stats["l1"] += 1
            logger.debug(f"MultiLevelCache L1 HIT: {key}")
            return value
        
        # 2. 尝试从 L2 获取
        value = self.l2_cache.get(key)
        if value is not None:
            self.hit_stats["l2"] += 1
            logger.debug(f"MultiLevelCache L2 HIT: {key}")
            
            # 回填到 L1
            self.l1_cache.set(key, value, l1_ttl)
            return value
        
        # 3. 缓存未命中
        self.hit_stats["miss"] += 1
        logger.debug(f"MultiLevelCache MISS: {key}")
        return default
    
    def set(
        self,
        key: str,
        value: Any,
        l1_ttl: Optional[int] = None,
        l2_ttl: Optional[int] = None,
        level: str = "both"
    ) -> bool:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            l1_ttl: L1 缓存过期时间（秒）
            l2_ttl: L2 缓存过期时间（秒）
            level: 缓存层级 ('l1', 'l2', 'both')
            
        Returns:
            是否成功
        """
        success = True
        
        if level in ("l1", "both"):
            self.l1_cache.set(key, value, l1_ttl)
            logger.debug(f"MultiLevelCache L1 SET: {key}")
        
        if level in ("l2", "both"):
            l2_success = self.l2_cache.set(key, value, l2_ttl)
            if not l2_success:
                success = False
            logger.debug(f"MultiLevelCache L2 SET: {key}")
        
        return success
    
    def delete(self, key: str, level: str = "both") -> bool:
        """
        删除缓存
        
        Args:
            key: 缓存键
            level: 缓存层级 ('l1', 'l2', 'both')
            
        Returns:
            是否成功删除
        """
        deleted = False
        
        if level in ("l1", "both"):
            if self.l1_cache.delete(key):
                deleted = True
        
        if level in ("l2", "both"):
            if self.l2_cache.delete(key):
                deleted = True
        
        if deleted:
            logger.debug(f"MultiLevelCache DELETE: {key}")
        
        return deleted
    
    def clear(self, level: str = "both", pattern: str = "*") -> int:
        """
        清空缓存
        
        Args:
            level: 缓存层级 ('l1', 'l2', 'both')
            pattern: 匹配模式（仅对 L2 有效）
            
        Returns:
            删除的键数
        """
        deleted_count = 0
        
        if level in ("l1", "both"):
            self.l1_cache.clear()
            deleted_count += 1
        
        if level in ("l2", "both"):
            deleted_count += self.l2_cache.clear(pattern)
        
        logger.info(f"MultiLevelCache cleared {deleted_count} keys")
        return deleted_count
    
    def get_with_loader(
        self,
        key: str,
        loader: Callable[[], Any],
        l1_ttl: Optional[int] = None,
        l2_ttl: Optional[int] = None,
        level: str = "both"
    ) -> Any:
        """
        获取缓存值，如果不存在则使用 loader 加载
        
        Args:
            key: 缓存键
            loader: 数据加载函数
            l1_ttl: L1 缓存过期时间
            l2_ttl: L2 缓存过期时间
            level: 缓存层级
            
        Returns:
            缓存值或加载的数据
        """
        # 尝试从缓存获取
        value = self.get(key, l1_ttl=l1_ttl)
        if value is not None:
            return value
        
        # 使用 loader 加载数据
        logger.debug(f"MultiLevelCache loading data for: {key}")
        value = loader()
        
        # 写入缓存
        if value is not None:
            self.set(key, value, l1_ttl, l2_ttl, level)
        
        return value
    
    def info(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            缓存统计信息
        """
        total_hits = sum(self.hit_stats.values())
        hit_rate = (total_hits - self.hit_stats["miss"]) / total_hits * 100 if total_hits > 0 else 0

        return {
            "l1_cache": self.l1_cache.info(),
            "l2_cache": self.l2_cache.info(),
            "hit_stats": self.hit_stats.copy(),
            "hit_rate_percent": round(hit_rate, 2),
            "total_requests": total_hits,
        }

    def get_stats(self) -> dict:
        """
        获取缓存统计信息（兼容 API 接口）

        Returns:
            缓存统计信息
        """
        total_hits = sum(self.hit_stats.values())
        total_misses = self.hit_stats["miss"]

        return {
            "l1_hits": self.hit_stats["l1"],
            "l1_misses": total_misses,
            "l2_hits": self.hit_stats["l2"],
            "l2_misses": total_misses,
            "misses": total_misses,
            "l1_keys": len(self.l1_cache._cache) if hasattr(self.l1_cache, '_cache') else 0,
            "l2_keys": 0  # Redis keys count would require SCAN, skip for now
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.hit_stats = {"l1": 0, "l2": 0, "miss": 0}
        logger.info("MultiLevelCache stats reset")


# 全局缓存实例（单例模式）
_global_cache: Optional[MultiLevelCache] = None


def get_cache() -> MultiLevelCache:
    """获取全局缓存实例"""
    global _global_cache
    if _global_cache is None:
        _global_cache = MultiLevelCache()
    return _global_cache


def cached(
    ttl: int = 3600,
    level: str = "both",
    key_func: Optional[Callable] = None,
    cache_instance: Optional[MultiLevelCache] = None
):
    """
    多级缓存装饰器
    
    使用示例：
        @cached(ttl=300, level="l1")
        def get_stock_list():
            return fetch_stock_list_from_api()
        
        @cached(ttl=600, level="both", key_func=lambda code: f"stock:{code}")
        def get_stock_info(code: str):
            return fetch_stock_info(code)
    """
    cache = cache_instance or get_cache()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # 使用 loader 模式获取数据
            return cache.get_with_loader(
                key=cache_key,
                loader=lambda: func(*args, **kwargs),
                l1_ttl=ttl if level in ("l1", "both") else None,
                l2_ttl=ttl if level in ("l2", "both") else None,
                level=level
            )
        
        # 附加缓存操作接口
        wrapper.cache = cache
        wrapper.clear_cache = lambda: cache.delete(
            key_func() if key_func else f"{func.__name__}:():{{}}"
        )
        
        return wrapper
    return decorator
