"""
L2 Redis 缓存实现

基于 Redis 的分布式缓存，支持跨进程数据共享和持久化。
"""

import json
import pickle
import hashlib
from typing import Any, Optional, Callable, Union
from functools import wraps

import redis
from loguru import logger


class RedisCache:
    """
    Redis 缓存类
    
    特性：
    - 分布式缓存
    - 支持多种序列化方式（JSON、Pickle）
    - 故障降级到内存缓存
    - 连接池管理
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        default_ttl: int = 3600,
        key_prefix: str = "xcnstock:",
        fallback_to_memory: bool = True
    ):
        """
        初始化 Redis 缓存
        
        Args:
            host: Redis 主机地址
            port: Redis 端口
            password: Redis 密码
            db: Redis 数据库编号
            default_ttl: 默认过期时间（秒）
            key_prefix: 键前缀
            fallback_to_memory: 是否降级到内存缓存
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self.fallback_to_memory = fallback_to_memory
        
        self._client: Optional[redis.Redis] = None
        self._fallback_cache = None
        
        if fallback_to_memory:
            from .memory_cache import MemoryCache
            self._fallback_cache = MemoryCache(maxsize=1000, default_ttl=default_ttl)
        
        self._connect()
    
    def _connect(self) -> bool:
        """建立 Redis 连接"""
        try:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=False,  # 使用二进制模式，支持多种序列化
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30,
            )
            # 测试连接
            self._client.ping()
            logger.info(f"RedisCache connected: {self.host}:{self.port}/{self.db}")
            return True
        except redis.ConnectionError as e:
            logger.error(f"Redis connection failed: {e}")
            self._client = None
            return False
    
    def _make_key(self, key: str) -> str:
        """生成带前缀的缓存键"""
        if isinstance(key, str):
            return f"{self.key_prefix}{key}"
        return f"{self.key_prefix}{hashlib.md5(str(key).encode()).hexdigest()}"
    
    def _serialize(self, value: Any) -> bytes:
        """序列化值"""
        try:
            # 尝试使用 JSON（可读性好）
            return json.dumps(value).encode('utf-8')
        except (TypeError, ValueError):
            # 复杂对象使用 Pickle
            return pickle.dumps(value)
    
    def _deserialize(self, data: bytes) -> Any:
        """反序列化值"""
        try:
            # 尝试 JSON
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 尝试 Pickle
            return pickle.loads(data)
    
    def is_connected(self) -> bool:
        """检查 Redis 连接状态"""
        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except redis.ConnectionError:
            return False
    
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
        
        # 尝试从 Redis 获取
        if self.is_connected():
            try:
                data = self._client.get(cache_key)
                if data is not None:
                    logger.debug(f"RedisCache HIT: {key}")
                    return self._deserialize(data)
            except redis.RedisError as e:
                logger.warning(f"Redis get error: {e}, falling back to memory")
        
        # 降级到内存缓存
        if self._fallback_cache:
            return self._fallback_cache.get(key, default)
        
        logger.debug(f"RedisCache MISS: {key}")
        return default
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒）
            
        Returns:
            是否成功
        """
        cache_key = self._make_key(key)
        expire = ttl if ttl is not None else self.default_ttl
        
        # 序列化值
        data = self._serialize(value)
        
        # 尝试写入 Redis
        if self.is_connected():
            try:
                self._client.setex(cache_key, expire, data)
                logger.debug(f"RedisCache SET: {key} (ttl={expire}s)")
                return True
            except redis.RedisError as e:
                logger.warning(f"Redis set error: {e}, falling back to memory")
        
        # 降级到内存缓存
        if self._fallback_cache:
            self._fallback_cache.set(key, value, ttl)
            return True
        
        return False
    
    def delete(self, key: str) -> bool:
        """
        删除缓存
        
        Args:
            key: 缓存键
            
        Returns:
            是否成功删除
        """
        cache_key = self._make_key(key)
        deleted = False
        
        # 从 Redis 删除
        if self.is_connected():
            try:
                result = self._client.delete(cache_key)
                deleted = result > 0
            except redis.RedisError as e:
                logger.warning(f"Redis delete error: {e}")
        
        # 从内存缓存删除
        if self._fallback_cache:
            self._fallback_cache.delete(key)
        
        if deleted:
            logger.debug(f"RedisCache DELETE: {key}")
        
        return deleted
    
    def clear(self, pattern: str = "*") -> int:
        """
        清空缓存
        
        Args:
            pattern: 匹配模式
            
        Returns:
            删除的键数
        """
        pattern_key = f"{self.key_prefix}{pattern}"
        deleted_count = 0
        
        # 清空 Redis
        if self.is_connected():
            try:
                keys = self._client.keys(pattern_key)
                if keys:
                    deleted_count = self._client.delete(*keys)
            except redis.RedisError as e:
                logger.warning(f"Redis clear error: {e}")
        
        # 清空内存缓存
        if self._fallback_cache:
            self._fallback_cache.clear()
        
        logger.info(f"RedisCache cleared {deleted_count} keys with pattern: {pattern}")
        return deleted_count
    
    def keys(self, pattern: str = "*") -> list:
        """获取匹配的缓存键"""
        pattern_key = f"{self.key_prefix}{pattern}"
        
        if self.is_connected():
            try:
                keys = self._client.keys(pattern_key)
                # 移除前缀
                prefix_len = len(self.key_prefix)
                return [k.decode('utf-8')[prefix_len:] if isinstance(k, bytes) else k[prefix_len:] for k in keys]
            except redis.RedisError as e:
                logger.warning(f"Redis keys error: {e}")
        
        # 降级到内存缓存
        if self._fallback_cache:
            return self._fallback_cache.keys()
        
        return []
    
    def info(self) -> dict:
        """
        获取缓存信息
        
        Returns:
            缓存统计信息
        """
        info = {
            "connected": self.is_connected(),
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "key_prefix": self.key_prefix,
            "default_ttl": self.default_ttl,
        }
        
        if self.is_connected():
            try:
                redis_info = self._client.info()
                info.update({
                    "used_memory": redis_info.get("used_memory_human", "N/A"),
                    "connected_clients": redis_info.get("connected_clients", 0),
                    "total_keys": self._client.dbsize(),
                })
            except redis.RedisError as e:
                logger.warning(f"Redis info error: {e}")
        
        if self._fallback_cache:
            info["fallback_cache"] = self._fallback_cache.info()
        
        return info
    
    def reconnect(self) -> bool:
        """重新连接 Redis"""
        logger.info("Attempting to reconnect to Redis...")
        return self._connect()


def redis_cached(
    host: str = "localhost",
    port: int = 6379,
    password: Optional[str] = None,
    db: int = 0,
    ttl: int = 3600,
    key_func: Optional[Callable] = None,
    key_prefix: str = "xcnstock:func:"
):
    """
    Redis 缓存装饰器
    
    使用示例：
        @redis_cached(ttl=300, key_prefix="stock:")
        def get_stock_info(code: str):
            return fetch_stock_info(code)
    """
    cache = RedisCache(
        host=host,
        port=port,
        password=password,
        db=db,
        default_ttl=ttl,
        key_prefix=key_prefix
    )
    
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
        wrapper.clear_cache = lambda pattern="*": cache.clear(pattern)
        
        return wrapper
    return decorator
