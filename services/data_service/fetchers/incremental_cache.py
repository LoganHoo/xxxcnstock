#!/usr/bin/env python3
"""
增量日期缓存管理器 (v3.0)

职责：
1. 管理每只股票的K线最新日期缓存
2. 支持Redis作为缓存后端
3. 批量读写优化
4. 缓存失效管理

设计原则：
- 单一职责：只负责增量日期缓存
- 性能优先：使用pipeline批量操作
- 容错性：Redis不可用时优雅降级
"""

from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class IncrementalDateCache:
    """
    增量日期缓存管理器
    
    将每只股票的K线最新日期缓存到Redis，避免每次读取Parquet文件
    
    使用方式：
        cache = IncrementalDateCache(redis_client=redis_conn)
        dates = cache.get_last_dates(['000001', '000002'])
        cache.set_last_date('000001', '2026-01-15')
    """
    
    def __init__(self, redis_client=None, config=None):
        """
        初始化缓存管理器
        
        Args:
            redis_client: Redis连接实例（可选，为None则自动创建）
            config: DualSourceConfig实例（可选）
        """
        self._config = config
        self._redis = redis_client or self._init_redis()
        
        if self._config is None:
            from services.data_service.fetchers.dual_source_config import get_default_config
            self._config = get_default_config()
    
    def _init_redis(self):
        """初始化Redis连接"""
        try:
            import redis
            from core.config import get_settings
            settings = get_settings()
            
            client = redis.Redis(
                host=getattr(settings, 'REDIS_HOST', 'localhost'),
                port=getattr(settings, 'REDIS_PORT', 6379),
                password=getattr(settings, 'REDIS_PASSWORD', None),
                db=getattr(settings, 'REDIS_DB', 0),
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            
            client.ping()
            logger.debug("Redis连接成功")
            return client
            
        except Exception as e:
            logger.warning(f"Redis连接失败: {e}，将禁用缓存")
            return None
    
    @property
    def _cache_key_prefix(self) -> str:
        """获取缓存键前缀"""
        return getattr(self._config, 'cache_key_prefix', 'xcnstock:kline:last_date')
    
    @property
    def _cache_ttl(self) -> int:
        """获取缓存TTL（秒）"""
        return getattr(self._config, 'cache_ttl', 86400)
    
    @property
    def _cache_enabled(self) -> bool:
        """检查缓存是否启用"""
        return getattr(self._config, 'cache_enabled', True)
    
    def get_last_dates(self, codes: List[str]) -> Dict[str, str]:
        """
        批量获取多只股票的最新日期
        
        Args:
            codes: 股票代码列表
            
        Returns:
            字典 {code: last_date}，仅包含有缓存的代码
        """
        if not self._redis or not self._cache_enabled:
            return {}
        
        if not codes:
            return {}
        
        result = {}
        
        try:
            pipe = self._redis.pipeline()
            for code in codes:
                key = f"{self._cache_key_prefix}:{code}"
                pipe.get(key)
            
            results = pipe.execute()
            
            for code, value in zip(codes, results):
                if value:
                    result[code] = value
            
            if result:
                logger.debug(f"从缓存获取 {len(result)}/{len(codes)} 只股票的最新日期")
            
            return result
            
        except Exception as e:
            logger.error(f"批量获取缓存失败: {e}")
            return {}
    
    def set_last_date(self, code: str, date: str) -> bool:
        """
        设置单只股票的最新日期
        
        Args:
            code: 股票代码
            date: 最新交易日期 (YYYY-MM-DD格式)
            
        Returns:
            是否设置成功
        """
        if not self._redis or not self._cache_enabled:
            return False
        
        try:
            key = f"{self._cache_key_prefix}:{code}"
            self._redis.setex(key, self._cache_ttl, date)
            return True
            
        except Exception as e:
            logger.error(f"设置缓存失败 {code}: {e}")
            return False
    
    def set_last_dates_batch(self, date_map: Dict[str, str]) -> int:
        """
        批量设置多只股票的最新日期
        
        Args:
            date_map: 字典 {code: date}
            
        Returns:
            成功设置的数量
        """
        if not self._redis or not self._cache_enabled or not date_map:
            return 0
        
        success_count = 0
        
        try:
            pipe = self._redis.pipeline()
            
            for code, date in date_map.items():
                key = f"{self._cache_key_prefix}:{code}"
                pipe.setex(key, self._cache_ttl, date)
            
            results = pipe.execute()
            success_count = sum(1 for r in results if r)
            
            logger.debug(f"批量设置缓存: {success_count}/{len(date_map)} 只成功")
            return success_count
            
        except Exception as e:
            logger.error(f"批量设置缓存失败: {e}")
            return 0
    
    def invalidate(self, code: str = None):
        """
        使缓存失效
        
        Args:
            code: 股票代码（为None则清除所有缓存）
        """
        if not self._redis:
            return
        
        try:
            if code:
                key = f"{self._cache_key_prefix}:{code}"
                self._redis.delete(key)
                logger.debug(f"已使缓存失效: {code}")
            else:
                pattern = f"{self._cache_key_prefix}:*"
                keys = self._redis.keys(pattern)
                if keys:
                    self._redis.delete(*keys)
                    logger.debug(f"已清除所有缓存 ({len(keys)} 个)")
                    
        except Exception as e:
            logger.error(f"使缓存失效失败: {e}")
    
    def get_stats(self) -> Dict:
        """获取缓存统计信息"""
        stats = {
            'enabled': self._cache_enabled,
            'redis_connected': self._redis is not None,
            'key_prefix': self._cache_key_prefix,
            'ttl_hours': round(self._cache_ttl / 3600, 1),
        }
        
        if self._redis:
            try:
                pattern = f"{self._cache_key_prefix}:*"
                keys = self._redis.keys(pattern)
                stats['cached_stocks'] = len(keys)
            except Exception as e:
                stats['error'] = str(e)
        
        return stats
    
    def close(self):
        """关闭Redis连接"""
        if self._redis and hasattr(self._redis, 'close'):
            try:
                self._redis.close()
                logger.debug("IncrementalDateCache Redis连接已关闭")
            except Exception as e:
                logger.warning(f"关闭Redis连接失败: {e}")
            finally:
                self._redis = None
