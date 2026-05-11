"""Redis 分布式锁管理器"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RedisLockManager:
    """Redis 分布式锁管理器"""

    def __init__(self, redis_client):
        """
        初始化锁管理器

        Args:
            redis_client: Redis 客户端实例
        """
        self.redis = redis_client

    def acquire(self, lock_key: str, timeout: int = 7200, blocking: bool = True, blocking_timeout: int = 10) -> bool:
        """
        获取分布式锁

        Args:
            lock_key: 锁键名
            timeout: 锁超时时间(秒)
            blocking: 是否阻塞等待
            blocking_timeout: 阻塞超时时间(秒)

        Returns:
            是否成功获取锁
        """
        lock = self.redis.lock(
            f"lock:{lock_key}",
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout
        )
        return lock.acquire()

    def release(self, lock_key: str) -> bool:
        """
        释放分布式锁

        Args:
            lock_key: 锁键名

        Returns:
            是否成功释放
        """
        try:
            lock = self.redis.lock(f"lock:{lock_key}")
            lock.release()
            return True
        except Exception as e:
            logger.warning(f"释放锁失败 {lock_key}: {e}")
            return False

    def is_locked(self, lock_key: str) -> bool:
        """
        检查锁是否被持有

        Args:
            lock_key: 锁键名

        Returns:
            锁是否已被持有
        """
        return self.redis.exists(f"lock:{lock_key}") > 0

    def get_lock_ttl(self, lock_key: str) -> int:
        """
        获取锁的剩余 TTL

        Args:
            lock_key: 锁键名

        Returns:
            剩余秒数，-1 表示无 TTL，-2 表示不存在
        """
        return self.redis.ttl(f"lock:{lock_key}")
