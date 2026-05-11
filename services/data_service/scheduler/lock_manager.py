"""
Task lock manager with Redis degradation fallback.

Wraps core.distributed_lock.DistributedLock with graceful degradation:
when Redis is unavailable, tasks execute without locking (degraded mode).
Creates a fresh Redis client on each attempt to avoid poisoned connection pools.
"""

import os
from typing import Optional

import redis
from loguru import logger

from core.distributed_lock import DistributedLock


class TaskLockManager:
    """Manages distributed locks for task execution with Redis degradation."""

    def __init__(
        self,
        redis_host: str = "",
        redis_port: int = 6379,
        redis_password: str = "",
        lock_timeout: int = 1800,
    ):
        self._redis_host = redis_host or os.getenv("REDIS_HOST", "localhost")
        self._redis_port = redis_port or int(os.getenv("REDIS_PORT", "6379"))
        self._redis_password = redis_password or os.getenv("REDIS_PASSWORD", "")
        self._lock_timeout = lock_timeout

    def _get_redis_client(self) -> Optional[redis.Redis]:
        """Create a fresh Redis client. Returns None on connection failure."""
        try:
            client = redis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                password=self._redis_password or None,
                socket_connect_timeout=3,
                socket_timeout=5,
                decode_responses=True,
            )
            # Verify connection is alive
            client.ping()
            return client
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis unavailable: {e}")
            return None
        except redis.AuthenticationError as e:
            logger.warning(f"Redis auth failed: {e}")
            return None

    def acquire(self, lock_key: str) -> Optional[DistributedLock | bool]:
        """Acquire a distributed lock.

        Returns:
            DistributedLock: Lock acquired successfully.
            True: Redis unavailable, running in degraded mode (no lock).
            None: Lock held by another instance, skip the task.
        """
        redis_client = self._get_redis_client()
        if redis_client is None:
            logger.warning(
                f"Redis unavailable, running in degraded mode [{lock_key}]"
            )
            return True  # Degraded: allow execution without lock

        try:
            lock = DistributedLock(
                redis_client=redis_client,
                lock_key=lock_key,
                ttl_seconds=self._lock_timeout,
                auto_renew=True,
            )
            acquired = lock.acquire(blocking=False)
            if acquired:
                return lock
            else:
                logger.info(f"Lock held by another instance, skipping [{lock_key}]")
                return None
        except redis.ConnectionError:
            logger.warning(
                f"Redis connection lost during lock acquire, degraded mode [{lock_key}]"
            )
            return True
        except Exception as e:
            logger.warning(f"Lock error, allowing execution [{lock_key}]: {e}")
            return True  # Fail open

    def release(self, lock: Optional[DistributedLock | bool]) -> None:
        """Release a previously acquired lock."""
        if lock is None or lock is True:
            return  # Degraded mode or no lock -- no-op
        if isinstance(lock, DistributedLock):
            try:
                lock.release()
            except Exception as e:
                logger.warning(f"Lock release error: {e}")
