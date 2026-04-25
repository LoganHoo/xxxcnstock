"""
分布式锁模块

基于 Redis 的分布式锁实现，用于双调度器协调。
支持锁续期、死锁检测和自动释放。
"""

import time
import uuid
import threading
from typing import Optional, Callable
from contextlib import contextmanager

import redis
from loguru import logger


class DistributedLock:
    """
    分布式锁
    
    基于 Redis Redlock 算法的简化实现。
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        lock_key: str,
        ttl_seconds: int = 60,
        auto_renew: bool = True
    ):
        """
        初始化分布式锁
        
        Args:
            redis_client: Redis 客户端
            lock_key: 锁键名
            ttl_seconds: 锁过期时间
            auto_renew: 是否自动续期
        """
        self.redis = redis_client
        self.lock_key = f"xcnstock:lock:{lock_key}"
        self.ttl = ttl_seconds
        self.auto_renew = auto_renew
        
        self.lock_value = str(uuid.uuid4())
        self._acquired = False
        self._renew_thread: Optional[threading.Thread] = None
        self._stop_renew = threading.Event()
    
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        获取锁
        
        Args:
            blocking: 是否阻塞等待
            timeout: 阻塞超时时间（秒）
            
        Returns:
            bool: 是否成功获取锁
        """
        start_time = time.time()
        
        while True:
            # 尝试获取锁
            acquired = self.redis.set(
                self.lock_key,
                self.lock_value,
                nx=True,  # 仅当 key 不存在时才设置
                ex=self.ttl
            )
            
            if acquired:
                self._acquired = True
                logger.debug(f"Lock acquired: {self.lock_key}")
                
                # 启动续期线程
                if self.auto_renew:
                    self._start_renewal()
                
                return True
            
            if not blocking:
                return False
            
            # 检查超时
            if timeout and (time.time() - start_time) >= timeout:
                return False
            
            # 短暂等待后重试
            time.sleep(0.1)
    
    def release(self) -> bool:
        """
        释放锁
        
        Returns:
            bool: 是否成功释放
        """
        if not self._acquired:
            return False
        
        # 停止续期线程
        if self._renew_thread:
            self._stop_renew.set()
            self._renew_thread.join(timeout=1)
        
        # 使用 Lua 脚本确保原子性
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = self.redis.eval(lua_script, 1, self.lock_key, self.lock_value)
            released = result == 1
            
            if released:
                logger.debug(f"Lock released: {self.lock_key}")
            
            self._acquired = False
            return released
        except Exception as e:
            logger.error(f"Failed to release lock: {e}")
            return False
    
    def _start_renewal(self):
        """启动续期线程"""
        self._stop_renew.clear()
        self._renew_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renew_thread.start()
    
    def _renew_loop(self):
        """续期循环"""
        # 在锁过期前 1/3 时间续期
        renew_interval = self.ttl / 3
        
        while not self._stop_renew.is_set():
            self._stop_renew.wait(renew_interval)
            
            if self._stop_renew.is_set():
                break
            
            # 续期
            try:
                lua_script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("expire", KEYS[1], ARGV[2])
                else
                    return 0
                end
                """
                result = self.redis.eval(
                    lua_script, 1, self.lock_key, self.lock_value, str(self.ttl)
                )
                
                if result != 1:
                    logger.warning(f"Lock renewal failed: {self.lock_key}")
                    break
            except Exception as e:
                logger.error(f"Lock renewal error: {e}")
                break
    
    def is_locked(self) -> bool:
        """检查是否持有锁"""
        if not self._acquired:
            return False
        
        try:
            value = self.redis.get(self.lock_key)
            return value == self.lock_value.encode() if value else False
        except Exception:
            return False
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class TaskStateManager:
    """
    任务状态管理器
    
    管理任务执行状态，支持状态同步和查询。
    """
    
    def __init__(self, redis_client: redis.Redis, key_prefix: str = "xcnstock:task:"):
        self.redis = redis_client
        self.key_prefix = key_prefix
    
    def _make_key(self, task_id: str) -> str:
        """生成任务键"""
        return f"{self.key_prefix}{task_id}"
    
    def set_state(
        self,
        task_id: str,
        state: str,
        metadata: Optional[dict] = None,
        ttl: int = 86400
    ) -> bool:
        """
        设置任务状态
        
        Args:
            task_id: 任务 ID
            state: 状态 (pending, running, success, failed)
            metadata: 元数据
            ttl: 过期时间（秒）
            
        Returns:
            bool: 是否成功
        """
        key = self._make_key(task_id)
        data = {
            "state": state,
            "updated_at": time.time(),
            "metadata": metadata or {}
        }
        
        try:
            self.redis.setex(key, ttl, str(data))
            return True
        except Exception as e:
            logger.error(f"Failed to set task state: {e}")
            return False
    
    def get_state(self, task_id: str) -> Optional[dict]:
        """
        获取任务状态
        
        Args:
            task_id: 任务 ID
            
        Returns:
            dict: 任务状态，不存在返回 None
        """
        key = self._make_key(task_id)
        
        try:
            data = self.redis.get(key)
            if data:
                import ast
                return ast.literal_eval(data.decode('utf-8'))
            return None
        except Exception as e:
            logger.error(f"Failed to get task state: {e}")
            return None
    
    def is_running(self, task_id: str) -> bool:
        """检查任务是否正在运行"""
        state = self.get_state(task_id)
        return state is not None and state.get("state") == "running"
    
    def clear_state(self, task_id: str) -> bool:
        """清除任务状态"""
        key = self._make_key(task_id)
        
        try:
            return self.redis.delete(key) > 0
        except Exception as e:
            logger.error(f"Failed to clear task state: {e}")
            return False
    
    def get_all_states(self, pattern: str = "*") -> dict:
        """
        获取所有任务状态
        
        Args:
            pattern: 匹配模式
            
        Returns:
            dict: 任务状态字典
        """
        key_pattern = f"{self.key_prefix}{pattern}"
        
        try:
            keys = self.redis.keys(key_pattern)
            states = {}
            
            for key in keys:
                task_id = key.decode('utf-8').replace(self.key_prefix, "")
                data = self.redis.get(key)
                if data:
                    import ast
                    states[task_id] = ast.literal_eval(data.decode('utf-8'))
            
            return states
        except Exception as e:
            logger.error(f"Failed to get all states: {e}")
            return {}


class HeartbeatRenewer:
    """
    心跳续期器
    
    维护调度器心跳，支持自动续期。
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        scheduler_id: str,
        ttl_seconds: int = 60,
        interval: int = 30
    ):
        self.redis = redis_client
        self.scheduler_id = scheduler_id
        self.ttl = ttl_seconds
        self.interval = interval
        
        self.key = f"xcnstock:heartbeat:{scheduler_id}"
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False
    
    def start(self):
        """启动心跳续期"""
        if self._running:
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        self._running = True
        
        logger.info(f"Heartbeat renewer started for {self.scheduler_id}")
    
    def stop(self):
        """停止心跳续期"""
        if not self._running:
            return
        
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        
        self._running = False
        
        # 清除心跳
        try:
            self.redis.delete(self.key)
        except Exception as e:
            logger.error(f"Failed to clear heartbeat: {e}")
        
        logger.info(f"Heartbeat renewer stopped for {self.scheduler_id}")
    
    def _heartbeat_loop(self):
        """心跳循环"""
        while not self._stop_event.is_set():
            try:
                heartbeat_data = {
                    "scheduler_id": self.scheduler_id,
                    "timestamp": time.time(),
                    "status": "healthy"
                }
                self.redis.setex(self.key, self.ttl, str(heartbeat_data))
            except Exception as e:
                logger.error(f"Heartbeat update failed: {e}")
            
            self._stop_event.wait(self.interval)
    
    def is_alive(self) -> bool:
        """检查心跳是否存活"""
        try:
            return self.redis.exists(self.key) == 1
        except Exception:
            return False


class DeadlockDetector:
    """
    死锁检测器
    
    检测并处理死锁情况。
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        check_interval: int = 60,
        max_lock_age: int = 300
    ):
        self.redis = redis_client
        self.check_interval = check_interval
        self.max_lock_age = max_lock_age
        
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False
    
    def start(self):
        """启动死锁检测"""
        if self._running:
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._detect_loop, daemon=True)
        self._thread.start()
        self._running = True
        
        logger.info("Deadlock detector started")
    
    def stop(self):
        """停止死锁检测"""
        if not self._running:
            return
        
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        
        self._running = False
        logger.info("Deadlock detector stopped")
    
    def _detect_loop(self):
        """检测循环"""
        while not self._stop_event.is_set():
            try:
                self._check_and_release_deadlocks()
            except Exception as e:
                logger.error(f"Deadlock detection error: {e}")
            
            self._stop_event.wait(self.check_interval)
    
    def _check_and_release_deadlocks(self):
        """检查并释放死锁"""
        # 获取所有锁
        lock_keys = self.redis.keys("xcnstock:lock:*")
        
        for key in lock_keys:
            try:
                ttl = self.redis.ttl(key)
                
                # 如果锁没有 TTL 或 TTL 异常，可能是死锁
                if ttl < 0:
                    # 获取锁信息
                    value = self.redis.get(key)
                    if value:
                        logger.warning(f"Potential deadlock detected: {key}")
                        # 可以选择在这里释放锁
                        # self.redis.delete(key)
            except Exception as e:
                logger.error(f"Error checking lock {key}: {e}")


@contextmanager
def distributed_lock(
    redis_client: redis.Redis,
    lock_key: str,
    ttl_seconds: int = 60,
    blocking: bool = True,
    timeout: Optional[float] = None
):
    """
    分布式锁上下文管理器
    
    使用示例：
        with distributed_lock(redis, "data_collection", ttl_seconds=300):
            # 执行需要互斥的操作
            collect_data()
    """
    lock = DistributedLock(redis_client, lock_key, ttl_seconds)
    acquired = lock.acquire(blocking=blocking, timeout=timeout)
    
    if not acquired:
        raise TimeoutError(f"Failed to acquire lock: {lock_key}")
    
    try:
        yield lock
    finally:
        lock.release()
