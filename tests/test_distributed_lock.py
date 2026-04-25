"""
分布式锁测试
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock

from core.distributed_lock import (
    DistributedLock,
    TaskStateManager,
    HeartbeatRenewer,
    DeadlockDetector,
    distributed_lock
)


class TestDistributedLock:
    """测试分布式锁"""
    
    def test_acquire_lock_success(self):
        """测试成功获取锁"""
        mock_redis = Mock()
        mock_redis.set.return_value = True
        
        lock = DistributedLock(mock_redis, "test_lock", ttl_seconds=60, auto_renew=False)
        
        result = lock.acquire(blocking=False)
        
        assert result is True
        assert lock._acquired is True
        mock_redis.set.assert_called_once()
    
    def test_acquire_lock_failed(self):
        """测试获取锁失败"""
        mock_redis = Mock()
        mock_redis.set.return_value = False
        mock_redis.get.return_value = b"other_lock_value"
        
        lock = DistributedLock(mock_redis, "test_lock", ttl_seconds=60, auto_renew=False)
        
        result = lock.acquire(blocking=False)
        
        assert result is False
        assert lock._acquired is False
    
    def test_release_lock(self):
        """测试释放锁"""
        mock_redis = Mock()
        mock_redis.set.return_value = True
        mock_redis.eval.return_value = 1
        
        lock = DistributedLock(mock_redis, "test_lock", ttl_seconds=60, auto_renew=False)
        lock.acquire(blocking=False)
        
        result = lock.release()
        
        assert result is True
        assert lock._acquired is False
    
    def test_context_manager(self):
        """测试上下文管理器"""
        mock_redis = Mock()
        mock_redis.set.return_value = True
        mock_redis.eval.return_value = 1
        
        lock = DistributedLock(mock_redis, "test_lock", ttl_seconds=60, auto_renew=False)
        
        with lock:
            assert lock._acquired is True
        
        assert lock._acquired is False


class TestTaskStateManager:
    """测试任务状态管理器"""
    
    def test_set_and_get_state(self):
        """测试设置和获取状态"""
        mock_redis = Mock()
        mock_redis.setex.return_value = True
        mock_redis.get.return_value = str({
            "state": "running",
            "updated_at": time.time(),
            "metadata": {"key": "value"}
        }).encode()
        
        manager = TaskStateManager(mock_redis)
        
        # 设置状态
        result = manager.set_state("task1", "running", {"key": "value"})
        assert result is True
        
        # 获取状态
        state = manager.get_state("task1")
        assert state is not None
        assert state["state"] == "running"
    
    def test_is_running(self):
        """测试检查是否正在运行"""
        mock_redis = Mock()
        mock_redis.get.return_value = str({
            "state": "running",
            "updated_at": time.time(),
            "metadata": {}
        }).encode()
        
        manager = TaskStateManager(mock_redis)
        
        assert manager.is_running("task1") is True
    
    def test_clear_state(self):
        """测试清除状态"""
        mock_redis = Mock()
        mock_redis.delete.return_value = 1
        
        manager = TaskStateManager(mock_redis)
        
        result = manager.clear_state("task1")
        
        assert result is True


class TestHeartbeatRenewer:
    """测试心跳续期器"""
    
    def test_start_stop(self):
        """测试启动和停止"""
        mock_redis = Mock()
        
        renewer = HeartbeatRenewer(
            mock_redis,
            "scheduler1",
            ttl_seconds=60,
            interval=30
        )
        
        # 启动
        renewer.start()
        assert renewer._running is True
        assert renewer._thread is not None
        
        # 停止
        renewer.stop()
        assert renewer._running is False
    
    def test_is_alive(self):
        """测试检查心跳是否存活"""
        mock_redis = Mock()
        mock_redis.exists.return_value = 1
        
        renewer = HeartbeatRenewer(mock_redis, "scheduler1")
        
        assert renewer.is_alive() is True
        
        mock_redis.exists.return_value = 0
        assert renewer.is_alive() is False


class TestDeadlockDetector:
    """测试死锁检测器"""
    
    def test_start_stop(self):
        """测试启动和停止"""
        mock_redis = Mock()
        mock_redis.keys.return_value = []
        
        detector = DeadlockDetector(mock_redis)
        
        # 启动
        detector.start()
        assert detector._running is True
        
        # 停止
        detector.stop()
        assert detector._running is False


class TestDistributedLockContextManager:
    """测试分布式锁上下文管理器"""
    
    def test_successful_lock(self):
        """测试成功获取锁"""
        mock_redis = Mock()
        mock_redis.set.return_value = True
        mock_redis.eval.return_value = 1
        
        with distributed_lock(mock_redis, "test_lock", ttl_seconds=60, blocking=False):
            pass  # 锁应该被获取和释放
        
        # 验证锁被释放
        assert mock_redis.eval.called
    
    def test_lock_timeout(self):
        """测试锁超时"""
        mock_redis = Mock()
        mock_redis.set.return_value = False
        mock_redis.get.return_value = b"other_value"
        
        with pytest.raises(TimeoutError):
            with distributed_lock(mock_redis, "test_lock", timeout=0.1):
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
