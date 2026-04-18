"""
锁文件管理器

提供进程间互斥锁功能，防止并发冲突
"""
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
import psutil

from core.logger import setup_logger

logger = setup_logger("lock_manager")


class LockManager:
    """
    锁文件管理器
    
    用于管理数据采集任务的并发控制
    """
    
    def __init__(self, lock_file_path: Optional[Path] = None, version: str = "v2"):
        """
        初始化锁管理器
        
        Args:
            lock_file_path: 锁文件路径，默认为 data/.data_fetch.lock
            version: 锁版本标识
        """
        if lock_file_path is None:
            # 从项目根目录计算
            project_root = Path(__file__).parent.parent.parent
            lock_file_path = project_root / "data" / ".data_fetch.lock"
        
        self.lock_file = Path(lock_file_path)
        self.version = version
        self._acquired = False
    
    def acquire(self, metadata: Optional[Dict] = None) -> bool:
        """
        获取锁
        
        Args:
            metadata: 额外的元数据信息
            
        Returns:
            是否成功获取锁
        """
        if self.lock_file.exists():
            # 检查是否已存在有效的锁
            try:
                with open(self.lock_file, 'r') as f:
                    data = json.load(f)
                    pid = data.get('pid')
                
                if pid and psutil.pid_exists(pid):
                    logger.warning(f"锁已被其他进程占用 (PID: {pid})")
                    return False
                else:
                    logger.info("发现残留锁文件，清理后继续")
                    self.release()
            except Exception as e:
                logger.warning(f"读取锁文件失败: {e}，尝试清理")
                self.release()
        
        # 创建新锁
        lock_data = {
            'pid': os.getpid(),
            'start_time': datetime.now().isoformat(),
            'version': self.version
        }
        
        if metadata:
            lock_data.update(metadata)
        
        try:
            self.lock_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.lock_file, 'w') as f:
                json.dump(lock_data, f)
            
            self._acquired = True
            logger.info(f"锁文件已创建: {self.lock_file}")
            return True
            
        except Exception as e:
            logger.error(f"创建锁文件失败: {e}")
            return False
    
    def release(self) -> bool:
        """
        释放锁
        
        Returns:
            是否成功释放
        """
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
                logger.info(f"锁文件已移除: {self.lock_file}")
            
            self._acquired = False
            return True
            
        except Exception as e:
            logger.error(f"移除锁文件失败: {e}")
            return False
    
    def check_conflict(self) -> bool:
        """
        检查是否存在冲突（其他进程正在运行）
        
        Returns:
            是否存在冲突
        """
        if not self.lock_file.exists():
            return False
        
        try:
            with open(self.lock_file, 'r') as f:
                data = json.load(f)
                pid = data.get('pid')
            
            if pid and psutil.pid_exists(pid):
                logger.warning(f"检测到其他采集任务正在运行 (PID: {pid})")
                return True
            else:
                # 残留锁文件
                logger.info("发现残留锁文件")
                return False
                
        except Exception as e:
            logger.warning(f"检查锁文件失败: {e}")
            return False
    
    def get_lock_info(self) -> Optional[Dict]:
        """
        获取锁信息
        
        Returns:
            锁信息字典或None
        """
        if not self.lock_file.exists():
            return None
        
        try:
            with open(self.lock_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"读取锁文件失败: {e}")
            return None
    
    def __enter__(self):
        """上下文管理器入口"""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.release()
        return False
    
    def __del__(self):
        """析构时自动释放锁"""
        if self._acquired:
            self.release()
