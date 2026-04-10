"""
数据版本检查混入类
用于获取锁定的交易日期，确保后续任务使用一致的数据
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path


class VersionAwareMixin:
    """版本感知混入类"""
    
    def __init__(self):
        self.version_manager = None
        self.locked_trade_date = None
    
    def init_version_manager(self):
        """初始化版本管理器"""
        if self.version_manager is None:
            try:
                from core.data_version_manager import get_version_manager
                self.version_manager = get_version_manager()
            except Exception as e:
                print(f"初始化版本管理器失败: {e}")
    
    def get_locked_trade_date(self) -> str:
        """
        获取锁定的交易日期
        
        Returns:
            str: 锁定的交易日期 (YYYY-MM-DD)，如果未锁定则返回None
        """
        self.init_version_manager()
        
        if self.version_manager:
            version = self.version_manager.get_locked_version()
            if version:
                self.locked_trade_date = version.get('trade_date')
                return self.locked_trade_date
        
        return None
    
    def is_version_locked(self) -> bool:
        """检查数据版本是否已锁定"""
        self.init_version_manager()
        
        if self.version_manager:
            return self.version_manager.is_version_locked()
        
        return False
    
    def check_and_warn(self):
        """检查版本锁定状态并警告"""
        if not self.is_version_locked():
            if hasattr(self, 'logger'):
                self.logger.warning("⚠️ 数据版本未锁定，分析可能使用不一致的数据")
            else:
                print("⚠️ 警告: 数据版本未锁定，分析可能使用不一致的数据")
        else:
            if hasattr(self, 'logger'):
                self.logger.info(f"✅ 使用锁定数据，交易日期: {self.locked_trade_date}")
            else:
                print(f"✅ 使用锁定数据，交易日期: {self.locked_trade_date}")
