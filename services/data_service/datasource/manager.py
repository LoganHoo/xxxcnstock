#!/usr/bin/env python3
"""
数据源管理器

管理多个数据源，实现自动故障转移
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

from .base import DataSourceProvider
from .tushare_provider import TushareProvider

logger = logging.getLogger(__name__)


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.primary_provider: Optional[DataSourceProvider] = None
        self.backup_providers: List[DataSourceProvider] = []
        self.current_source: Optional[str] = None
        self.backup_sources: List[str] = []
        self._health_status: Dict[str, Dict] = {}
        
        self._initialize_providers()
    
    def _initialize_providers(self):
        """初始化数据源提供者"""
        # 主源: Tushare
        try:
            self.primary_provider = TushareProvider(
                token=self.config.get('tushare_token')
            )
            self.current_source = 'tushare'
            logger.info("Primary provider (Tushare) initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Tushare: {e}")
        
        # 备源将在后续任务中实现
        self.backup_sources = ['akshare', 'baostock']
    
    def initialize(self):
        """初始化连接"""
        if self.primary_provider:
            self.primary_provider.connect()
    
    @property
    def is_primary_active(self) -> bool:
        """检查主源是否活跃"""
        return self.current_source == 'tushare' and self.primary_provider is not None
    
    def fetch_kline(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = 'd'
    ) -> pd.DataFrame:
        """
        获取K线数据，自动故障转移
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率
        
        Returns:
            K线数据DataFrame
        """
        # 尝试主源
        if self.primary_provider:
            try:
                df = self.primary_provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"Primary source failed: {e}")
        
        # 主源失败，尝试备源
        for provider in self.backup_providers:
            try:
                df = provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    self.current_source = provider.name
                    logger.info(f"Switched to backup source: {provider.name}")
                    return df
            except Exception as e:
                logger.warning(f"Backup source {provider.name} failed: {e}")
        
        raise Exception("All data sources failed")
    
    def check_primary_health(self) -> bool:
        """检查主源健康状态"""
        if not self.primary_provider:
            return False
        
        is_healthy = self.primary_provider.health_check()
        
        if is_healthy and self.current_source != 'tushare':
            # 主源恢复，切回主源
            self.current_source = 'tushare'
            logger.info("Primary source recovered, switched back")
        
        self._health_status['tushare'] = {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'last_check': datetime.now()
        }
        
        return is_healthy
    
    def get_health_status(self) -> Dict[str, Dict]:
        """获取所有数据源健康状态"""
        # 确保所有源都有状态记录
        for source in ['tushare'] + self.backup_sources:
            if source not in self._health_status:
                self._health_status[source] = {
                    'status': 'unknown',
                    'last_check': None
                }
        return self._health_status.copy()
    
    def simulate_primary_failure(self):
        """模拟主源失效 (用于测试)"""
        self.current_source = 'akshare'
    
    def simulate_primary_recovery(self):
        """模拟主源恢复 (用于测试)"""
        if self.primary_provider and self.primary_provider.health_check():
            self.current_source = 'tushare'
