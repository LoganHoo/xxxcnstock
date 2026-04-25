#!/usr/bin/env python3
"""
数据源管理器

管理多个数据源，实现自动故障转移
支持从配置文件读取数据源配置

主备策略：
- 主源: Baostock
- 备源1: Tencent
- 备源2: AKShare
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import yaml
from pathlib import Path

from .base import DataSourceProvider
from .providers import BaostockProvider, TencentProvider, AKShareProvider
from core.delisting_guard import get_delisting_guard

logger = logging.getLogger(__name__)


class DataSourceManager:
    """数据源管理器"""

    def __init__(self, config: Optional[Dict[str, Any]] = None, config_path: Optional[str] = None):
        """
        初始化数据源管理器

        Args:
            config: 配置字典，如果为None则从配置文件读取
            config_path: 配置文件路径，默认为 config/datasource.yaml
        """
        self.config = config or self._load_config(config_path)
        self.primary_provider: Optional[DataProvider] = None
        self.backup_providers: List[DataProvider] = []
        self.current_source: Optional[str] = None
        self.backup_sources: List[str] = []
        self._health_status: Dict[str, Dict] = {}
        self._failure_counts: Dict[str, int] = {}

        self._initialize_providers()

    def _load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """从YAML配置文件加载配置"""
        if config_path is None:
            # 默认配置文件路径
            project_root = Path(__file__).parent.parent.parent.parent
            config_path = project_root / "config" / "datasource.yaml"
        else:
            config_path = Path(config_path)

        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"已加载数据源配置文件: {config_path}")
                    return config.get('datasource', {})
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}")

        # 返回默认配置
        return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'primary': {
                'name': 'baostock',
                'enabled': True,
                'priority': 1
            },
            'backups': [
                {'name': 'tencent', 'enabled': True, 'priority': 1},
                {'name': 'akshare', 'enabled': True, 'priority': 2}
            ]
        }

    def _initialize_providers(self):
        """初始化数据源提供者 - 根据配置"""
        ds_config = self.config

        # 初始化主源
        primary_config = ds_config.get('primary', {})
        if primary_config.get('enabled', True):
            primary_name = primary_config.get('name', 'baostock')
            try:
                if primary_name == 'baostock':
                    self.primary_provider = BaostockProvider()
                    self.current_source = 'baostock'
                    logger.info(f"主数据源初始化成功: {primary_name}")
                elif primary_name == 'tencent':
                    self.primary_provider = TencentProvider()
                    self.current_source = 'tencent'
                    logger.info(f"主数据源初始化成功: {primary_name}")
                else:
                    logger.warning(f"不支持的主数据源: {primary_name}")
            except Exception as e:
                logger.error(f"主数据源初始化失败: {e}")

        # 初始化备源
        backup_configs = ds_config.get('backups', [])
        for backup_config in backup_configs:
            if not backup_config.get('enabled', True):
                continue

            backup_name = backup_config.get('name')
            try:
                if backup_name == 'tencent':
                    provider = TencentProvider()
                    self.backup_providers.append(provider)
                    self.backup_sources.append('tencent')
                    logger.info(f"备数据源初始化成功: {backup_name}")
                elif backup_name == 'akshare':
                    provider = AKShareProvider()
                    self.backup_providers.append(provider)
                    self.backup_sources.append('akshare')
                    logger.info(f"备数据源初始化成功: {backup_name}")
                else:
                    logger.warning(f"不支持的备数据源: {backup_name}")
            except Exception as e:
                logger.error(f"备数据源初始化失败 {backup_name}: {e}")

        logger.info(f"数据源初始化完成: 主源={self.current_source}, 备源={self.backup_sources}")

    def initialize(self):
        """初始化连接"""
        # BaostockProvider 使用 _login 方法，不需要显式调用
        # TencentProvider 和 AKShareProvider 也不需要显式连接
        pass

    @property
    def is_primary_active(self) -> bool:
        """检查主源是否活跃"""
        return self.current_source in ['baostock', 'tencent'] and self.primary_provider is not None

    async def fetch_kline(
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
        if self.primary_provider and self.current_source in ['baostock', 'tencent']:
            try:
                df = await self.primary_provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    # 成功，重置失败计数
                    self._failure_counts[self.current_source] = 0
                    return df
                else:
                    # 空数据，视为失败
                    self._record_failure(self.current_source)
            except Exception as e:
                logger.warning(f"主源 {self.current_source} 获取K线失败: {e}")
                self._record_failure(self.current_source)

        # 主源失败，尝试备源（按优先级顺序）
        for provider in self.backup_providers:
            try:
                df = await provider.fetch_kline(code, start_date, end_date, frequency)
                if not df.empty:
                    self.current_source = provider.name.lower()
                    logger.info(f"切换到备数据源: {provider.name}")
                    # 重置该备源的失败计数
                    self._failure_counts[provider.name.lower()] = 0
                    return df
                else:
                    self._record_failure(provider.name.lower())
            except Exception as e:
                logger.warning(f"备源 {provider.name} 获取K线失败: {e}")
                self._record_failure(provider.name.lower())

        raise Exception("所有数据源均失败")

    def _record_failure(self, source_name: str):
        """记录数据源失败"""
        if source_name not in self._failure_counts:
            self._failure_counts[source_name] = 0
        self._failure_counts[source_name] += 1

        # 检查是否需要切换主源
        failover_config = self.config.get('failover', {})
        threshold = failover_config.get('failure_threshold', 3)

        if source_name == 'baostock' and self._failure_counts[source_name] >= threshold:
            logger.warning(f"主源 Baostock 连续失败 {threshold} 次，切换到备源")
            # 切换到第一个可用的备源
            if self.backup_providers:
                self.current_source = self.backup_providers[0].name.lower()

    async def fetch_stock_list(self, filter_delisted: bool = True) -> pd.DataFrame:
        """
        获取股票列表
        
        Args:
            filter_delisted: 是否过滤退市股票，默认为True
        """
        df = pd.DataFrame()
        
        # 首先尝试从本地缓存加载
        try:
            from core.paths import get_data_path
            cache_file = get_data_path() / "stock_list.parquet"
            if cache_file.exists():
                df = pd.read_parquet(cache_file)
                # 过滤掉指数，只保留个股
                if 'code' in df.columns:
                    df = df[df['code'].str.match(r'^\d{6}$', na=False)]
                if not df.empty:
                    logger.info(f"从本地缓存加载股票列表: {len(df)} 只")
                    # 过滤退市股票
                    if filter_delisted and 'name' in df.columns:
                        delisting_guard = get_delisting_guard()
                        df = delisting_guard.filter_stock_list(df)
                    return df
        except Exception as e:
            logger.warning(f"从本地缓存加载股票列表失败: {e}")
        
        # 股票列表优先使用主源
        if self.primary_provider:
            try:
                df = await self.primary_provider.fetch_stock_list()
                if not df.empty:
                    logger.info(f"主源获取股票列表: {len(df)} 只")
            except Exception as e:
                logger.warning(f"主源获取股票列表失败: {e}")

        # 主源失败，尝试备源（akshare通常有更完整的列表）
        if df.empty:
            for provider in self.backup_providers:
                if provider.name.lower() == 'akshare':
                    try:
                        df = await provider.fetch_stock_list()
                        if not df.empty:
                            logger.info(f"AKShare获取股票列表: {len(df)} 只")
                            break
                    except Exception as e:
                        logger.warning(f"AKShare获取股票列表失败: {e}")

        # 再尝试其他备源
        if df.empty:
            for provider in self.backup_providers:
                if provider.name.lower() != 'akshare':
                    try:
                        df = await provider.fetch_stock_list()
                        if not df.empty:
                            logger.info(f"{provider.name}获取股票列表: {len(df)} 只")
                            break
                    except Exception as e:
                        logger.warning(f"{provider.name}获取股票列表失败: {e}")

        # 过滤退市股票
        if filter_delisted and not df.empty and 'name' in df.columns:
            delisting_guard = get_delisting_guard()
            original_count = len(df)
            df = delisting_guard.filter_stock_list(df)
            filtered_count = original_count - len(df)
            if filtered_count > 0:
                logger.info(f"已过滤 {filtered_count} 只退市/风险股票，剩余 {len(df)} 只")
        
        return df

    async def fetch_realtime_quotes(self) -> pd.DataFrame:
        """获取实时行情 - 优先使用更快的备源"""
        # 实时行情优先使用腾讯（通常更快）
        for provider in self.backup_providers:
            if provider.name.lower() == 'tencent':
                try:
                    df = await provider.fetch_realtime_quotes()
                    if not df.empty:
                        return df
                except Exception as e:
                    logger.warning(f"Tencent获取实时行情失败: {e}")

        # 再尝试AKShare
        for provider in self.backup_providers:
            if provider.name.lower() == 'akshare':
                try:
                    df = await provider.fetch_realtime_quotes()
                    if not df.empty:
                        return df
                except Exception as e:
                    logger.warning(f"AKShare获取实时行情失败: {e}")

        # 最后尝试主源
        if self.primary_provider:
            try:
                df = await self.primary_provider.fetch_realtime_quotes()
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"主源获取实时行情失败: {e}")

        return pd.DataFrame()

    async def check_primary_health(self) -> bool:
        """检查主源健康状态"""
        if not self.primary_provider:
            return False

        try:
            is_healthy = await self.primary_provider.health_check()
        except Exception as e:
            logger.warning(f"主源健康检查失败: {e}")
            is_healthy = False

        failover_config = self.config.get('failover', {})
        auto_recover = failover_config.get('auto_recover', True)

        # 如果主源健康且当前不是主源，切回主源
        if is_healthy and self.current_source != 'baostock' and auto_recover:
            recover_delay = failover_config.get('recover_delay', 300)
            logger.info(f"主源 Baostock 恢复，{recover_delay}秒后切回")
            # 实际项目中可以使用定时器延迟切回
            self.current_source = 'baostock'
            logger.info("已切回主数据源: Baostock")

        self._health_status['baostock'] = {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'last_check': datetime.now(),
            'failure_count': self._failure_counts.get('baostock', 0)
        }

        return is_healthy

    def get_health_status(self) -> Dict[str, Dict]:
        """获取所有数据源健康状态"""
        # 确保所有源都有状态记录
        all_sources = ['baostock'] + self.backup_sources
        for source in all_sources:
            if source not in self._health_status:
                self._health_status[source] = {
                    'status': 'unknown',
                    'last_check': None,
                    'failure_count': self._failure_counts.get(source, 0)
                }
        return self._health_status.copy()

    def get_current_source(self) -> str:
        """获取当前使用的数据源"""
        return self.current_source or 'unknown'

    def simulate_primary_failure(self):
        """模拟主源失效 (用于测试)"""
        if self.backup_providers:
            self.current_source = self.backup_providers[0].name.lower()
            logger.info(f"模拟主源失效，切换到: {self.current_source}")

    def simulate_primary_recovery(self):
        """模拟主源恢复 (用于测试)"""
        if self.primary_provider:
            self.current_source = 'baostock'
            logger.info("模拟主源恢复，切回: baostock")


# 全局管理器实例
_datasource_manager: Optional[DataSourceManager] = None


def get_datasource_manager(config: Optional[Dict] = None, config_path: Optional[str] = None) -> DataSourceManager:
    """获取数据源管理器单例"""
    global _datasource_manager
    if _datasource_manager is None:
        _datasource_manager = DataSourceManager(config=config, config_path=config_path)
    return _datasource_manager
