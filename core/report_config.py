"""
报告配置管理模块

用于加载和管理报告相关的配置
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# 默认配置文件路径
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "reports" / "report_config.yaml"


@dataclass
class ReportTypeConfig:
    """报告类型配置"""
    name: str
    description: str
    schedule: str
    script: str
    template: str
    required_data: List[str] = field(default_factory=list)
    optional_data: List[str] = field(default_factory=list)
    output: Dict[str, Any] = field(default_factory=dict)
    timeout: int = 300


@dataclass
class DataFileConfig:
    """数据文件配置"""
    path: str
    description: str
    max_age_hours: int = 24
    format: str = "json"
    fallback_to_yesterday: bool = False


@dataclass
class EmailConfig:
    """邮件配置"""
    enabled: bool = True
    default_recipients: List[str] = field(default_factory=list)
    subject_prefix: Dict[str, str] = field(default_factory=dict)


@dataclass
class DataAvailabilityConfig:
    """数据可用性检查配置"""
    enabled: bool = True
    max_age_hours: int = 24
    check_before_generate: bool = True
    fail_on_missing_required: bool = True
    warn_on_missing_optional: bool = True


@dataclass
class CacheConfig:
    """缓存配置"""
    enabled: bool = True
    ttl_seconds: int = 300
    max_size: int = 100


@dataclass
class MonitoringConfig:
    """监控配置"""
    enabled: bool = True
    log_level: str = "INFO"
    save_status: bool = True
    alert_on_failure: bool = True


class ReportConfigManager:
    """报告配置管理器"""

    _instance = None
    _config = None

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self, config_path: Optional[Path] = None):
        """
        加载配置文件

        Args:
            config_path: 配置文件路径，默认使用默认路径
        """
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            logger.info(f"配置文件加载成功: {config_path}")
        except FileNotFoundError:
            logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
            self._config = self._get_default_config()
        except yaml.YAMLError as e:
            logger.error(f"配置文件解析失败: {e}")
            self._config = self._get_default_config()

    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'report_types': {},
            'data_files': {},
            'email': {
                'enabled': True,
                'default_recipients': ['287363@qq.com']
            },
            'data_availability': {
                'enabled': True,
                'max_age_hours': 24
            },
            'cache': {
                'enabled': True,
                'ttl_seconds': 300
            },
            'monitoring': {
                'enabled': True,
                'log_level': 'INFO'
            }
        }

    def get_report_type(self, report_type: str) -> Optional[ReportTypeConfig]:
        """
        获取报告类型配置

        Args:
            report_type: 报告类型名称

        Returns:
            ReportTypeConfig or None
        """
        config = self._config.get('report_types', {}).get(report_type)
        if config is None:
            return None

        return ReportTypeConfig(
            name=config.get('name', report_type),
            description=config.get('description', ''),
            schedule=config.get('schedule', ''),
            script=config.get('script', ''),
            template=config.get('template', ''),
            required_data=config.get('required_data', []),
            optional_data=config.get('optional_data', []),
            output=config.get('output', {}),
            timeout=config.get('timeout', 300)
        )

    def get_data_file(self, data_name: str) -> Optional[DataFileConfig]:
        """
        获取数据文件配置

        Args:
            data_name: 数据名称

        Returns:
            DataFileConfig or None
        """
        config = self._config.get('data_files', {}).get(data_name)
        if config is None:
            return None

        return DataFileConfig(
            path=config.get('path', ''),
            description=config.get('description', ''),
            max_age_hours=config.get('max_age_hours', 24),
            format=config.get('format', 'json'),
            fallback_to_yesterday=config.get('fallback_to_yesterday', False)
        )

    def get_email_config(self) -> EmailConfig:
        """获取邮件配置"""
        config = self._config.get('email', {})
        return EmailConfig(
            enabled=config.get('enabled', True),
            default_recipients=config.get('default_recipients', ['287363@qq.com']),
            subject_prefix=config.get('subject_prefix', {})
        )

    def get_data_availability_config(self) -> DataAvailabilityConfig:
        """获取数据可用性检查配置"""
        config = self._config.get('data_availability', {})
        return DataAvailabilityConfig(
            enabled=config.get('enabled', True),
            max_age_hours=config.get('max_age_hours', 24),
            check_before_generate=config.get('check_before_generate', True),
            fail_on_missing_required=config.get('fail_on_missing_required', True),
            warn_on_missing_optional=config.get('warn_on_missing_optional', True)
        )

    def get_cache_config(self) -> CacheConfig:
        """获取缓存配置"""
        config = self._config.get('cache', {})
        return CacheConfig(
            enabled=config.get('enabled', True),
            ttl_seconds=config.get('ttl_seconds', 300),
            max_size=config.get('max_size', 100)
        )

    def get_monitoring_config(self) -> MonitoringConfig:
        """获取监控配置"""
        config = self._config.get('monitoring', {})
        return MonitoringConfig(
            enabled=config.get('enabled', True),
            log_level=config.get('log_level', 'INFO'),
            save_status=config.get('save_status', True),
            alert_on_failure=config.get('alert_on_failure', True)
        )

    def get_all_report_types(self) -> List[str]:
        """获取所有报告类型"""
        return list(self._config.get('report_types', {}).keys())

    def get_all_data_files(self) -> List[str]:
        """获取所有数据文件名称"""
        return list(self._config.get('data_files', {}).keys())

    def reload(self):
        """重新加载配置"""
        self.load_config()


# 全局配置管理器实例
_config_manager: Optional[ReportConfigManager] = None


def get_config_manager() -> ReportConfigManager:
    """获取配置管理器实例（单例）"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ReportConfigManager()
    return _config_manager


# 便捷函数
def get_report_type_config(report_type: str) -> Optional[ReportTypeConfig]:
    """获取报告类型配置"""
    return get_config_manager().get_report_type(report_type)


def get_data_file_config(data_name: str) -> Optional[DataFileConfig]:
    """获取数据文件配置"""
    return get_config_manager().get_data_file(data_name)


def get_email_settings() -> EmailConfig:
    """获取邮件配置"""
    return get_config_manager().get_email_config()


def get_data_availability_settings() -> DataAvailabilityConfig:
    """获取数据可用性检查配置"""
    return get_config_manager().get_data_availability_config()
