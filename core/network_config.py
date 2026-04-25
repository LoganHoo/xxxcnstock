#!/usr/bin/env python3
"""
网络配置模块

统一管理网络代理设置，确保所有HTTP请求都遵循统一的代理配置
"""
import os
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class NetworkConfig:
    """网络配置管理器"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._load_config()
    
    def _load_config(self):
        """从环境变量加载配置"""
        # 加载环境变量
        from dotenv import load_dotenv
        load_dotenv()
        
        # 代理配置
        self.http_proxy = os.getenv('HTTP_PROXY', '')
        self.https_proxy = os.getenv('HTTPS_PROXY', '')
        self.no_proxy = os.getenv('NO_PROXY', 'localhost,127.0.0.1')
        
        # 请求超时配置
        self.default_timeout = int(os.getenv('REQUEST_TIMEOUT', '30'))
        self.max_retries = int(os.getenv('REQUEST_MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('REQUEST_RETRY_DELAY', '1.0'))
        
        # 并发配置
        self.max_workers = int(os.getenv('REQUEST_MAX_WORKERS', '4'))
        self.request_delay = float(os.getenv('REQUEST_DELAY', '0.01'))
        
        # 应用到环境变量（影响 requests, urllib 等库）
        self._apply_to_environment()
        
        logger.info(f"网络配置加载完成: proxy={self.is_proxy_enabled()}")
    
    def _apply_to_environment(self):
        """将配置应用到系统环境变量"""
        if self.http_proxy:
            os.environ['HTTP_PROXY'] = self.http_proxy
            os.environ['http_proxy'] = self.http_proxy
        else:
            # 显式删除代理环境变量
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('http_proxy', None)
        
        if self.https_proxy:
            os.environ['HTTPS_PROXY'] = self.https_proxy
            os.environ['https_proxy'] = self.https_proxy
        else:
            # 显式删除代理环境变量
            os.environ.pop('HTTPS_PROXY', None)
            os.environ.pop('https_proxy', None)
        
        if self.no_proxy:
            os.environ['NO_PROXY'] = self.no_proxy
            os.environ['no_proxy'] = self.no_proxy
    
    def is_proxy_enabled(self) -> bool:
        """检查是否启用了代理"""
        return bool(self.http_proxy or self.https_proxy)
    
    def get_proxy_dict(self) -> Optional[Dict[str, str]]:
        """
        获取代理字典（用于 requests）
        
        Returns:
            代理字典或 None（如果代理未启用）
        """
        if not self.is_proxy_enabled():
            return None
        
        proxies = {}
        if self.http_proxy:
            proxies['http'] = self.http_proxy
        if self.https_proxy:
            proxies['https'] = self.https_proxy
        
        return proxies if proxies else None
    
    def get_requests_kwargs(self) -> Dict:
        """
        获取 requests 库的默认参数
        
        Returns:
            包含代理和超时设置的字典
        """
        kwargs = {
            'timeout': self.default_timeout,
        }
        
        proxies = self.get_proxy_dict()
        if proxies:
            kwargs['proxies'] = proxies
        
        return kwargs
    
    def disable_proxy(self):
        """临时禁用代理"""
        self.http_proxy = ''
        self.https_proxy = ''
        self._apply_to_environment()
        logger.info("代理已禁用")
    
    def enable_proxy(self, http_proxy: str = None, https_proxy: str = None):
        """
        启用代理
        
        Args:
            http_proxy: HTTP代理地址
            https_proxy: HTTPS代理地址
        """
        if http_proxy:
            self.http_proxy = http_proxy
        if https_proxy:
            self.https_proxy = https_proxy
        self._apply_to_environment()
        logger.info(f"代理已启用: http={self.http_proxy}, https={self.https_proxy}")


# 全局网络配置实例
_network_config: Optional[NetworkConfig] = None


def get_network_config() -> NetworkConfig:
    """获取网络配置单例"""
    global _network_config
    if _network_config is None:
        _network_config = NetworkConfig()
    return _network_config


def configure_requests_session(session):
    """
    配置 requests.Session 使用统一的代理设置
    
    Args:
        session: requests.Session 实例
    """
    config = get_network_config()
    
    # 设置代理
    proxies = config.get_proxy_dict()
    if proxies:
        session.proxies = proxies
    
    # 设置默认超时
    # 注意：requests.Session 没有默认超时属性，需要在请求时设置
    
    return session


# 便捷函数
def get_proxies() -> Optional[Dict[str, str]]:
    """获取代理字典"""
    return get_network_config().get_proxy_dict()


def get_timeout() -> int:
    """获取默认超时时间"""
    return get_network_config().default_timeout


def is_proxy_enabled() -> bool:
    """检查代理是否启用"""
    return get_network_config().is_proxy_enabled()


def disable_proxy():
    """禁用代理"""
    get_network_config().disable_proxy()


def enable_proxy(http_proxy: str = None, https_proxy: str = None):
    """启用代理"""
    get_network_config().enable_proxy(http_proxy, https_proxy)
