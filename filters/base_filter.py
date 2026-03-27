"""
过滤器基类
定义所有过滤器的统一接口
"""
from abc import ABC, abstractmethod
import polars as pl
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseFilter(ABC):
    """过滤器基类"""
    
    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        self.name = name
        self.params = params or {}
        self.description = description
        self.enabled = self.params.get("enabled", True)
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """
        过滤股票列表
        
        Args:
            stock_list: 股票列表 DataFrame
                        必须包含: code, name 等字段
        
        Returns:
            过滤后的股票列表 DataFrame
        """
        pass
    
    def is_enabled(self) -> bool:
        """检查过滤器是否启用"""
        return self.enabled
    
    def get_filter_name(self) -> str:
        """获取过滤器名称"""
        return self.name
    
    def __repr__(self) -> str:
        return f"Filter(name={self.name}, enabled={self.enabled})"


class FilterRegistry:
    """过滤器注册表"""
    
    _instance = None
    _filters: Dict[str, type] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def register(cls, name: str, filter_class: type):
        """注册过滤器"""
        cls._filters[name] = filter_class
        logger.info(f"注册过滤器: {name}")
    
    @classmethod
    def get(cls, name: str) -> Optional[type]:
        """获取过滤器类"""
        return cls._filters.get(name)
    
    @classmethod
    def list_all(cls) -> Dict[str, type]:
        """列出所有已注册过滤器"""
        return cls._filters.copy()


def register_filter(name: str):
    """过滤器注册装饰器"""
    def decorator(cls):
        FilterRegistry.register(name, cls)
        return cls
    return decorator
