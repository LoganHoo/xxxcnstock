"""
因子库基类
定义所有因子的统一接口
"""
from abc import ABC, abstractmethod
import polars as pl
from typing import Dict, Any, Optional
from core.logger import get_logger

logger = get_logger(__name__)


class BaseFactor(ABC):
    """因子基类"""
    
    def __init__(
        self, 
        name: str, 
        category: str, 
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        self.name = name
        self.category = category
        self.params = params or {}
        self.description = description
        self.logger = get_logger(f"{__name__}.{name}")
    
    @abstractmethod
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        计算因子值
        
        Args:
            data: 包含 K 线数据的 DataFrame
                  必须包含: code, trade_date, open, high, low, close, volume
        
        Returns:
            添加了因子列的 DataFrame
        """
        pass
    
    def normalize(self, value: float, min_val: float = 0, max_val: float = 100) -> float:
        """
        标准化因子值到 0-1 区间
        
        Args:
            value: 原始值
            min_val: 最小值
            max_val: 最大值
        
        Returns:
            标准化后的值 (0-1)
        """
        if max_val == min_val:
            return 0.5
        return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))
    
    def get_score(self, factor_value: float) -> float:
        """
        将因子值转换为得分 (0-100)
        
        Args:
            factor_value: 因子值
        
        Returns:
            得分 (0-100)
        """
        return self.normalize(factor_value) * 100
    
    def get_factor_column_name(self) -> str:
        """获取因子列名"""
        return f"factor_{self.name}"
    
    def __repr__(self) -> str:
        return f"Factor(name={self.name}, category={self.category}, params={self.params})"


class FactorRegistry:
    """因子注册表"""
    
    _instance = None
    _factors: Dict[str, type] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def register(cls, name: str, factor_class: type):
        """注册因子"""
        if name not in cls._factors:
            cls._factors[name] = factor_class
            logger.info(f"注册因子: {name}")
        else:
            logger.debug(f"因子已注册，跳过: {name}")
    
    @classmethod
    def get(cls, name: str) -> Optional[type]:
        """获取因子类"""
        return cls._factors.get(name)
    
    @classmethod
    def list_all(cls) -> Dict[str, type]:
        """列出所有已注册因子"""
        return cls._factors.copy()


def register_factor(name: str):
    """因子注册装饰器"""
    def decorator(cls):
        FactorRegistry.register(name, cls)
        return cls
    return decorator
