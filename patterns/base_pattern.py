"""
K线形态识别基类
"""
import polars as pl
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class SignalType(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class PatternStrength(Enum):
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    VERY_STRONG = 4


@dataclass
class PatternResult:
    pattern_name: str
    signal: SignalType
    strength: PatternStrength
    confidence: float
    description: str
    details: Dict[str, Any]


class BasePattern(ABC):
    """K线形态基类"""
    
    name: str = "base_pattern"
    description: str = "基础形态"
    min_periods: int = 1
    
    def __init__(self, params: Optional[Dict] = None):
        self.params = params or {}
    
    @abstractmethod
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        """
        检测形态
        
        Args:
            df: K线数据，包含 open, high, low, close, volume 列
        
        Returns:
            PatternResult 或 None
        """
        pass
    
    def _get_candle_properties(self, row: Dict) -> Dict[str, float]:
        """获取单根K线的属性"""
        open_price = float(row['open'])
        high = float(row['high'])
        low = float(row['low'])
        close = float(row['close'])
        
        body = abs(close - open_price)
        total_range = high - low
        upper_shadow = high - max(open_price, close)
        lower_shadow = min(open_price, close) - low
        
        is_bullish = close > open_price
        is_bearish = close < open_price
        
        if total_range > 0:
            body_ratio = body / total_range
            upper_shadow_ratio = upper_shadow / total_range
            lower_shadow_ratio = lower_shadow / total_range
        else:
            body_ratio = 0.0
            upper_shadow_ratio = 0.0
            lower_shadow_ratio = 0.0
        
        return {
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'body': body,
            'total_range': total_range,
            'upper_shadow': upper_shadow,
            'lower_shadow': lower_shadow,
            'is_bullish': is_bullish,
            'is_bearish': is_bearish,
            'body_ratio': body_ratio,
            'upper_shadow_ratio': upper_shadow_ratio,
            'lower_shadow_ratio': lower_shadow_ratio,
            'is_doji': body_ratio < 0.1
        }
    
    def _calculate_avg_body(self, df: pl.DataFrame, periods: int = 10) -> float:
        """计算平均实体大小"""
        if len(df) < periods:
            periods = len(df)
        
        bodies = df.select([
            (pl.col('close') - pl.col('open')).abs().alias('body')
        ]).tail(periods)
        
        return bodies['body'].mean()
    
    def _calculate_avg_range(self, df: pl.DataFrame, periods: int = 10) -> float:
        """计算平均波动范围"""
        if len(df) < periods:
            periods = len(df)
        
        ranges = df.select([
            (pl.col('high') - pl.col('low')).alias('range')
        ]).tail(periods)
        
        return ranges['range'].mean()


class PatternRegistry:
    """形态注册表"""
    
    _patterns: Dict[str, type] = {}
    
    @classmethod
    def register(cls, pattern_class: type) -> type:
        """注册形态类"""
        cls._patterns[pattern_class.__name__] = pattern_class
        return pattern_class
    
    @classmethod
    def get_pattern(cls, name: str, params: Optional[Dict] = None) -> Optional[BasePattern]:
        """获取形态实例"""
        pattern_class = cls._patterns.get(name)
        if pattern_class:
            instance = pattern_class(params)
            return instance
        return None
    
    @classmethod
    def list_patterns(cls) -> List[str]:
        """列出所有已注册的形态"""
        return list(cls._patterns.keys())
    
    @classmethod
    def get_all_patterns(cls) -> Dict[str, type]:
        """获取所有形态类"""
        return cls._patterns.copy()
