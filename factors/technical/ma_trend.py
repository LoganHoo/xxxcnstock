"""
均线趋势因子
判断多头排列、空头排列
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("ma_trend")
class MaTrendFactor(BaseFactor):
    """均线趋势因子"""
    
    def __init__(self, name: str = "ma_trend", category: str = "technical", 
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "均线趋势因子")
        
        self.short_period = self.params.get("short_period", 5)
        self.mid_period = self.params.get("mid_period", 10)
        self.long_period = self.params.get("long_period", 20)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算均线趋势得分"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("close").rolling_mean(self.short_period).alias("ma_short"),
            pl.col("close").rolling_mean(self.mid_period).alias("ma_mid"),
            pl.col("close").rolling_mean(self.long_period).alias("ma_long"),
        ])
        
        latest = df.tail(1)
        
        close = latest["close"].item()
        ma_short = latest["ma_short"].item()
        ma_mid = latest["ma_mid"].item()
        ma_long = latest["ma_long"].item()
        
        if ma_short is None or ma_mid is None or ma_long is None:
            score = 50.0
        elif close > ma_short > ma_mid > ma_long:
            score = 100.0
        elif close > ma_short > ma_mid:
            score = 80.0
        elif close > ma_short:
            score = 60.0
        elif close > ma_long:
            score = 40.0
        elif close < ma_short < ma_mid < ma_long:
            score = 0.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(ma_short).alias("ma_short"),
            pl.lit(ma_mid).alias("ma_mid"),
            pl.lit(ma_long).alias("ma_long"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
