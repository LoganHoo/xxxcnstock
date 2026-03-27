"""
ATR 波动率因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("atr")
class AtrFactor(BaseFactor):
    """ATR 波动率因子"""
    
    def __init__(self, name: str = "atr", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "ATR波动率因子")
        
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 ATR 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("high").shift(1).alias("prev_close")
        ])
        
        df = df.with_columns([
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("prev_close")).abs(),
                (pl.col("low") - pl.col("prev_close")).abs()
            ).alias("tr")
        ])
        
        df = df.with_columns([
            pl.col("tr").rolling_mean(self.period).alias("atr")
        ])
        
        latest = df.tail(1)
        atr = latest["atr"].item()
        close = latest["close"].item()
        
        if atr is None or close is None or close == 0:
            score = 50.0
            atr_ratio = 0
        else:
            atr_ratio = atr / close * 100
            
            if atr_ratio > 5:
                score = 20.0
            elif atr_ratio > 4:
                score = 30.0
            elif atr_ratio > 3:
                score = 40.0
            elif atr_ratio > 2:
                score = 60.0
            elif atr_ratio > 1:
                score = 80.0
            else:
                score = 70.0
        
        return data.with_columns([
            pl.lit(atr).alias("atr"),
            pl.lit(atr_ratio).alias("atr_ratio"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
