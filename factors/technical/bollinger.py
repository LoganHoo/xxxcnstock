"""
布林带因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("bollinger")
class BollingerFactor(BaseFactor):
    """布林带因子"""
    
    def __init__(self, name: str = "bollinger", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "布林带因子")
        
        self.period = self.params.get("period", 20)
        self.std_dev = self.params.get("std_dev", 2)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算布林带因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("close").rolling_mean(self.period).alias("boll_mid"),
            pl.col("close").rolling_std(self.period).alias("boll_std"),
        ])
        
        df = df.with_columns([
            (pl.col("boll_mid") + self.std_dev * pl.col("boll_std")).alias("boll_upper"),
            (pl.col("boll_mid") - self.std_dev * pl.col("boll_std")).alias("boll_lower"),
        ])
        
        latest = df.tail(1)
        close = latest["close"].item()
        upper = latest["boll_upper"].item()
        lower = latest["boll_lower"].item()
        mid = latest["boll_mid"].item()
        
        if upper is None or lower is None or mid is None:
            score = 50.0
            boll_position = 0.5
        else:
            boll_width = upper - lower
            if boll_width > 0:
                boll_position = (close - lower) / boll_width
            else:
                boll_position = 0.5
            
            if boll_position < 0:
                score = 90.0
            elif boll_position < 0.2:
                score = 80.0
            elif boll_position < 0.4:
                score = 60.0
            elif boll_position < 0.6:
                score = 50.0
            elif boll_position < 0.8:
                score = 40.0
            elif boll_position < 1.0:
                score = 30.0
            else:
                score = 20.0
        
        return data.with_columns([
            pl.lit(upper).alias("boll_upper"),
            pl.lit(mid).alias("boll_mid"),
            pl.lit(lower).alias("boll_lower"),
            pl.lit(boll_position).alias("boll_position"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
