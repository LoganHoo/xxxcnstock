"""
PSY 心理线因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("psy")
class PsyFactor(BaseFactor):
    """PSY 心理线因子"""
    
    def __init__(self, name: str = "psy", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "PSY心理线因子")
        self.period = self.params.get("period", 12)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.when(pl.col("close") > pl.col("close").shift(1)).then(1.0).otherwise(0.0).alias("up")
        ])
        
        df = df.with_columns([
            pl.col("up").rolling_sum(self.period).alias("up_count")
        ])
        
        df = df.with_columns([
            (pl.col("up_count") / self.period * 100).alias("psy")
        ])
        
        latest = df.tail(1)
        psy = latest["psy"].item()
        
        if psy is None:
            score = 50.0
        elif psy < 25:
            score = 90.0
        elif psy < 40:
            score = 70.0
        elif psy < 60:
            score = 50.0
        elif psy < 75:
            score = 40.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(psy).alias("psy"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
