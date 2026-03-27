"""
EMV 简易波动因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("emv")
class EmvFactor(BaseFactor):
    """EMV 简易波动因子"""
    
    def __init__(self, name: str = "emv", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "EMV简易波动因子")
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            ((pl.col("high") + pl.col("low")) / 2).alias("mid"),
            ((pl.col("high") - pl.col("low"))).alias("br"),
        ])
        
        df = df.with_columns([
            (pl.col("mid") - pl.col("mid").shift(1)).alias("mid_change")
        ])
        
        df = df.with_columns([
            pl.when((pl.col("br") == 0) | (pl.col("volume") == 0))
            .then(0.0)
            .otherwise(pl.col("mid_change") / (pl.col("br") / pl.col("volume") * 1000000)).alias("emv_raw")
        ])
        
        df = df.with_columns([
            pl.col("emv_raw").rolling_mean(self.period).alias("emv")
        ])
        
        latest = df.tail(1)
        emv = latest["emv"].item()
        
        if emv is None:
            score = 50.0
        elif emv > 0:
            score = 70.0
        else:
            score = 40.0
        
        return data.with_columns([
            pl.lit(emv).alias("emv"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
