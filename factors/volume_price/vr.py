"""
VR 容量比率因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("vr")
class VrFactor(BaseFactor):
    """VR 容量比率因子"""
    
    def __init__(self, name: str = "vr", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "VR容量比率因子")
        self.period = self.params.get("period", 26)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.when(pl.col("close") > pl.col("close").shift(1))
            .then(pl.col("volume")).otherwise(0.0).alias("up_vol"),
            pl.when(pl.col("close") < pl.col("close").shift(1))
            .then(pl.col("volume")).otherwise(0.0).alias("down_vol"),
            pl.when(pl.col("close") == pl.col("close").shift(1))
            .then(pl.col("volume")).otherwise(0.0).alias("eq_vol"),
        ])
        
        df = df.with_columns([
            pl.col("up_vol").rolling_sum(self.period).alias("up_sum"),
            pl.col("down_vol").rolling_sum(self.period).alias("down_sum"),
            pl.col("eq_vol").rolling_sum(self.period).alias("eq_sum"),
        ])
        
        df = df.with_columns([
            pl.when((pl.col("down_sum") + pl.col("eq_sum") / 2) == 0)
            .then(100.0)
            .otherwise((pl.col("up_sum") + pl.col("eq_sum") / 2) / 
                       (pl.col("down_sum") + pl.col("eq_sum") / 2) * 100).alias("vr")
        ])
        
        latest = df.tail(1)
        vr = latest["vr"].item()
        
        if vr is None:
            score = 50.0
        elif vr < 40:
            score = 90.0
        elif vr < 70:
            score = 70.0
        elif vr < 150:
            score = 50.0
        elif vr < 250:
            score = 40.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(vr).alias("vr"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
