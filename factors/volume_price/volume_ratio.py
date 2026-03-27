"""
量比因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("volume_ratio")
class VolumeRatioFactor(BaseFactor):
    """量比因子"""
    
    def __init__(self, name: str = "volume_ratio", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "量比因子")
        
        self.period = self.params.get("period", 5)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算量比得分"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("volume").rolling_mean(self.period).alias("vol_ma"),
        ])
        
        latest = df.tail(1)
        volume = latest["volume"].item()
        vol_ma = latest["vol_ma"].item()
        
        if vol_ma is None or vol_ma == 0:
            ratio = 1.0
        else:
            ratio = volume / vol_ma
        
        if ratio > 3:
            score = 100.0
        elif ratio > 2:
            score = 80.0
        elif ratio > 1.5:
            score = 60.0
        elif ratio > 0.8:
            score = 50.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(ratio).alias("volume_ratio"),
            pl.lit(score).alias(self.get_factor_column_name()),
        ])
