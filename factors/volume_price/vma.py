"""
VMA 量均线因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("vma")
class VmaFactor(BaseFactor):
    """VMA 量均线因子"""
    
    def __init__(self, name: str = "vma", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "VMA量均线因子")
        self.short = self.params.get("short", 5)
        self.long = self.params.get("long", 20)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("volume").rolling_mean(self.short).alias("vma_short"),
            pl.col("volume").rolling_mean(self.long).alias("vma_long"),
        ])
        
        latest = df.tail(1)
        vma_short = latest["vma_short"].item()
        vma_long = latest["vma_long"].item()
        
        if vma_long is None or vma_long == 0:
            score = 50.0
            vma_ratio = 1.0
        else:
            vma_ratio = vma_short / vma_long
            
            if vma_ratio > 2:
                score = 90.0
            elif vma_ratio > 1.5:
                score = 75.0
            elif vma_ratio > 1.0:
                score = 60.0
            elif vma_ratio > 0.8:
                score = 50.0
            else:
                score = 40.0
        
        return data.with_columns([
            pl.lit(vma_short).alias("vma_short"),
            pl.lit(vma_long).alias("vma_long"),
            pl.lit(vma_ratio).alias("vma_ratio"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
