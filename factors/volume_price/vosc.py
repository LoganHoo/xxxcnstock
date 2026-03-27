"""
VOSC 成交量震荡因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("vosc")
class VoscFactor(BaseFactor):
    """VOSC 成交量震荡因子"""
    
    def __init__(self, name: str = "vosc", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "VOSC成交量震荡因子")
        self.short = self.params.get("short", 12)
        self.long = self.params.get("long", 26)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("volume").rolling_mean(self.short).alias("vma_short"),
            pl.col("volume").rolling_mean(self.long).alias("vma_long"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("vma_long") == 0)
            .then(0.0)
            .otherwise((pl.col("vma_short") - pl.col("vma_long")) / pl.col("vma_long") * 100).alias("vosc")
        ])
        
        latest = df.tail(1)
        vosc = latest["vosc"].item()
        
        if vosc is None:
            score = 50.0
        elif vosc < -20:
            score = 90.0
        elif vosc < -10:
            score = 70.0
        elif vosc < 0:
            score = 55.0
        elif vosc < 10:
            score = 50.0
        elif vosc < 20:
            score = 40.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(vosc).alias("vosc"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
