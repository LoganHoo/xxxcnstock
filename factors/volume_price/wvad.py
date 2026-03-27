"""
WVAD 威廉变异离散量因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("wvad")
class WvadFactor(BaseFactor):
    """WVAD 威廉变异离散量因子"""
    
    def __init__(self, name: str = "wvad", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "WVAD威廉变异离散量因子")
        self.period = self.params.get("period", 24)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.when(pl.col("high") == pl.col("low"))
            .then(0.0)
            .otherwise((pl.col("close") - pl.col("open")) / 
                       (pl.col("high") - pl.col("low")) * pl.col("volume")).alias("wvad_daily")
        ])
        
        df = df.with_columns([
            pl.col("wvad_daily").rolling_sum(self.period).alias("wvad")
        ])
        
        latest = df.tail(1)
        wvad = latest["wvad"].item()
        
        if wvad is None:
            score = 50.0
        elif wvad > 0:
            score = 70.0
        else:
            score = 40.0
        
        return data.with_columns([
            pl.lit(wvad).alias("wvad"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
