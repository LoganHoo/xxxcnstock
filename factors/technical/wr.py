"""
WR 威廉指标因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("wr")
class WrFactor(BaseFactor):
    """WR 威廉指标因子"""
    
    def __init__(self, name: str = "wr", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "WR威廉指标因子")
        
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 WR 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("high").rolling_max(self.period).alias("highest"),
            pl.col("low").rolling_min(self.period).alias("lowest"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("highest") == pl.col("lowest"))
            .then(-50.0)
            .otherwise((pl.col("highest") - pl.col("close")) / 
                       (pl.col("highest") - pl.col("lowest")) * -100).alias("wr")
        ])
        
        latest = df.tail(1)
        wr_value = latest["wr"].item()
        
        if wr_value is None:
            score = 50.0
        elif wr_value < -80:
            score = 90.0
        elif wr_value < -50:
            score = 70.0
        elif wr_value < -20:
            score = 50.0
        elif wr_value < 0:
            score = 30.0
        else:
            score = 20.0
        
        return data.with_columns([
            pl.lit(wr_value).alias("wr"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
