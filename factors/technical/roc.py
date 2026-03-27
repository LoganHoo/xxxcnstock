"""
ROC 变动率因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("roc")
class RocFactor(BaseFactor):
    """ROC 变动率因子"""
    
    def __init__(self, name: str = "roc", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "ROC变动率因子")
        self.period = self.params.get("period", 12)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.when(pl.col("close").shift(self.period) == 0)
            .then(0.0)
            .otherwise((pl.col("close") - pl.col("close").shift(self.period)) / 
                       pl.col("close").shift(self.period) * 100).alias("roc")
        ])
        
        latest = df.tail(1)
        roc = latest["roc"].item()
        
        if roc is None:
            score = 50.0
        elif roc < -15:
            score = 90.0
        elif roc < -5:
            score = 70.0
        elif roc < 5:
            score = 50.0
        elif roc < 15:
            score = 40.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(roc).alias("roc"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
