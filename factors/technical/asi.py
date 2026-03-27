"""
ASI 振动升降因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("asi")
class AsiFactor(BaseFactor):
    """ASI 振动升降因子"""
    
    def __init__(self, name: str = "asi", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "ASI振动升降因子")
        self.period = self.params.get("period", 5)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            (pl.col("high") - pl.col("close").shift(1)).abs().alias("a"),
            (pl.col("low") - pl.col("close").shift(1)).abs().alias("b"),
            (pl.col("high") - pl.col("low")).alias("c"),
            (pl.col("close").shift(1) - pl.col("open").shift(1)).abs().alias("d"),
        ])
        
        df = df.with_columns([
            pl.max_horizontal(["a", "b"]).alias("max_ab")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("max_ab") == pl.col("a"))
            .then(pl.col("a") + pl.col("b") / 2 + pl.col("d") / 4)
            .otherwise(pl.col("b") + pl.col("a") / 2 + pl.col("d") / 4).alias("r")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("r") == 0)
            .then(0.0)
            .otherwise(
                (pl.col("close") - pl.col("close").shift(1) +
                 (pl.col("close") - pl.col("open")) / 2 +
                 pl.col("close").shift(1) - pl.col("open").shift(1)) /
                pl.col("r") * 50
            ).alias("si")
        ])
        
        df = df.with_columns([
            pl.col("si").rolling_sum(self.period).alias("asi")
        ])
        
        latest = df.tail(1)
        asi = latest["asi"].item()
        
        if asi is None:
            score = 50.0
        elif asi > 50:
            score = 75.0
        elif asi > 0:
            score = 60.0
        elif asi > -50:
            score = 50.0
        else:
            score = 40.0
        
        return data.with_columns([
            pl.lit(asi).alias("asi"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
