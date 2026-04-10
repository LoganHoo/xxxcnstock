"""
换手率因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("turnover")
class TurnoverFactor(BaseFactor):
    """换手率因子"""
    
    def __init__(self, name: str = "turnover", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "换手率因子")
        
        self.period = self.params.get("period", 5)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算换手率因子"""
        df = data.sort("trade_date")
        
        if "turnover" in df.columns:
            df = df.with_columns([
                pl.col("turnover").alias("turnover_rate")
            ])
            latest = df.tail(1)
            turnover = latest["turnover_rate"].item()
        else:
            if "volume" in df.columns and "amount" in df.columns:
                df = df.with_columns([
                    pl.when(pl.col("amount") <= 0)
                    .then(0.0)
                    .otherwise(pl.col("volume") / pl.col("amount") * 100)
                    .alias("turnover_rate")
                ])
                latest = df.tail(1)
                turnover = latest["turnover_rate"].item()
            else:
                df = df.with_columns([
                    pl.col("volume").rolling_mean(self.period).alias("vol_ma")
                ])
                latest = df.tail(1)
                volume = latest["volume"].item()
                vol_ma = latest["vol_ma"].item()
                
                if vol_ma and vol_ma > 0:
                    turnover = volume / vol_ma * 5
                else:
                    turnover = 5.0
        
        if turnover is None:
            score = 50.0
        elif turnover > 20:
            score = 90.0
        elif turnover > 10:
            score = 80.0
        elif turnover > 5:
            score = 70.0
        elif turnover > 3:
            score = 60.0
        elif turnover > 1:
            score = 50.0
        else:
            score = 40.0
        
        return data.with_columns([
            pl.lit(turnover).alias("turnover_rate"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
