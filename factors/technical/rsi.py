"""
RSI 因子
相对强弱指标
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("rsi")
class RsiFactor(BaseFactor):
    """RSI 因子"""
    
    def __init__(self, name: str = "rsi", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "RSI因子")
        
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 RSI 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("close").diff().alias("price_change"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("price_change") > 0)
              .then(pl.col("price_change"))
              .otherwise(0.0).alias("gain"),
            pl.when(pl.col("price_change") < 0)
              .then(-pl.col("price_change"))
              .otherwise(0.0).alias("loss"),
        ])
        
        df = df.with_columns([
            pl.col("gain").rolling_mean(self.period).alias("avg_gain"),
            pl.col("loss").rolling_mean(self.period).alias("avg_loss"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("avg_loss") == 0)
              .then(100.0)
              .otherwise(100 - 100 / (1 + pl.col("avg_gain") / pl.col("avg_loss")))
              .alias("rsi")
        ])
        
        latest = df.tail(1)
        rsi_value = latest["rsi"].item()
        
        if rsi_value is None:
            score = 50.0
        elif rsi_value < 30:
            score = 80.0
        elif rsi_value < 50:
            score = 60.0
        elif rsi_value < 70:
            score = 50.0
        elif rsi_value < 80:
            score = 30.0
        else:
            score = 20.0
        
        return data.with_columns([
            pl.lit(rsi_value).alias("rsi"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
