"""
MACD 因子
金叉死叉信号
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("macd")
class MacdFactor(BaseFactor):
    """MACD 因子"""
    
    def __init__(self, name: str = "macd", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "MACD因子")
        
        self.fast = self.params.get("fast", 12)
        self.slow = self.params.get("slow", 26)
        self.signal = self.params.get("signal", 9)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 MACD 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("close").ewm_mean(span=self.fast).alias("ema_fast"),
            pl.col("close").ewm_mean(span=self.slow).alias("ema_slow"),
        ])
        
        df = df.with_columns([
            (pl.col("ema_fast") - pl.col("ema_slow")).alias("dif"),
        ])
        
        df = df.with_columns([
            pl.col("dif").ewm_mean(span=self.signal).alias("dea"),
        ])
        
        df = df.with_columns([
            (pl.col("dif") - pl.col("dea")).alias("macd"),
        ])
        
        recent = df.tail(2)
        
        if len(recent) < 2:
            return data.with_columns([
                pl.lit(50.0).alias(self.get_factor_column_name())
            ])
        
        macd_today = recent["macd"].tail(1).item()
        macd_yest = recent["macd"].head(1).item()
        dif_today = recent["dif"].tail(1).item()
        
        if macd_today > 0 and macd_yest <= 0:
            score = 100.0
        elif macd_today > 0 and dif_today > 0:
            score = 80.0
        elif macd_today > 0:
            score = 60.0
        elif macd_today < 0 and macd_yest >= 0:
            score = 20.0
        elif macd_today < 0:
            score = 40.0
        else:
            score = 50.0
        
        return data.with_columns([
            pl.lit(dif_today).alias("dif"),
            pl.lit(recent["dea"].tail(1).item()).alias("dea"),
            pl.lit(macd_today).alias("macd"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
