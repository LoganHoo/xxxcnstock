"""
MTM 动量因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("mtm")
class MtmFactor(BaseFactor):
    """MTM 动量因子"""
    
    def __init__(self, name: str = "mtm", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "MTM动量因子")
        self.period = self.params.get("period", 12)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            (pl.col("close") - pl.col("close").shift(self.period)).alias("mtm")
        ])
        
        latest = df.tail(1)
        mtm = latest["mtm"].item()
        close = latest["close"].item()
        
        if mtm is None or close is None or close == 0:
            score = 50.0
        else:
            mtm_pct = mtm / close * 100
            if mtm_pct < -10:
                score = 90.0
            elif mtm_pct < -5:
                score = 70.0
            elif mtm_pct < 0:
                score = 55.0
            elif mtm_pct < 5:
                score = 50.0
            elif mtm_pct < 10:
                score = 40.0
            else:
                score = 30.0
        
        return data.with_columns([
            pl.lit(mtm).alias("mtm"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
