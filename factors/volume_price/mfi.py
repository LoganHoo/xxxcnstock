"""
MFI 资金流量因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("mfi")
class MfiFactor(BaseFactor):
    """MFI 资金流量因子"""
    
    def __init__(self, name: str = "mfi", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "MFI资金流量因子")
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("tp")
        ])
        
        df = df.with_columns([
            (pl.col("tp") * pl.col("volume")).alias("mf")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("tp") > pl.col("tp").shift(1))
            .then(pl.col("mf")).otherwise(0.0).alias("pmf"),
            pl.when(pl.col("tp") < pl.col("tp").shift(1))
            .then(pl.col("mf")).otherwise(0.0).alias("nmf"),
        ])
        
        df = df.with_columns([
            pl.col("pmf").rolling_sum(self.period).alias("pmf_sum"),
            pl.col("nmf").rolling_sum(self.period).alias("nmf_sum"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("nmf_sum") == 0)
            .then(100.0)
            .otherwise(100 - 100 / (1 + pl.col("pmf_sum") / pl.col("nmf_sum"))).alias("mfi")
        ])
        
        latest = df.tail(1)
        mfi = latest["mfi"].item()
        
        if mfi is None:
            score = 50.0
        elif mfi < 20:
            score = 90.0
        elif mfi < 40:
            score = 70.0
        elif mfi < 60:
            score = 50.0
        elif mfi < 80:
            score = 40.0
        else:
            score = 30.0
        
        return data.with_columns([
            pl.lit(mfi).alias("mfi"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
