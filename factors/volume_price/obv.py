"""
OBV 能量潮因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("obv")
class ObvFactor(BaseFactor):
    """OBV 能量潮因子"""
    
    def __init__(self, name: str = "obv", category: str = "volume_price",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "OBV能量潮因子")
        
        self.period = self.params.get("period", 20)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 OBV 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.when(pl.col("close") > pl.col("close").shift(1))
            .then(pl.col("volume"))
            .when(pl.col("close") < pl.col("close").shift(1))
            .then(-pl.col("volume"))
            .otherwise(0.0)
            .alias("obv_change")
        ])
        
        df = df.with_columns([
            pl.col("obv_change").cum_sum().alias("obv")
        ])
        
        df = df.with_columns([
            pl.col("obv").rolling_mean(self.period).alias("obv_ma")
        ])
        
        latest = df.tail(1)
        obv = latest["obv"].item()
        obv_ma = latest["obv_ma"].item()
        
        if obv is None or obv_ma is None:
            score = 50.0
            obv_ratio = 1.0
        else:
            if obv_ma != 0:
                obv_ratio = obv / obv_ma
            else:
                obv_ratio = 1.0
            
            if obv_ratio > 1.5:
                score = 90.0
            elif obv_ratio > 1.2:
                score = 80.0
            elif obv_ratio > 1.0:
                score = 70.0
            elif obv_ratio > 0.8:
                score = 50.0
            elif obv_ratio > 0.5:
                score = 40.0
            else:
                score = 30.0
        
        return data.with_columns([
            pl.lit(obv).alias("obv"),
            pl.lit(obv_ma).alias("obv_ma"),
            pl.lit(obv_ratio).alias("obv_ratio"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
