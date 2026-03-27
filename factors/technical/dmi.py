"""
DMI 趋势强度因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("dmi")
class DmiFactor(BaseFactor):
    """DMI 趋势强度因子"""
    
    def __init__(self, name: str = "dmi", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "DMI趋势强度因子")
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        df = data.sort("trade_date")
        
        df = df.with_columns([
            (pl.col("high") - pl.col("high").shift(1)).alias("up_move"),
            (pl.col("low").shift(1) - pl.col("low")).alias("down_move"),
        ])
        
        df = df.with_columns([
            pl.when((pl.col("up_move") > pl.col("down_move")) & (pl.col("up_move") > 0))
            .then(pl.col("up_move")).otherwise(0.0).alias("+dm"),
            pl.when((pl.col("down_move") > pl.col("up_move")) & (pl.col("down_move") > 0))
            .then(pl.col("down_move")).otherwise(0.0).alias("-dm"),
        ])
        
        df = df.with_columns([
            pl.col("high") - pl.col("low"),
        ])
        tr = (pl.col("high") - pl.col("low")).abs()
        tr2 = (pl.col("high") - pl.col("close").shift(1)).abs()
        tr3 = (pl.col("low") - pl.col("close").shift(1)).abs()
        df = df.with_columns([
            pl.max_horizontal([tr, tr2, tr3]).alias("tr")
        ])
        
        df = df.with_columns([
            pl.col("tr").rolling_sum(self.period).alias("tr_sum"),
            pl.col("+dm").rolling_sum(self.period).alias("+dm_sum"),
            pl.col("-dm").rolling_sum(self.period).alias("-dm_sum"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("tr_sum") == 0)
            .then(0.0)
            .otherwise(pl.col("+dm_sum") / pl.col("tr_sum") * 100).alias("+di"),
            pl.when(pl.col("tr_sum") == 0)
            .then(0.0)
            .otherwise(pl.col("-dm_sum") / pl.col("tr_sum") * 100).alias("-di"),
        ])
        
        df = df.with_columns([
            pl.when((pl.col("+di") + pl.col("-di")) == 0)
            .then(0.0)
            .otherwise((pl.col("+di") - pl.col("-di")).abs() / (pl.col("+di") + pl.col("-di")) * 100).alias("adx")
        ])
        
        latest = df.tail(1)
        adx = latest["adx"].item()
        pdi = latest["+di"].item()
        mdi = latest["-di"].item()
        
        if adx is None:
            score = 50.0
        elif adx > 60:
            score = 90.0
        elif adx > 40:
            score = 70.0
        elif adx > 25:
            score = 60.0
        elif adx > 20:
            score = 50.0
        else:
            score = 40.0
        
        return data.with_columns([
            pl.lit(adx).alias("adx"),
            pl.lit(pdi).alias("pdi"),
            pl.lit(mdi).alias("mdi"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
