"""
KDJ 随机指标因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("kdj")
class KdjFactor(BaseFactor):
    """KDJ 随机指标因子"""
    
    def __init__(self, name: str = "kdj", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "KDJ随机指标因子")
        
        self.n = self.params.get("n", 9)
        self.m1 = self.params.get("m1", 3)
        self.m2 = self.params.get("m2", 3)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 KDJ 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            pl.col("high").rolling_max(self.n).alias("highest"),
            pl.col("low").rolling_min(self.n).alias("lowest"),
        ])
        
        df = df.with_columns([
            pl.when(pl.col("highest") == pl.col("lowest"))
            .then(50.0)
            .otherwise((pl.col("close") - pl.col("lowest")) / 
                       (pl.col("highest") - pl.col("lowest")) * 100).alias("rsv")
        ])
        
        df = df.with_columns([
            pl.col("rsv").ewm_mean(span=self.m1).alias("k"),
        ])
        
        df = df.with_columns([
            pl.col("k").ewm_mean(span=self.m2).alias("d"),
        ])
        
        df = df.with_columns([
            (3 * pl.col("k") - 2 * pl.col("d")).alias("j"),
        ])
        
        latest = df.tail(1)
        k_value = latest["k"].item()
        d_value = latest["d"].item()
        j_value = latest["j"].item()
        
        if k_value is None or d_value is None or j_value is None:
            score = 50.0
        elif j_value < 0:
            score = 90.0
        elif j_value < 20:
            score = 80.0
        elif j_value < 50:
            score = 60.0
        elif j_value < 80:
            score = 40.0
        elif j_value < 100:
            score = 20.0
        else:
            score = 10.0
        
        return data.with_columns([
            pl.lit(k_value).alias("kdj_k"),
            pl.lit(d_value).alias("kdj_d"),
            pl.lit(j_value).alias("kdj_j"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
