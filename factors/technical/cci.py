"""
CCI 顺势指标因子
"""
import polars as pl
from typing import Dict, Any
from core.factor_library import BaseFactor, register_factor


@register_factor("cci")
class CciFactor(BaseFactor):
    """CCI 顺势指标因子"""
    
    def __init__(self, name: str = "cci", category: str = "technical",
                 params: Dict[str, Any] = None, description: str = ""):
        super().__init__(name, category, params, description or "CCI顺势指标因子")
        
        self.period = self.params.get("period", 14)
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算 CCI 因子"""
        df = data.sort("trade_date")
        
        df = df.with_columns([
            ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("tp")
        ])
        
        df = df.with_columns([
            pl.col("tp").rolling_mean(self.period).alias("tp_sma")
        ])
        
        df = df.with_columns([
            (pl.col("tp") - pl.col("tp_sma")).abs().rolling_mean(self.period).alias("md")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("md") == 0)
            .then(0.0)
            .otherwise((pl.col("tp") - pl.col("tp_sma")) / (0.015 * pl.col("md"))).alias("cci")
        ])
        
        latest = df.tail(1)
        cci_value = latest["cci"].item()
        
        if cci_value is None:
            score = 50.0
        elif cci_value < -200:
            score = 90.0
        elif cci_value < -100:
            score = 80.0
        elif cci_value < 0:
            score = 60.0
        elif cci_value < 100:
            score = 50.0
        elif cci_value < 200:
            score = 30.0
        else:
            score = 20.0
        
        return data.with_columns([
            pl.lit(cci_value).alias("cci"),
            pl.lit(score).alias(self.get_factor_column_name())
        ])
