"""
位置因子
MA5_Bias：(当前价 - 5日均线) / 5日均线
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("ma5_bias")
class MA5BiasFactor(BaseFactor):
    """MA5_Bias因子
    (当前价 - 5日均线) / 5日均线
    衡量短线超买/超卖程度
    """
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "ma5_bias",
            category=category or "technical",
            params=params or {},
            description=description or "(当前价 - 5日均线) / 5日均线"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        # 计算5日均线
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=5).alias("ma5")
        ])
        
        # 计算偏差率
        data = data.with_columns([
            ((pl.col("close") - pl.col("ma5")) / pl.col("ma5")).alias(self.get_factor_column_name())
        ])
        
        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0.0)
        ])
        
        return data
