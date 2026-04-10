"""
大盘趋势因子
计算大盘指数的趋势强度
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("market_trend")
class MarketTrendFactor(BaseFactor):
    """大盘趋势因子"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "market_trend",
            category=category or "market",
            params=params or {"period": 20},
            description=description or "大盘指数趋势强度"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        period = self.params.get("period", 20)
        
        # 计算大盘指数的MA和斜率
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=period).alias(f"ma{period}"),
            pl.col("close").rolling_std(window_size=period).alias(f"std{period}")
        ])
        
        # 计算趋势强度（MA斜率 / 波动率）
        data = data.with_columns([
            (pl.col(f"ma{period}") - pl.col(f"ma{period}").shift(1)).alias("ma_slope"),
            (pl.col(f"ma{period}") / pl.col(f"std{period}")).alias("trend_strength")
        ])
        
        # 标准化为因子值
        data = data.with_columns([
            pl.col("trend_strength").fill_nan(0).alias(self.get_factor_column_name())
        ])
        
        return data
