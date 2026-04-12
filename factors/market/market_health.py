"""
大盘荣枯线因子
计算MA5/MA10趋势判断，确认市场处于上涨/下跌/震荡状态
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("market_health")
class MarketHealthFactor(BaseFactor):
    """大盘荣枯线因子
    MA5 > MA10: 多头排列 (健康上涨)
    MA5 < MA10: 空头排列 (风险下跌)
    MA5 ≈ MA10: 震荡状态
    """
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "market_health",
            category=category or "market",
            params=params or {
                "ma5_period": 5,
                "ma10_period": 10,
                "ma5_threshold": 0.0,
                "ma10_threshold": 0.0
            },
            description=description or "大盘荣枯线(MA5/MA10趋势判断)"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        ma5_period = self.params.get("ma5_period", 5)
        ma10_period = self.params.get("ma10_period", 10)
        
        if "close" not in data.columns:
            data = data.with_columns([
                pl.lit(0.0).alias(self.get_factor_column_name())
            ])
            return data
        
        data = data.with_columns([
            pl.col("close").rolling_mean(window_size=ma5_period).alias(f"ma{ma5_period}"),
            pl.col("close").rolling_mean(window_size=ma10_period).alias(f"ma{ma10_period}")
        ])
        
        data = data.with_columns([
            (pl.col(f"ma{ma5_period}") - pl.col(f"ma{ma5_period}").shift(1)).alias("ma5_slope"),
            (pl.col(f"ma{ma10_period}") - pl.col(f"ma{ma10_period}").shift(1)).alias("ma10_slope")
        ])
        
        data = data.with_columns([
            (pl.col("ma5_slope") / pl.col("close") * 100).alias("ma5_angle"),
            (pl.col("ma10_slope") / pl.col("close") * 100).alias("ma10_angle")
        ])
        
        ma5_threshold = self.params.get("ma5_threshold", 0.0)
        ma10_threshold = self.params.get("ma10_threshold", 0.0)
        
        health_score = pl.when(
            (pl.col("ma5_angle") > ma5_threshold) & (pl.col("ma10_angle") > ma10_threshold)
        ).then(2.0)\
        .when(
            (pl.col("ma5_angle") > ma5_threshold) | (pl.col("ma10_angle") > ma10_threshold)
        ).then(1.0)\
        .when(
            (pl.col("ma5_angle") < -ma5_threshold) & (pl.col("ma10_angle") < -ma10_threshold)
        ).then(-1.0)\
        .otherwise(0.0)
        
        data = data.with_columns([
            health_score.alias(self.get_factor_column_name())
        ])
        
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0.0)
        ])
        
        return data
