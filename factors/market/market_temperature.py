"""
市场温度因子
综合多种市场指标计算市场温度
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("market_temperature")
class MarketTemperatureFactor(BaseFactor):
    """市场温度因子"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "market_temperature",
            category=category or "market",
            params=params or {"period": 20, "high_low_period": 52},
            description=description or "市场温度指标"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        period = self.params.get("period", 20)
        high_low_period = self.params.get("high_low_period", 52)
        
        # 计算52周新高/新低
        data = data.with_columns([
            pl.col("close").rolling_max(window_size=high_low_period).alias("52w_high"),
            pl.col("close").rolling_min(window_size=high_low_period).alias("52w_low")
        ])
        
        data = data.with_columns([
            (pl.col("close") >= pl.col("52w_high")).cast(pl.Int64).alias("new_high"),
            (pl.col("close") <= pl.col("52w_low")).cast(pl.Int64).alias("new_low")
        ])
        
        # 按日期分组计算
        daily_stats = data.group_by("trade_date").agg([
            pl.sum("new_high").alias("total_new_high"),
            pl.sum("new_low").alias("total_new_low"),
            pl.count("code").alias("total_stocks")
        ])
        
        # 计算温度指标
        daily_stats = daily_stats.with_columns([
            (pl.col("total_new_high") - pl.col("total_new_low")).alias("high_low_diff")
        ])
        daily_stats = daily_stats.with_columns([
            (pl.col("high_low_diff") / pl.col("total_stocks")).alias("temperature_raw")
        ])
        
        # 平滑处理
        daily_stats = daily_stats.with_columns([
            pl.col("temperature_raw").rolling_mean(window_size=period).alias(self.get_factor_column_name())
        ])
        
        # 合并回原数据
        data = data.join(daily_stats[["trade_date", self.get_factor_column_name()]], on="trade_date", how="left")
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0)
        ])
        
        return data
