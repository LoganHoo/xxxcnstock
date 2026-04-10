"""
大盘宽度因子
衡量市场涨跌家数的对比
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("market_breadth")
class MarketBreadthFactor(BaseFactor):
    """大盘宽度因子"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "market_breadth",
            category=category or "market",
            params=params or {"period": 10},
            description=description or "市场涨跌家数对比"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        period = self.params.get("period", 10)
        
        # 计算每日涨跌家数
        data = data.with_columns([
            (pl.col("close") > pl.col("open")).cast(pl.Int64).alias("advancing"),
            (pl.col("close") < pl.col("open")).cast(pl.Int64).alias("declining")
        ])
        
        # 按日期分组计算涨跌家数
        daily_stats = data.group_by("trade_date").agg([
            pl.sum("advancing").alias("total_advancing"),
            pl.sum("declining").alias("total_declining"),
            pl.count("code").alias("total_stocks")
        ])
        
        # 计算宽度指标
        daily_stats = daily_stats.with_columns([
            (pl.col("total_advancing") - pl.col("total_declining")).alias("breadth_diff"),
            (pl.col("total_advancing") / pl.col("total_stocks")).alias("advancing_ratio"),
        ])
        daily_stats = daily_stats.with_columns([
            (pl.col("breadth_diff") / pl.col("total_stocks")).alias("breadth_indicator")
        ])
        
        # 计算移动平均
        daily_stats = daily_stats.with_columns([
            pl.col("breadth_indicator").rolling_mean(window_size=period).alias(self.get_factor_column_name())
        ])
        
        # 合并回原数据
        data = data.join(daily_stats[["trade_date", self.get_factor_column_name()]], on="trade_date", how="left")
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0)
        ])
        
        return data
