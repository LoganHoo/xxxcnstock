"""
市场情绪因子
基于成交量和价格波动计算市场情绪
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("market_sentiment")
class MarketSentimentFactor(BaseFactor):
    """市场情绪因子"""
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "market_sentiment",
            category=category or "market",
            params=params or {"vol_period": 20, "price_period": 10},
            description=description or "市场情绪指标"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        vol_period = self.params.get("vol_period", 20)
        price_period = self.params.get("price_period", 10)
        
        # 计算成交量变化率
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=vol_period).alias("vol_ma"),
            (pl.col("volume") / pl.col("volume").shift(1) - 1).alias("vol_change")
        ])
        
        # 计算价格波动
        data = data.with_columns([
            ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("price_volatility"),
            (pl.col("close") / pl.col("open") - 1).alias("daily_return")
        ])
        
        # 计算情绪指标
        data = data.with_columns([
            (pl.col("vol_change") * pl.col("daily_return")).alias("sentiment_raw")
        ])
        
        # 移动平均平滑
        data = data.with_columns([
            pl.col("sentiment_raw").rolling_mean(window_size=price_period).alias(self.get_factor_column_name())
        ])
        
        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0)
        ])
        
        return data
