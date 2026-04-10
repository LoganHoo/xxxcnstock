"""
量能因子
包含V_ratio10和V_total两个因子
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("v_ratio10")
class VRatio10Factor(BaseFactor):
    """V_ratio10因子
    (当日09:30-10:00成交量) / (前一日同期成交量)
    识别早盘动能是否爆发
    """
    
    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "v_ratio10",
            category=category or "volume_price",
            params=params or {},
            description=description or "早盘09:30-10:00成交量与前一日同期比值"
        )
    
    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        # 这里需要分时数据，暂时使用日K数据模拟
        # 实际实现需要接入实时分时数据
        
        # 计算日成交量的5日平均作为替代
        data = data.with_columns([
            pl.col("volume").rolling_mean(window_size=5).alias("volume_ma5"),
            pl.col("volume").shift(1).alias("prev_volume"),
        ])
        data = data.with_columns([
            pl.when(pl.col("prev_volume").is_null() | (pl.col("prev_volume") == 0))
            .then(1.0)
            .otherwise(pl.col("volume") / pl.col("prev_volume"))
            .alias(self.get_factor_column_name())
        ])
        
        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_null(1.0).fill_nan(1.0)
        ])
        
        return data


@register_factor("v_total")
class VTotalFactor(BaseFactor):
    """V_total因子
    全市场实时总成交金额（万亿）
    判断市场整体承载力
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "v_total",
            category=category or "market",
            params=params or {},
            description=description or "全市场实时总成交金额（万亿）"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        # 计算每日全市场总成交额
        # 公式: Σ(volume * close) / 1e8 = 亿（转换为万亿需/10000）
        daily_total = data.group_by("trade_date").agg([
            ((pl.col("volume") * pl.col("close")).sum() / 1e8).alias(self.get_factor_column_name())
        ])

        # 合并回原数据，每个股票都有对应日期的总成交额
        data = data.join(daily_total, on="trade_date", how="left")

        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0.0)
        ])

        return data
