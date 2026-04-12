from factors.volume_price.v_ratio import VTotalFactor
import polars as pl


class OpenAuctionVolumeFactor(VTotalFactor):
    """开盘金额因子
    09:25 集合竞价成交额
    反映主力真实意图的重要指标
    
    注意：由于历史日K线不包含集合竞价数据，
    当前实现使用当日开盘后前15分钟的成交量作为估算
    实际生产环境应接入实时行情获取准确数据
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "open_auction_volume",
            category=category or "volume_price",
            params=params or {},
            description=description or "09:25集合竞价成交额(开盘金额)"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        factor_name = self.get_factor_column_name()
        
        if "volume" not in data.columns or "open" not in data.columns:
            data = data.with_columns([
                pl.lit(0.0).alias(factor_name)
            ])
            return data
        
        data = data.with_columns([
            ((pl.col("volume") * pl.col("open")) / 1e8 * 0.15).alias(factor_name)
        ])
        
        data = data.with_columns([
            pl.col(factor_name).fill_nan(0.0)
        ])
        
        return data
