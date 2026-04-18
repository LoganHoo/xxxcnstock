"""
筹码因子
Cost_Peak：筹码分布图中最大的压力/支撑密集峰位
筹码峰位计算逻辑：
1. 对过去N日的价格和成交量进行分析
2. 计算每个价格区间的持仓成本（成交量加权）
3. 找到持仓成本最集中的价格区间作为筹码峰值
"""
import polars as pl
import numpy as np
from core.factor_library import BaseFactor, register_factor


@register_factor("cost_peak")
class CostPeakFactor(BaseFactor):
    """Cost_Peak因子
    筹码分布图中最大的压力/支撑密集峰位
    确定大趋势的基石

    计算方法：
    1. 使用过去window日的收盘价作为筹码分布区间
    2. 用成交量作为权重，计算加权平均成本
    3. 找到最密集的成本区间（密度峰值）
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "cost_peak",
            category=category or "market",
            params=params or {"window": 20, "bins": 50},
            description=description or "筹码分布最大密集峰位"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        window = self.params.get("window", 20)
        bins = self.params.get("bins", 50)

        def compute_cost_peak(price_series, volume_series):
            """计算单个股票的筹码峰值"""
            if len(price_series) < 5:
                return float(price_series[-1]) if len(price_series) > 0 else 0.0

            prices = np.array(price_series)
            volumes = np.array(volume_series)

            min_price = prices.min()
            max_price = prices.max()

            if max_price == min_price:
                return float(max_price)

            bin_width = (max_price - min_price) / bins
            if bin_width == 0:
                return float(min_price)

            volume_density = np.zeros(bins)
            for i in range(len(prices)):
                bin_idx = int((prices[i] - min_price) / bin_width)
                bin_idx = min(max(bin_idx, 0), bins - 1)
                volume_density[bin_idx] += volumes[i]

            peak_bin = np.argmax(volume_density)
            peak_price = min_price + (peak_bin + 0.5) * bin_width

            return float(peak_price)

        data = data.sort(["code", "trade_date"])

        peak_prices = []
        for col in ["code", "trade_date", "close", "volume"]:
            peak_prices.append(data[col].to_list())

        codes = data["code"].unique().to_list()
        trade_dates = data["trade_date"].unique().to_list()

        result_data = []
        for code in codes:
            stock_data = data.filter(pl.col("code") == code).sort("trade_date")
            prices = stock_data["close"].to_list()
            volumes = stock_data["volume"].to_list()
            dates = stock_data["trade_date"].to_list()

            for i, date in enumerate(dates):
                if i < window - 1:
                    window_prices = prices[:i+1]
                    window_volumes = volumes[:i+1]
                else:
                    window_prices = prices[i-window+1:i+1]
                    window_volumes = volumes[i-window+1:i+1]

                peak = compute_cost_peak(window_prices, window_volumes)
                result_data.append({"code": code, "trade_date": date, self.get_factor_column_name(): peak})

        result_df = pl.DataFrame(result_data)
        # 确保code列类型一致（处理Categorical类型）
        if data['code'].dtype != result_df['code'].dtype:
            result_df = result_df.with_columns([
                pl.col('code').cast(data['code'].dtype)
            ])
        data = data.join(result_df, on=["code", "trade_date"], how="left")
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(pl.col("close"))
        ])

        return data
