"""
情绪因子
包含Limit_Up_Score和Pioneer_Status两个因子

涨跌停判定逻辑：
- 主板(沪深): 涨跌幅限制10%
- 创业板(300): 涨跌幅限制20%
- 科创板(688): 涨跌幅限制20%
- 北交所(8开头): 涨跌幅限制30%

涨停: close >= open * (1 + limit_rate)
跌停: close <= open * (1 - limit_rate)
"""
import polars as pl
from core.factor_library import BaseFactor, register_factor


@register_factor("limit_up_score")
class LimitUpScoreFactor(BaseFactor):
    """Limit_Up_Score因子
    (涨停家数 - 跌停家数) + 连板高度
    衡量短线赚钱效应
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "limit_up_score",
            category=category or "market",
            params=params or {},
            description=description or "涨停家数 - 跌停家数 + 连板高度"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        # 确定涨跌停判定比率
        # 主板: 10%, 创业板/科创板: 20%, 北交所: 30%
        def get_limit_rate(code):
            code_str = str(code)
            if code_str.startswith('300') or code_str.startswith('688'):
                return 0.20  # 创业板、科创板
            elif code_str.startswith('8') or code_str.startswith('4'):
                return 0.30  # 北交所
            else:
                return 0.10  # 主板

        # 计算每只股票的涨跌停标记
        data = data.with_columns([
            pl.col("code").map_elements(get_limit_rate, return_dtype=pl.Float64).alias("limit_rate")
        ])

        # 计算涨跌停 (使用前日收盘价判断)
        # 注意: 需要先有 prev_close 列, 如果没有则跳过涨跌停计算
        if "prev_close" in data.columns:
            # 处理 prev_close 为 0 的情况
            prev_close_safe = pl.when(pl.col("prev_close") == 0).then(1).otherwise(pl.col("prev_close"))
            change_pct = (pl.col("close") - pl.col("prev_close")) / prev_close_safe
            data = data.with_columns([
                (change_pct >= pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_up"),
                (change_pct <= -pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_down")
            ])
        else:
            # 如果没有 prev_close, 使用 open 近似 (不太准确)
            open_safe = pl.when(pl.col("open") == 0).then(1).otherwise(pl.col("open"))
            change_pct = (pl.col("close") - pl.col("open")) / open_safe
            data = data.with_columns([
                (change_pct >= pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_up"),
                (change_pct <= -pl.col("limit_rate")).cast(pl.Int64).alias("is_limit_down")
            ])

        # 按日期分组计算涨跌停家数和连板高度
        daily_stats = data.group_by("trade_date").agg([
            pl.sum("is_limit_up").alias("total_limit_up"),
            pl.sum("is_limit_down").alias("total_limit_down")
        ])

        # 计算连板高度：取涨停家数的对数作为市场热度指标
        # 涨停家数越多，市场越强势，连板高度代表赚钱效应
        daily_stats = daily_stats.with_columns([
            (pl.col("total_limit_up") / 50).clip(0, 10).alias("consecutive_height")
        ])

        # 计算情绪得分: (涨停家数 - 跌停家数) + 连板高度
        daily_stats = daily_stats.with_columns([
            (pl.col("total_limit_up") - pl.col("total_limit_down") + pl.col("consecutive_height")).alias(self.get_factor_column_name())
        ])

        # 合并回原数据
        data = data.join(
            daily_stats[["trade_date", self.get_factor_column_name(), "total_limit_up", "total_limit_down", "consecutive_height"]],
            on="trade_date",
            how="left"
        )

        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0)
        ])

        return data


@register_factor("pioneer_status")
class PioneerStatusFactor(BaseFactor):
    """Pioneer_Status因子
    核心领涨个股（如七彩化学）的实时涨跌幅
    捕捉市场崩塌的先兆
    """

    def __init__(self, name=None, category=None, params=None, description=None):
        super().__init__(
            name=name or "pioneer_status",
            category=category or "market",
            params=params or {"pioneer_codes": ["300255"]},  # 七彩化学代码
            description=description or "核心领涨个股实时涨跌幅"
        )

    def calculate(self, data: pl.DataFrame) -> pl.DataFrame:
        pioneer_codes = self.params.get("pioneer_codes", ["300255"])

        # 筛选核心领涨股
        pioneer_data = data.filter(pl.col("code").is_in(pioneer_codes))

        if len(pioneer_data) == 0:
            # 如果没有核心领涨股数据，返回默认值
            data = data.with_columns([
                pl.lit(0.0).alias(self.get_factor_column_name())
            ])
            return data

        # 计算核心领涨股的涨跌幅
        pioneer_data = pioneer_data.with_columns([
            ((pl.col("close") / pl.col("open") - 1) * 100).alias("pioneer_change")
        ])

        # 按日期分组取平均涨跌幅
        daily_pioneer = pioneer_data.group_by("trade_date").agg([
            pl.mean("pioneer_change").alias(self.get_factor_column_name())
        ])

        # 合并回原数据
        data = data.join(daily_pioneer, on="trade_date", how="left")

        # 填充缺失值
        data = data.with_columns([
            pl.col(self.get_factor_column_name()).fill_nan(0.0)
        ])

        return data
