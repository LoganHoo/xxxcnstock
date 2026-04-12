"""
指标引擎
实现资金行为学系统的指标层逻辑
"""
import polars as pl
from typing import Dict, Any
import logging

from core.fund_behavior_config import config_manager

logger = logging.getLogger(__name__)


class FundBehaviorIndicatorEngine:
    """资金行为学指标引擎"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.FundBehaviorIndicatorEngine")

    def auto_adjust_thresholds(self, data: pl.DataFrame):
        """
        根据实际数据范围自动调整阈值

        Args:
            data: 包含因子数据的DataFrame
        """
        if data is None or len(data) == 0:
            self.logger.warning("数据为空，跳过阈值自动调整")
            return
            
        if "factor_v_total" not in data.columns:
            return

        try:
            v_total_stats = data.select([
                pl.col("factor_v_total").mean().alias("mean"),
                pl.col("factor_v_total").min().alias("min"),
                pl.col("factor_v_total").max().alias("max"),
                pl.col("factor_v_total").quantile(0.25).alias("q25"),
                pl.col("factor_v_total").quantile(0.75).alias("q75")
            ]).to_dict(as_series=False)

            v_total_mean = v_total_stats["mean"][0] if v_total_stats.get("mean") and v_total_stats["mean"][0] is not None else 0
            v_total_q75 = v_total_stats["q75"][0] if v_total_stats.get("q75") and v_total_stats["q75"][0] is not None else 0
        except Exception as e:
            self.logger.warning(f"计算v_total统计失败: {e}")
            v_total_mean = 0
            v_total_q75 = 0

        adjusted_items = {}

        # 动态调整 strong_v_total（使用75分位数）
        if v_total_q75 and v_total_q75 > 0:
            new_strong_v_total = v_total_q75 * 0.9  # 90%分位数作为强市阈值
            adjusted_items['indicators.market_sentiment.thresholds.strong_v_total'] = new_strong_v_total
            self.logger.info(f"动态调整 strong_v_total: {new_strong_v_total:.2f} (基于75分位数 {v_total_q75:.2f})")

        # 动态调整 oscillating 范围
        if v_total_mean and v_total_mean > 0:
            new_osc_min = v_total_mean * 0.8
            new_osc_max = v_total_mean * 1.2
            adjusted_items['indicators.market_sentiment.thresholds.oscillating_v_total_min'] = new_osc_min
            adjusted_items['indicators.market_sentiment.thresholds.oscillating_v_total_max'] = new_osc_max
            self.logger.info(f"动态调整 oscillating_v_total: [{new_osc_min:.2f}, {new_osc_max:.2f}] (基于均值 {v_total_mean:.2f})")

        # 动态调整 price_threshold（使用收盘价均值）
        if "close" in data.columns:
            try:
                close_stats = data.select([
                    pl.col("close").mean().alias("mean"),
                    pl.col("close").quantile(0.5).alias("median")
                ]).to_dict(as_series=False)
                new_price_threshold = close_stats["median"][0] if close_stats.get("median") and close_stats["median"][0] is not None else 4000
            except Exception as e:
                self.logger.warning(f"计算close统计失败: {e}")
                new_price_threshold = 4000
            if new_price_threshold:
                adjusted_items['indicators.10am_pivot.price_threshold'] = new_price_threshold
                self.logger.info(f"动态调整 price_threshold: {new_price_threshold:.2f}")

        # 动态调整 cost_peak_support（使用实际峰位均值）
        if "factor_cost_peak" in data.columns:
            try:
                cost_peak_stats = data.select([
                    pl.col("factor_cost_peak").mean().alias("mean"),
                    pl.col("factor_cost_peak").std().alias("std")
                ]).to_dict(as_series=False)
                if cost_peak_stats.get("mean") and cost_peak_stats["mean"][0] is not None:
                    new_cost_peak_support = cost_peak_stats["mean"][0] * 0.98
                    adjusted_items['indicators.market_sentiment.thresholds.cost_peak_support'] = new_cost_peak_support
                    self.logger.info(f"动态调整 cost_peak_support: {new_cost_peak_support:.2f}")
            except Exception as e:
                self.logger.warning(f"计算cost_peak统计失败: {e}")

        # 批量保存
        if adjusted_items:
            config_manager.batch_set(adjusted_items)

    def calculate_market_sentiment(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        计算市场定性指标
        
        Args:
            data: 包含所有因子数据的DataFrame
        
        Returns:
            市场状态字典
        """
        # 从配置中获取阈值
        thresholds = config_manager.get('indicators.market_sentiment.thresholds', {
            'strong_v_total': 1800,  # 强市阈值（亿），实际数据约1600-1900亿
            'oscillating_v_total_min': 1200,  # 震荡下限（亿）
            'oscillating_v_total_max': 2000,  # 震荡上限（亿）
            'sentiment_temperature_strong': 50,
            'sentiment_temperature_overheat': 80,
            'cost_peak_support': 0.995
        })
        
        # 按日期分组计算市场指标
        # 注意: limit_up_score因子已经按日期聚合了涨跌停数据
        # 每只股票在同一天的 factor_limit_up_score, total_limit_up 等值相同
        daily_data = data.group_by("trade_date").agg([
            pl.mean("factor_v_total").alias("avg_v_total"),
            pl.mean("factor_limit_up_score").alias("avg_limit_up_score"),
            pl.mean("factor_pioneer_status").alias("avg_pioneer_status"),
            pl.mean("factor_cost_peak").alias("avg_cost_peak"),
            pl.mean("close").alias("avg_close"),
            pl.mean("open").alias("avg_open"),
            pl.count().alias("total_stocks"),
            pl.first("total_limit_up").alias("total_limit_up"),
            pl.first("total_limit_down").alias("total_limit_down")
        ]).sort("trade_date")

        # 注意: limit_up_score 因子已经包含了全市场聚合值(每只股票该日期相同)
        # 这里取平均值即可得到市场级别的涨跌停得分
        # 同时计算情绪温度: 基于涨停家数和赚钱效应
        # 情绪温度范围 0-100，50为中性，>50偏热，<50偏冷
        daily_data = daily_data.with_columns([
            ((pl.col("avg_limit_up_score") + 100) / 4).clip(0, 100).alias("sentiment_temperature")
        ])
        
        # 计算温度变化
        daily_data = daily_data.with_columns([
            pl.col("sentiment_temperature").shift(1).alias("prev_temp")
        ])
        
        daily_data = daily_data.with_columns([
            (pl.col("sentiment_temperature") - pl.col("prev_temp")).alias("delta_temperature")
        ])
        
        # 计算预期跌幅（用于惯性判定）
        daily_data = daily_data.with_columns([
            (pl.col("avg_close") * 0.97).alias("expected_drop")
        ])
        
        # 先锋哨兵修正
        daily_data = daily_data.with_columns([
            pl.when(pl.col("avg_pioneer_status") < -5)
             .then(pl.lit(0.5))
             .otherwise(pl.lit(1.0))
             .alias("sentiment_multiplier")
        ])
        
        # 应用先锋修正
        daily_data = daily_data.with_columns([
            (pl.col("sentiment_temperature") * pl.col("sentiment_multiplier")).alias("adjusted_temperature")
        ])
        
        # 计算惯性信号
        daily_data = daily_data.with_columns([
            pl.when((pl.col("delta_temperature") < -30) & (pl.col("avg_open") > pl.col("expected_drop")))
             .then(pl.lit("Inertia_Sell"))
             .otherwise(pl.lit("Normal"))
             .alias("inertia_signal")
        ])
        
        # 计算平均筹码峰位作为动态基准
        avg_cost_peak_mean = daily_data["avg_cost_peak"].mean() if len(daily_data) > 0 else 0
        
        # 确定市场状态
        def determine_market_state(row):
            v_total = row.get("avg_v_total") or 0
            temp = row.get("adjusted_temperature") or 50
            pioneer_status = row.get("avg_pioneer_status") or 0
            cost_peak = row.get("avg_cost_peak") or 0

            # 风险预警
            if pioneer_status < -5 or temp > thresholds['sentiment_temperature_overheat']:
                return "risk"

            # 强市
            if v_total > thresholds['strong_v_total'] and temp > thresholds['sentiment_temperature_strong']:
                return "strong"

            # 震荡
            if (thresholds['oscillating_v_total_min'] < v_total < thresholds['oscillating_v_total_max'] and
                cost_peak > avg_cost_peak_mean * thresholds['cost_peak_support']):
                return "oscillating"

            # 弱市
            return "weak"
        
        daily_data = daily_data.with_columns([
            pl.struct(daily_data.columns).map_elements(
                lambda row: determine_market_state(row), 
                return_dtype=pl.Utf8
            ).alias("market_state")
        ])
        
        return daily_data.to_dict(as_series=False)
    
    def calculate_10am_pivot(self, data: pl.DataFrame) -> Dict[str, bool]:
        """
        10点定基调
        
        Args:
            data: 包含因子数据的DataFrame
        
        Returns:
            向上变盘信号字典
        """
        # 从配置中获取阈值
        # price_threshold: 全市场平均股价阈值(元)，用于判断市场整体价格水位
        # 注意: avg_close 是全市场平均股价(约10-20元)，不是指数点位
        pivot_config = config_manager.get('indicators.10am_pivot', {
            'v_ratio10_threshold': 1.1,
            'price_threshold': 15.0  # 全市场平均股价阈值(元)
        })

        # 按日期分组计算10点定基调指标
        daily_data = data.group_by("trade_date").agg([
            pl.mean("factor_v_ratio10").alias("avg_v_ratio10"),
            pl.mean("close").alias("avg_close"),  # 全市场平均股价(元)
            pl.mean("factor_ma5_bias").alias("avg_ma5_bias")
        ]).sort("trade_date")

        # 计算分时线斜率（使用MA5偏差作为替代）
        daily_data = daily_data.with_columns([
            pl.col("avg_ma5_bias").alias("trend_slope")
        ])

        # 确定向上变盘信号
        # 条件: 早盘放量 > 1.1倍 AND 全市场平均股价 > 阈值 AND 趋势向上
        daily_data = daily_data.with_columns([
            ((pl.col("avg_v_ratio10") > pivot_config['v_ratio10_threshold']) &
             (pl.col("avg_close") > pivot_config['price_threshold']) &
             (pl.col("trend_slope") > 0)).alias("upward_pivot")
        ])
        
        return daily_data.select(["trade_date", "upward_pivot"]).to_dict(as_series=False)
    
    def calculate_exit_lines(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        机械化减仓线
        
        Args:
            data: 包含因子数据的DataFrame
        
        Returns:
            减仓信号字典
        """
        # 计算VWAP（成交量加权平均价）
        data = data.with_columns([
            (pl.col("volume") * pl.col("close")).alias("volume_price")
        ])
        
        daily_data = data.group_by("trade_date").agg([
            pl.sum("volume_price").alias("total_volume_price"),
            pl.sum("volume").alias("total_volume"),
            pl.mean("open").alias("avg_open"),
            pl.mean("close").alias("avg_close"),
            pl.mean("high").alias("avg_high")
        ]).sort("trade_date")
        
        # 计算VWAP
        daily_data = daily_data.with_columns([
            (pl.col("total_volume_price") / pl.col("total_volume")).alias("vwap")
        ])
        
        # 预期线：是否封板（收盘价接近涨停）
        daily_data = daily_data.with_columns([
            ((pl.col("avg_close") / pl.col("avg_open") > 1.095)).alias("opening_limit")
        ])
        
        # 均价线：是否跌破分时黄线
        daily_data = daily_data.with_columns([
            ((pl.col("avg_close") < pl.col("vwap")).alias("break_vwap"))
        ])
        
        # 收盘线：是否封板或翻绿
        daily_data = daily_data.with_columns([
            (((pl.col("avg_close") / pl.col("avg_open") > 1.095) | 
              (pl.col("avg_close") < pl.col("avg_open"))).alias("closing_condition"))
        ])
        
        return daily_data.select([
            "trade_date", "opening_limit", "break_vwap", "closing_condition"
        ]).to_dict(as_series=False)
    
    def calculate_all_indicators(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        计算所有指标

        Args:
            data: 包含所有因子数据的DataFrame

        Returns:
            综合指标结果
        """
        # 先自动调整阈值
        self.auto_adjust_thresholds(data)

        result = {
            "market_sentiment": self.calculate_market_sentiment(data),
            "10am_pivot": self.calculate_10am_pivot(data),
            "exit_lines": self.calculate_exit_lines(data)
        }

        return result
