"""
交易活跃度过滤器
包括量比、换手率、成交量稳定性等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("volume_ratio_filter")
class VolumeRatioFilter(BaseFilter):
    """量比过滤器 - 排除量比异常的股票"""
    
    def __init__(self, name: str = "volume_ratio_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除量比异常的股票")
        self.min_ratio = self.params.get("min_ratio", 1.0)
        self.max_ratio = self.params.get("max_ratio", 5.0)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤量比异常股票"""
        if not self.enabled:
            return stock_list
        
        if "volume_ratio" not in stock_list.columns:
            self.logger.warning("缺少volume_ratio字段，跳过量比过滤")
            return stock_list
        
        filtered = stock_list.filter(
            (pl.col("volume_ratio") >= self.min_ratio) &
            (pl.col("volume_ratio") <= self.max_ratio)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"量比过滤器: 移除 {removed_count} 只量比不在"
                f"[{self.min_ratio}, {self.max_ratio}]范围内的股票"
            )
        
        return filtered


@register_filter("turnover_rate_filter")
class TurnoverRateFilter(BaseFilter):
    """换手率过滤器 - 排除换手率异常的股票"""
    
    def __init__(self, name: str = "turnover_rate_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除换手率异常的股票")
        self.min_rate = self.params.get("min_rate", 0.05)
        self.max_rate = self.params.get("max_rate", 0.20)
        self.check_consecutive = self.params.get("check_consecutive", False)
        self.consecutive_days = self.params.get("consecutive_days", 2)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤换手率异常股票"""
        if not self.enabled:
            return stock_list
        
        if "turnover_rate" not in stock_list.columns:
            self.logger.warning("缺少turnover_rate字段，跳过换手率过滤")
            return stock_list
        
        filtered = stock_list.filter(
            (pl.col("turnover_rate") >= self.min_rate) &
            (pl.col("turnover_rate") <= self.max_rate)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"换手率过滤器: 移除 {removed_count} 只换手率不在"
                f"[{self.min_rate:.0%}, {self.max_rate:.0%}]范围内的股票"
            )
        
        return filtered


@register_filter("volume_stability_filter")
class VolumeStabilityFilter(BaseFilter):
    """成交量稳定性过滤器 - 排除成交量不稳定的股票"""
    
    def __init__(self, name: str = "volume_stability_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除成交量不稳定的股票")
        self.stability_threshold = self.params.get("stability_threshold", 0.5)
        self.lookback_period = self.params.get("lookback_period", 10)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤成交量不稳定股票"""
        if not self.enabled:
            return stock_list
        
        if "volume" not in stock_list.columns:
            self.logger.warning("缺少volume字段，跳过成交量稳定性过滤")
            return stock_list
        
        if len(stock_list) < self.lookback_period:
            return stock_list
        
        df = stock_list.clone()
        df = df.with_columns([
            pl.col("volume").rolling_std(self.lookback_period).alias("vol_std"),
            pl.col("volume").rolling_mean(self.lookback_period).alias("vol_mean")
        ])
        
        df = df.with_columns([
            (pl.col("vol_std") / pl.col("vol_mean")).alias("vol_cv")
        ])
        
        latest = df.tail(1)
        vol_cv = latest["vol_cv"].item()
        
        if vol_cv is not None and vol_cv > self.stability_threshold:
            self.logger.info(
                f"成交量稳定性过滤器: 成交量变异系数 {vol_cv:.2f} "
                f"超过阈值 {self.stability_threshold}，移除"
            )
            return stock_list.head(0)
        
        return stock_list


@register_filter("continuous_low_turnover_filter")
class ContinuousLowTurnoverFilter(BaseFilter):
    """连续低换手率过滤器 - 排除连续N天换手率低于阈值的股票"""
    
    def __init__(self, name: str = "continuous_low_turnover_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除连续低换手率的股票")
        self.min_rate = self.params.get("min_rate", 0.05)
        self.consecutive_days = self.params.get("consecutive_days", 2)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤连续低换手率股票"""
        if not self.enabled:
            return stock_list
        
        if "turnover_rate" not in stock_list.columns:
            self.logger.warning("缺少turnover_rate字段，跳过连续低换手率过滤")
            return stock_list
        
        if len(stock_list) < self.consecutive_days:
            return stock_list
        
        recent = stock_list.tail(self.consecutive_days)
        
        if (recent["turnover_rate"] < self.min_rate).all():
            self.logger.info(
                f"连续低换手率过滤器: 连续{self.consecutive_days}天换手率低于"
                f"{self.min_rate:.0%}，移除"
            )
            return stock_list.head(0)
        
        return stock_list
