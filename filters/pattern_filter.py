"""
特定筹码形态过滤器
包括涨停陷阱、过度炒作、回补信号等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("limit_up_trap_filter")
class LimitUpTrapFilter(BaseFilter):
    """涨停陷阱过滤器 - 排除涨停后一字板打开/换手率过高的股票"""
    
    def __init__(self, name: str = "limit_up_trap_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除涨停陷阱股票")
        self.max_turnover_on_limit = self.params.get("max_turnover_on_limit", 0.20)
        self.exclude_one_word_board = self.params.get("exclude_one_word_board", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤涨停陷阱股票"""
        if not self.enabled:
            return stock_list
        
        filtered = stock_list.clone()
        
        if "limit_up_type" in filtered.columns and self.exclude_one_word_board:
            filtered = filtered.filter(
                pl.col("limit_up_type") != "一字板"
            )
        
        if "turnover_rate" in filtered.columns and "is_limit_up" in filtered.columns:
            filtered = filtered.filter(
                ~((pl.col("is_limit_up")) &
                  (pl.col("turnover_rate") > self.max_turnover_on_limit))
            )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"涨停陷阱过滤器: 移除 {removed_count} 只涨停陷阱股票")
        
        return filtered


@register_filter("over_hyped_filter")
class OverHypedFilter(BaseFilter):
    """过度炒作过滤器 - 排除三连板以上/前期大涨股票"""
    
    def __init__(self, name: str = "over_hyped_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除过度炒作股票")
        self.max_limit_days = self.params.get("max_limit_days", 3)
        self.max_gain_period = self.params.get("max_gain_period", 20)
        self.max_gain_pct = self.params.get("max_gain_pct", 0.5)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤过度炒作股票"""
        if not self.enabled:
            return stock_list
        
        filtered = stock_list.clone()
        
        if "continuous_limit_days" in filtered.columns:
            filtered = filtered.filter(
                pl.col("continuous_limit_days") <= self.max_limit_days
            )
        
        if "gain_pct_20d" in filtered.columns:
            filtered = filtered.filter(
                pl.col("gain_pct_20d") <= self.max_gain_pct
            )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"过度炒作过滤器: 移除 {removed_count} 只过度炒作股票")
        
        return filtered


@register_filter("pullback_signal_filter")
class PullbackSignalFilter(BaseFilter):
    """回补信号过滤器 - 排除涨停后N天未回踩均线的股票"""
    
    def __init__(self, name: str = "pullback_signal_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除无回补信号的股票")
        self.pullback_days = self.params.get("pullback_days", 3)
        self.ma_period = self.params.get("ma_period", 5)
        self.require_pullback = self.params.get("require_pullback", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤无回补信号股票"""
        if not self.enabled:
            return stock_list
        
        if "days_since_limit" not in stock_list.columns:
            self.logger.warning("缺少days_since_limit字段，跳过回补信号过滤")
            return stock_list
        
        if not self.require_pullback:
            return stock_list
        
        ma_col = f"ma{self.ma_period}"
        
        if ma_col not in stock_list.columns:
            self.logger.warning(f"缺少{ma_col}字段，跳过回补信号过滤")
            return stock_list
        
        filtered = stock_list.filter(
            ~((pl.col("days_since_limit") >= self.pullback_days) &
              (~pl.col("has_pullback")))
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"回补信号过滤器: 移除 {removed_count} 只涨停后"
                f"{self.pullback_days}天未回踩{self.ma_period}日线的股票"
            )
        
        return filtered


@register_filter("institution_signal_filter")
class InstitutionSignalFilter(BaseFilter):
    """机构信号过滤器 - 排除无机构买入信号的股票"""
    
    def __init__(self, name: str = "institution_signal_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除无机构买入信号的股票")
        self.require_institution = self.params.get("require_institution", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤无机构信号股票"""
        if not self.enabled:
            return stock_list
        
        if not self.require_institution:
            return stock_list
        
        if "institution_buy_signal" not in stock_list.columns:
            self.logger.warning("缺少institution_buy_signal字段，跳过机构信号过滤")
            return stock_list
        
        filtered = stock_list.filter(
            pl.col("institution_buy_signal")
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"机构信号过滤器: 移除 {removed_count} 只无机构买入信号的股票")
        
        return filtered


@register_filter("limit_up_after_filter")
class LimitUpAfterFilter(BaseFilter):
    """涨停后状态过滤器 - 排除涨停后状态不佳的股票"""
    
    def __init__(self, name: str = "limit_up_after_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除涨停后状态不佳的股票")
        self.max_days_after_limit = self.params.get("max_days_after_limit", 5)
        self.require_volume_shrink = self.params.get("require_volume_shrink", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤涨停后状态不佳股票"""
        if not self.enabled:
            return stock_list
        
        filtered = stock_list.clone()
        
        if "days_since_limit" in filtered.columns:
            filtered = filtered.filter(
                pl.col("days_since_limit") <= self.max_days_after_limit
            )
        
        if self.require_volume_shrink and "volume_shrink" in filtered.columns:
            filtered = filtered.filter(
                pl.col("volume_shrink")
            )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"涨停后状态过滤器: 移除 {removed_count} 只涨停后状态不佳的股票")
        
        return filtered
