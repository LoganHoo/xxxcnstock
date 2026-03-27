"""
技术趋势过滤器
包括趋势、均线位置、MACD金叉等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("trend_filter")
class TrendFilter(BaseFilter):
    """趋势过滤器 - 排除下跌趋势股票"""
    
    def __init__(self, name: str = "trend_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除下跌趋势股票")
        self.ma_short = self.params.get("ma_short", 5)
        self.ma_long = self.params.get("ma_long", 20)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤下跌趋势股票"""
        if not self.enabled:
            return stock_list
        
        required_cols = ["ma_short", "ma_long"]
        if not all(col in stock_list.columns for col in required_cols):
            if "close" in stock_list.columns:
                df = stock_list.clone()
                df = df.with_columns([
                    pl.col("close").rolling_mean(self.ma_short).alias("ma_short"),
                    pl.col("close").rolling_mean(self.ma_long).alias("ma_long")
                ])
                latest = df.tail(1)
                ma_short_val = latest["ma_short"].item()
                ma_long_val = latest["ma_long"].item()
                
                if ma_short_val is not None and ma_long_val is not None:
                    if ma_short_val < ma_long_val:
                        self.logger.info("趋势过滤器: 股票处于下跌趋势，移除")
                        return stock_list.head(0)
                return stock_list
            
            self.logger.warning("缺少ma_short/ma_long或close字段，跳过趋势过滤")
            return stock_list
        
        filtered = stock_list.filter(
            pl.col("ma_short") > pl.col("ma_long")
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"趋势过滤器: 移除 {removed_count} 只下跌趋势股票")
        
        return filtered


@register_filter("ma_position_filter")
class MaPositionFilter(BaseFilter):
    """均线位置过滤器 - 排除未站上均线的股票"""
    
    def __init__(self, name: str = "ma_position_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除未站上均线的股票")
        self.ma_periods = self.params.get("ma_periods", [20, 60])
        self.require_all = self.params.get("require_all", False)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤未站上均线的股票"""
        if not self.enabled:
            return stock_list
        
        if "close" not in stock_list.columns:
            self.logger.warning("缺少close字段，跳过均线位置过滤")
            return stock_list
        
        conditions = []
        for period in self.ma_periods:
            ma_col = f"ma{period}"
            if ma_col in stock_list.columns:
                conditions.append(pl.col("close") > pl.col(ma_col))
        
        if not conditions:
            self.logger.warning("缺少均线字段，跳过均线位置过滤")
            return stock_list
        
        if self.require_all:
            combined = conditions[0]
            for cond in conditions[1:]:
                combined = combined & cond
        else:
            combined = conditions[0]
            for cond in conditions[1:]:
                combined = combined | cond
        
        filtered = stock_list.filter(combined)
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"均线位置过滤器: 移除 {removed_count} 只未站上均线的股票")
        
        return filtered


@register_filter("monthly_ma_filter")
class MonthlyMaFilter(BaseFilter):
    """月均线过滤器 - 排除跌破60月均线的股票"""
    
    def __init__(self, name: str = "monthly_ma_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除跌破60月均线的股票")
        self.ma_period = self.params.get("ma_period", 60)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤跌破月均线的股票"""
        if not self.enabled:
            return stock_list
        
        ma_col = f"monthly_ma{self.ma_period}"
        
        if ma_col not in stock_list.columns and "close" not in stock_list.columns:
            self.logger.warning("缺少月均线字段，跳过月均线过滤")
            return stock_list
        
        if ma_col in stock_list.columns:
            filtered = stock_list.filter(
                pl.col("close") > pl.col(ma_col)
            )
        else:
            filtered = stock_list
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"月均线过滤器: 移除 {removed_count} 只跌破{self.ma_period}月均线的股票")
        
        return filtered


@register_filter("macd_cross_filter")
class MacdCrossFilter(BaseFilter):
    """MACD金叉过滤器 - 排除月线MACD未金叉/日线死叉股票"""
    
    def __init__(self, name: str = "macd_cross_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除月线MACD未金叉/日线死叉股票")
        self.check_monthly = self.params.get("check_monthly", True)
        self.check_daily = self.params.get("check_daily", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤MACD信号不佳的股票"""
        if not self.enabled:
            return stock_list
        
        filtered = stock_list.clone()
        
        if self.check_monthly:
            if "monthly_macd_dif" in filtered.columns and "monthly_macd_dea" in filtered.columns:
                filtered = filtered.filter(
                    pl.col("monthly_macd_dif") > pl.col("monthly_macd_dea")
                )
        
        if self.check_daily:
            if "macd_dif" in filtered.columns and "macd_dea" in filtered.columns:
                filtered = filtered.filter(
                    pl.col("macd_dif") > pl.col("macd_dea")
                )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"MACD金叉过滤器: 移除 {removed_count} 只MACD信号不佳的股票")
        
        return filtered
