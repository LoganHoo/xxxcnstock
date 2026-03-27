"""
基本面风控过滤器
包括违法违规、业绩风险、大盘熔断等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("illegal_filter")
class IllegalFilter(BaseFilter):
    """违法违规过滤器 - 排除减持计划、快退市、财务造假股票"""
    
    RISK_KEYWORDS = [
        "减持计划",
        "减持公告",
        "退市",
        "终止上市",
        "财务造假",
        "立案调查",
        "行政处罚",
        "违规",
    ]
    
    def __init__(self, name: str = "illegal_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除减持计划、快退市、财务造假股票")
        self.exclude_types = self.params.get("exclude_types", self.RISK_KEYWORDS)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤违法违规股票"""
        if not self.enabled:
            return stock_list
        
        if "risk_flag" not in stock_list.columns and "announcement" not in stock_list.columns:
            self.logger.warning("缺少risk_flag或announcement字段，跳过违法违规过滤")
            return stock_list
        
        check_col = "risk_flag" if "risk_flag" in stock_list.columns else "announcement"
        pattern = "|".join(self.exclude_types)
        
        filtered = stock_list.filter(
            ~pl.col(check_col).str.contains(pattern)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"违法违规过滤器: 移除 {removed_count} 只有风险股票")
        
        return filtered


@register_filter("performance_filter")
class PerformanceFilter(BaseFilter):
    """业绩过滤器 - 排除前三季度业绩亏损/暴雷股票"""
    
    def __init__(self, name: str = "performance_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除前三季度业绩亏损/暴雷股票")
        self.check_quarters = self.params.get("check_quarters", 3)
        self.min_profit_growth = self.params.get("min_profit_growth", -0.5)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤业绩亏损股票"""
        if not self.enabled:
            return stock_list
        
        required_cols = ["net_profit"]
        if not all(col in stock_list.columns for col in required_cols):
            self.logger.warning("缺少net_profit字段，跳过业绩过滤")
            return stock_list
        
        filtered = stock_list.filter(
            pl.col("net_profit") > 0
        )
        
        if "profit_yoy" in stock_list.columns:
            filtered = filtered.filter(
                pl.col("profit_yoy") >= self.min_profit_growth
            )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"业绩过滤器: 移除 {removed_count} 只业绩亏损/暴雷股票")
        
        return filtered


@register_filter("market_crash_filter")
class MarketCrashFilter(BaseFilter):
    """大盘熔断过滤器 - 大盘跌幅超阈值时返回空列表"""
    
    def __init__(self, name: str = "market_crash_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "大盘跌幅超阈值时暂停买入")
        self.max_index_drop = self.params.get("max_index_drop", 0.02)
        self.index_code = self.params.get("index_code", "000001")
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """大盘熔断过滤"""
        if not self.enabled:
            return stock_list
        
        if "index_change_pct" not in stock_list.columns:
            self.logger.warning("缺少index_change_pct字段，跳过大盘熔断过滤")
            return stock_list
        
        latest_index_change = stock_list.select("index_change_pct").head(1).item()
        
        if latest_index_change is not None and latest_index_change < -self.max_index_drop:
            self.logger.warning(
                f"大盘跌幅 {latest_index_change:.2%} 超过阈值 {self.max_index_drop:.2%}，暂停买入"
            )
            return stock_list.head(0)
        
        return stock_list
