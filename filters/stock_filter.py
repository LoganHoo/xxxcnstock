"""
股票属性过滤器
包括ST股、新股、退市风险等过滤
"""
import polars as pl
from typing import Dict, Any
from datetime import datetime, timedelta

from filters.base_filter import BaseFilter, register_filter


@register_filter("st_filter")
class STFilter(BaseFilter):
    """ST股票过滤器 - 排除ST/*ST股票"""
    
    def __init__(self, name: str = "st_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除ST/*ST股票")
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤ST股票"""
        if not self.enabled or "name" not in stock_list.columns:
            return stock_list
        
        st_pattern = r"(ST|st|\*ST|\*st|S\*T|S\*st)"
        
        filtered = stock_list.filter(
            ~pl.col("name").str.contains(st_pattern)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"ST过滤器: 移除 {removed_count} 只ST股票")
        
        return filtered


@register_filter("new_stock_filter")
class NewStockFilter(BaseFilter):
    """新股过滤器 - 排除上市不足N天的股票"""
    
    def __init__(self, name: str = "new_stock_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除上市不足N天的新股")
        self.min_listing_days = self.params.get("min_listing_days", 60)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤新股"""
        if not self.enabled:
            return stock_list
        
        if "list_date" not in stock_list.columns:
            self.logger.warning("缺少list_date字段，跳过新股过滤")
            return stock_list
        
        cutoff_datetime = datetime.now() - timedelta(days=self.min_listing_days)
        list_date_dtype = stock_list.schema.get("list_date")

        if list_date_dtype == pl.Date:
            cutoff_date = cutoff_datetime.date()
        elif list_date_dtype == pl.Datetime:
            cutoff_date = cutoff_datetime
        else:
            cutoff_date = cutoff_datetime.strftime("%Y-%m-%d")
        
        filtered = stock_list.filter(
            pl.col("list_date") <= cutoff_date
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"新股过滤器: 移除 {removed_count} 只上市不足{self.min_listing_days}天的股票")
        
        return filtered


@register_filter("delisting_filter")
class DelistingFilter(BaseFilter):
    """退市风险过滤器 - 排除有退市风险的股票"""
    
    DELISTING_PATTERNS = [
        r"退",
        r"退市",
        r"终止上市",
        r"暂停上市",
        r"风险警示",
    ]
    
    def __init__(self, name: str = "delisting_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除有退市风险的股票")
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤退市风险股票"""
        if not self.enabled or "name" not in stock_list.columns:
            return stock_list
        
        pattern = "|".join(self.DELISTING_PATTERNS)
        
        filtered = stock_list.filter(
            ~pl.col("name").str.contains(pattern)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"退市风险过滤器: 移除 {removed_count} 只有退市风险的股票")
        
        return filtered
