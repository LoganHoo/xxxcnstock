"""
市值价格过滤器
包括流通市值、价格区间等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("float_market_cap_filter")
class FloatMarketCapFilter(BaseFilter):
    """流通市值过滤器 - 排除流通市值异常的股票"""
    
    def __init__(self, name: str = "float_market_cap_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除流通市值异常的股票")
        self.min_cap = self.params.get("min_cap", 3_000_000_000)
        self.max_cap = self.params.get("max_cap", 30_000_000_000)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤流通市值异常股票"""
        if not self.enabled:
            return stock_list
        
        cap_col = None
        for col in ["float_market_cap", "circ_mv", "market_cap"]:
            if col in stock_list.columns:
                cap_col = col
                break
        
        if cap_col is None:
            self.logger.warning("缺少市值字段，跳过流通市值过滤")
            return stock_list
        
        filtered = stock_list.filter(
            (pl.col(cap_col) >= self.min_cap) &
            (pl.col(cap_col) <= self.max_cap)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            min_cap_yi = self.min_cap / 100_000_000
            max_cap_yi = self.max_cap / 100_000_000
            self.logger.info(
                f"流通市值过滤器: 移除 {removed_count} 只市值不在"
                f"[{min_cap_yi:.0f}亿, {max_cap_yi:.0f}亿]范围内的股票"
            )
        
        return filtered


@register_filter("price_range_filter")
class PriceRangeFilter(BaseFilter):
    """价格区间过滤器 - 排除价格不在指定区间的股票"""
    
    def __init__(self, name: str = "price_range_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除价格不在指定区间的股票")
        self.min_price = self.params.get("min_price", 5.0)
        self.max_price = self.params.get("max_price", 80.0)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤价格区间外的股票"""
        if not self.enabled:
            return stock_list
        
        if "close" not in stock_list.columns:
            self.logger.warning("缺少close字段，跳过价格区间过滤")
            return stock_list
        
        filtered = stock_list.filter(
            (pl.col("close") >= self.min_price) &
            (pl.col("close") <= self.max_price)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"价格区间过滤器: 移除 {removed_count} 只价格不在"
                f"[{self.min_price}, {self.max_price}]范围内的股票"
            )
        
        return filtered


@register_filter("valuation_filter")
class ValuationFilter(BaseFilter):
    """估值过滤器 - 排除估值异常的股票"""
    
    def __init__(self, name: str = "valuation_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除估值异常的股票")
        self.max_pe = self.params.get("max_pe", 100)
        self.max_pb = self.params.get("max_pb", 10)
        self.check_pe = self.params.get("check_pe", True)
        self.check_pb = self.params.get("check_pb", True)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤估值异常股票"""
        if not self.enabled:
            return stock_list
        
        filtered = stock_list.clone()
        
        if self.check_pe and "pe_ttm" in filtered.columns:
            filtered = filtered.filter(
                (pl.col("pe_ttm") > 0) &
                (pl.col("pe_ttm") <= self.max_pe)
            )
        
        if self.check_pb and "pb" in filtered.columns:
            filtered = filtered.filter(
                (pl.col("pb") > 0) &
                (pl.col("pb") <= self.max_pb)
            )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"估值过滤器: 移除 {removed_count} 只估值异常的股票")
        
        return filtered
