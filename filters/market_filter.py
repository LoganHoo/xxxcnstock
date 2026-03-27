"""
市场数据过滤器
包括市值、停牌等过滤
"""
import polars as pl
from typing import Dict, Any

from filters.base_filter import BaseFilter, register_filter


@register_filter("market_cap_filter")
class MarketCapFilter(BaseFilter):
    """市值过滤器 - 排除市值低于阈值的股票"""
    
    def __init__(self, name: str = "market_cap_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除市值低于阈值的股票")
        self.min_market_cap = self.params.get("min_market_cap", 5_000_000_000)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤低市值股票"""
        if not self.enabled:
            return stock_list
        
        if "market_cap" not in stock_list.columns and "total_mv" not in stock_list.columns:
            self.logger.warning("缺少市值字段，跳过市值过滤")
            return stock_list
        
        cap_col = "market_cap" if "market_cap" in stock_list.columns else "total_mv"
        
        filtered = stock_list.filter(
            pl.col(cap_col) >= self.min_market_cap
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            min_cap_yi = self.min_market_cap / 100_000_000
            self.logger.info(f"市值过滤器: 移除 {removed_count} 只市值低于{min_cap_yi:.0f}亿的股票")
        
        return filtered


@register_filter("suspension_filter")
class SuspensionFilter(BaseFilter):
    """停牌过滤器 - 排除停牌股票"""
    
    def __init__(self, name: str = "suspension_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除停牌股票")
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤停牌股票"""
        if not self.enabled:
            return stock_list
        
        if "trade_status" not in stock_list.columns:
            if "volume" in stock_list.columns:
                filtered = stock_list.filter(pl.col("volume") > 0)
                removed_count = len(stock_list) - len(filtered)
                if removed_count > 0:
                    self.logger.info(f"停牌过滤器: 移除 {removed_count} 只成交量为0的股票")
                return filtered
            self.logger.warning("缺少trade_status或volume字段，跳过停牌过滤")
            return stock_list
        
        filtered = stock_list.filter(
            pl.col("trade_status") != "停牌"
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"停牌过滤器: 移除 {removed_count} 只停牌股票")
        
        return filtered


@register_filter("price_filter")
class PriceFilter(BaseFilter):
    """价格过滤器 - 排除价格超出范围的股票"""
    
    def __init__(self, name: str = "price_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除价格超出范围的股票")
        self.min_price = self.params.get("min_price", 2.0)
        self.max_price = self.params.get("max_price", 300.0)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤价格范围外的股票"""
        if not self.enabled:
            return stock_list
        
        if "close" not in stock_list.columns:
            self.logger.warning("缺少close字段，跳过价格过滤")
            return stock_list
        
        filtered = stock_list.filter(
            (pl.col("close") >= self.min_price) &
            (pl.col("close") <= self.max_price)
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"价格过滤器: 移除 {removed_count} 只价格不在"
                f"[{self.min_price}, {self.max_price}]范围内的股票"
            )
        
        return filtered


@register_filter("volume_filter")
class VolumeFilter(BaseFilter):
    """成交量过滤器 - 排除成交量过低的股票"""
    
    def __init__(self, name: str = "volume_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "排除成交量过低的股票")
        self.min_volume = self.params.get("min_volume", 1_000_000)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤低成交量股票"""
        if not self.enabled:
            return stock_list
        
        if "volume" not in stock_list.columns:
            self.logger.warning("缺少volume字段，跳过成交量过滤")
            return stock_list
        
        filtered = stock_list.filter(
            pl.col("volume") >= self.min_volume
        )
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(f"成交量过滤器: 移除 {removed_count} 只成交量低于{self.min_volume}的股票")
        
        return filtered
