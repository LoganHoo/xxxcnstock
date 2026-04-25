#!/usr/bin/env python3
"""
公告事件选股过滤器

基于公告事件进行选股过滤:
- 重大事项: 并购重组、股权激励、增发
- 业绩预告: 预增、扭亏
- 股权变动: 增持、回购
- 交易提示: 复牌

使用示例:
    filter = PerformanceForecastFilter(params={"forecast_types": ["预增", "扭亏"]})
    filtered_stocks = filter.filter(stock_list)
"""
import polars as pl
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from filters.base_filter import BaseFilter, FilterRegistry
from services.data_service.fetchers.announcement import (
    AnnouncementFetcher,
    AnnouncementType,
)

from core.logger import get_logger

logger = get_logger(__name__)


def register_filter(name: str):
    """过滤器注册装饰器"""
    def decorator(cls):
        FilterRegistry.register(name, cls)
        return cls
    return decorator


class AnnouncementFilterBase(BaseFilter):
    """公告过滤器基类"""
    
    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        super().__init__(name, params, description)
        self.announcement_fetcher = AnnouncementFetcher()


@register_filter("performance_forecast_filter")
class PerformanceForecastFilter(AnnouncementFilterBase):
    """
    业绩预告过滤器
    
    筛选业绩预告类型:
    - 预增: 净利润大幅增长
    - 预减: 净利润大幅下降(反向筛选)
    - 扭亏: 亏损转盈利
    - 预亏: 盈利转亏损(反向筛选)
    """
    
    def __init__(self, name: str = "performance_forecast_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "业绩预告过滤器")
        self.forecast_types = self.params.get("forecast_types", ["预增", "扭亏"])
        self.min_change_range = self.params.get("min_change_range", 20.0)  # 最小变动幅度
        self.lookback_days = self.params.get("lookback_days", 7)  # 回溯天数
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤业绩预告股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
        
        # 获取业绩预告
        forecast_df = self.announcement_fetcher.fetch_performance_forecasts(start_date, end_date)
        
        if forecast_df.empty:
            self.logger.warning("业绩预告数据为空")
            return stock_list
        
        # 筛选指定类型的预告
        if 'forecast_type' in forecast_df.columns:
            filtered_forecast = forecast_df[
                forecast_df['forecast_type'].isin(self.forecast_types)
            ]
            forecast_codes = filtered_forecast['code'].unique().tolist()
        else:
            forecast_codes = forecast_df['code'].unique().tolist()
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(forecast_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"业绩预告过滤器: 筛选出 {len(filtered)} 只{self.forecast_types}股票"
            )
        
        return filtered


@register_filter("major_event_filter")
class MajorEventFilter(AnnouncementFilterBase):
    """
    重大事项过滤器
    
    筛选重大事项公告:
    - 并购重组
    - 股权激励
    - 增发
    - 重大合同
    """
    
    def __init__(self, name: str = "major_event_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "重大事项过滤器")
        self.event_types = self.params.get("event_types", ["并购重组", "股权激励", "重大合同"])
        self.lookback_days = self.params.get("lookback_days", 3)
        self.importance = self.params.get("importance", "high")  # high/normal/all
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤重大事项股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
        
        # 获取重大事项
        events_df = self.announcement_fetcher.fetch_major_events(start_date, end_date)
        
        if events_df.empty:
            self.logger.warning("重大事项数据为空")
            return stock_list
        
        # 按重要性筛选
        if self.importance == "high" and 'importance' in events_df.columns:
            events_df = events_df[events_df['importance'] == 'high']
        
        # 按事件类型筛选
        if 'announcement_type' in events_df.columns:
            filtered_events = events_df[
                events_df['announcement_type'].isin(self.event_types)
            ]
            event_codes = filtered_events['code'].unique().tolist()
        else:
            event_codes = events_df['code'].unique().tolist()
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(event_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"重大事项过滤器: 筛选出 {len(filtered)} 只{self.event_types}股票"
            )
        
        return filtered


@register_filter("equity_change_filter")
class EquityChangeFilter(AnnouncementFilterBase):
    """
    股权变动过滤器
    
    筛选股权变动公告:
    - 增持: 大股东或管理层增持
    - 回购: 公司股份回购
    """
    
    def __init__(self, name: str = "equity_change_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "股权变动过滤器")
        self.change_types = self.params.get("change_types", ["增持"])  # 增持/减持
        self.min_change_ratio = self.params.get("min_change_ratio", 0.5)  # 最小变动比例(%)
        self.lookback_days = self.params.get("lookback_days", 7)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤股权变动股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
        
        # 获取股权变动数据
        changes_df = self.announcement_fetcher.fetch_equity_changes(start_date, end_date)
        
        if changes_df.empty:
            self.logger.warning("股权变动数据为空")
            return stock_list
        
        # 筛选增持类型
        if 'announcement_type' in changes_df.columns:
            filtered_changes = changes_df[
                changes_df['announcement_type'].isin(self.change_types)
            ]
            
            # 筛选变动比例
            if 'change_ratio' in filtered_changes.columns:
                filtered_changes = filtered_changes[
                    filtered_changes['change_ratio'] >= self.min_change_ratio
                ]
            
            change_codes = filtered_changes['code'].unique().tolist()
        else:
            change_codes = changes_df['code'].unique().tolist()
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(change_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"股权变动过滤器: 筛选出 {len(filtered)} 只{self.change_types}股票"
            )
        
        return filtered


@register_filter("trading_resume_filter")
class TradingResumeFilter(AnnouncementFilterBase):
    """
    复牌股票过滤器
    
    筛选即将复牌或刚复牌的股票
    """
    
    def __init__(self, name: str = "trading_resume_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "复牌股票过滤器")
        self.lookback_days = self.params.get("lookback_days", 1)
        self.include_expected = self.params.get("include_expected", True)  # 包含预计复牌
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤复牌股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 获取交易提示(停牌复牌)
        hints_df = self.announcement_fetcher.fetch_trading_hints()
        
        if hints_df.empty:
            self.logger.warning("交易提示数据为空")
            return stock_list
        
        # 获取停牌股票代码
        if 'code' in hints_df.columns:
            halt_codes = hints_df['code'].unique().tolist()
            # 排除停牌股票(保留未停牌的)
            filtered = stock_list.filter(~pl.col('code').is_in(halt_codes))
            
            removed_count = len(stock_list) - len(filtered)
            if removed_count > 0:
                self.logger.info(f"复牌过滤器: 排除 {removed_count} 只停牌股票")
            
            return filtered
        
        return stock_list


@register_filter("announcement_composite_filter")
class AnnouncementCompositeFilter(AnnouncementFilterBase):
    """
    公告综合过滤器
    
    综合多个公告类型:
    - 业绩预告(预增/扭亏)
    - 重大事项(并购重组/股权激励)
    - 股权变动(增持)
    
    满足任一条件即可
    """
    
    def __init__(self, name: str = "announcement_composite_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "公告综合过滤器")
        self.lookback_days = self.params.get("lookback_days", 7)
        self.require_performance = self.params.get("require_performance", True)
        self.require_major_event = self.params.get("require_major_event", False)
        self.require_equity_change = self.params.get("require_equity_change", False)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """根据综合公告条件过滤股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
        
        all_codes = set()
        
        # 业绩预告
        if self.require_performance:
            forecast_df = self.announcement_fetcher.fetch_performance_forecasts(start_date, end_date)
            if not forecast_df.empty and 'code' in forecast_df.columns:
                forecast_codes = set(forecast_df['code'].unique().tolist())
                all_codes.update(forecast_codes)
                self.logger.info(f"业绩预告: {len(forecast_codes)} 只股票")
        
        # 重大事项
        if self.require_major_event:
            events_df = self.announcement_fetcher.fetch_major_events(start_date, end_date)
            if not events_df.empty and 'code' in events_df.columns:
                event_codes = set(events_df['code'].unique().tolist())
                all_codes.update(event_codes)
                self.logger.info(f"重大事项: {len(event_codes)} 只股票")
        
        # 股权变动
        if self.require_equity_change:
            changes_df = self.announcement_fetcher.fetch_equity_changes(start_date, end_date)
            if not changes_df.empty and 'code' in changes_df.columns:
                change_codes = set(changes_df['code'].unique().tolist())
                all_codes.update(change_codes)
                self.logger.info(f"股权变动: {len(change_codes)} 只股票")
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(list(all_codes)))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"公告综合过滤器: 从 {len(stock_list)} 只中筛选出 {len(filtered)} 只"
            )
        
        return filtered


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 公告事件选股过滤器")
    print("=" * 60)
    
    # 创建测试数据
    test_stocks = pl.DataFrame({
        'code': ['000001', '000002', '600000', '000858', '600519'],
        'name': ['平安银行', '万科A', '浦发银行', '五粮液', '贵州茅台'],
    })
    
    print(f"\n测试股票列表: {len(test_stocks)} 只")
    print(test_stocks)
    
    # 测试业绩预告过滤器
    print("\n1. 测试业绩预告过滤器:")
    pf_filter = PerformanceForecastFilter(params={"forecast_types": ["预增", "扭亏"]})
    filtered = pf_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试重大事项过滤器
    print("\n2. 测试重大事项过滤器:")
    me_filter = MajorEventFilter(params={"event_types": ["并购重组", "股权激励"]})
    filtered = me_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试公告综合过滤器
    print("\n3. 测试公告综合过滤器:")
    ac_filter = AnnouncementCompositeFilter(params={
        "require_performance": True,
        "require_major_event": True
    })
    filtered = ac_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
