#!/usr/bin/env python3
"""
市场行为选股过滤器

基于市场行为数据进行选股过滤:
- 龙虎榜: 机构买入、游资参与
- 资金流向: 主力净流入、大单买入
- 北向资金: 北向持股、北向净流入

使用示例:
    filter = DragonTigerFilter(params={"min_institution_net": 1000})
    filtered_stocks = filter.filter(stock_list)
"""
import polars as pl
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from filters.base_filter import BaseFilter, FilterRegistry
from services.data_service.fetchers.market_behavior import (
    DragonTigerFetcher,
    MoneyFlowFetcher,
)

from core.logger import get_logger

logger = get_logger(__name__)


def register_filter(name: str):
    """过滤器注册装饰器"""
    def decorator(cls):
        FilterRegistry.register(name, cls)
        return cls
    return decorator


class MarketBehaviorFilterBase(BaseFilter):
    """市场行为过滤器基类"""
    
    def __init__(
        self,
        name: str,
        params: Dict[str, Any] = None,
        description: str = ""
    ):
        super().__init__(name, params, description)
        self.dragon_tiger_fetcher = DragonTigerFetcher()
        self.money_flow_fetcher = MoneyFlowFetcher()


@register_filter("dragon_tiger_filter")
class DragonTigerFilter(MarketBehaviorFilterBase):
    """
    龙虎榜过滤器
    
    筛选龙虎榜上榜股票:
    - 机构净买入 >= 阈值
    - 游资参与度
    - 上榜原因筛选
    """
    
    def __init__(self, name: str = "dragon_tiger_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "龙虎榜过滤器")
        self.min_institution_net = self.params.get("min_institution_net", 500)  # 机构净买入(万元)
        self.trade_date = self.params.get("trade_date", None)  # 指定日期
        self.lookback_days = self.params.get("lookback_days", 1)  # 回溯天数
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤龙虎榜股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 获取龙虎榜数据
        dragon_tiger_df = self.dragon_tiger_fetcher.fetch_daily_list(self.trade_date)
        
        if dragon_tiger_df.empty:
            self.logger.warning("龙虎榜数据为空")
            return stock_list
        
        # 获取机构专用数据
        if self.trade_date:
            start_date = self.trade_date
            end_date = self.trade_date
        else:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y%m%d')
        
        institution_df = self.dragon_tiger_fetcher.fetch_institution_trading(start_date, end_date)
        
        # 合并数据
        if not institution_df.empty and 'code' in institution_df.columns:
            # 筛选机构净买入达标的股票
            filtered_inst = institution_df[institution_df['institution_net'] >= self.min_institution_net]
            dragon_tiger_codes = filtered_inst['code'].unique().tolist()
        else:
            # 如果没有机构数据,使用所有龙虎榜股票
            dragon_tiger_codes = dragon_tiger_df['code'].unique().tolist()
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(dragon_tiger_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"龙虎榜过滤器: 筛选出 {len(filtered)} 只机构净买入>={self.min_institution_net}万的股票"
            )
        
        return filtered


@register_filter("money_flow_filter")
class MoneyFlowFilter(MarketBehaviorFilterBase):
    """
    资金流向过滤器
    
    筛选资金流入股票:
    - 主力净流入 >= 阈值
    - 主力净流入占比 >= 阈值
    """
    
    def __init__(self, name: str = "money_flow_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "资金流向过滤器")
        self.min_main_net = self.params.get("min_main_net", 1000)  # 主力净流入(万元)
        self.min_main_ratio = self.params.get("min_main_ratio", 5.0)  # 主力净流入占比(%)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤资金流入股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        filtered_codes = []
        
        for code in codes:
            try:
                # 获取资金流向
                money_flow = self.money_flow_fetcher.fetch_stock_money_flow(code)
                
                if money_flow is None:
                    continue
                
                # 检查主力净流入
                main_net = money_flow.main_net_flow or 0
                main_ratio = money_flow.main_net_ratio or 0
                
                if main_net >= self.min_main_net and main_ratio >= self.min_main_ratio:
                    filtered_codes.append(code)
                    
            except Exception as e:
                self.logger.debug(f"{code} 资金流向获取失败: {e}")
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"资金流向过滤器: 筛选出 {len(filtered)} 只主力净流入>={self.min_main_net}万的股票"
            )
        
        return filtered


@register_filter("northbound_filter")
class NorthboundFilter(MarketBehaviorFilterBase):
    """
    北向资金过滤器
    
    筛选北向资金持股股票:
    - 北向持股比例 >= 阈值
    - 北向资金净流入
    """
    
    def __init__(self, name: str = "northbound_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "北向资金过滤器")
        self.min_hold_ratio = self.params.get("min_hold_ratio", 1.0)  # 最低持股比例(%)
        self.min_increase = self.params.get("min_increase", 0)  # 最低增持比例(%)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤北向资金持股股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        # 获取北向持股数据
        holdings_df = self.money_flow_fetcher.fetch_northbound_holdings()
        
        if holdings_df.empty:
            self.logger.warning("北向持股数据为空")
            return stock_list
        
        # 筛选北向持股比例达标的股票
        if 'hold_ratio' in holdings_df.columns:
            filtered_holdings = holdings_df[holdings_df['hold_ratio'] >= self.min_hold_ratio]
            northbound_codes = filtered_holdings['code'].unique().tolist()
        else:
            northbound_codes = holdings_df['code'].unique().tolist()
        
        # 过滤股票列表
        filtered = stock_list.filter(pl.col('code').is_in(northbound_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"北向资金过滤器: 筛选出 {len(filtered)} 只北向持股>={self.min_hold_ratio}%的股票"
            )
        
        return filtered


@register_filter("main_force_filter")
class MainForceFilter(MarketBehaviorFilterBase):
    """
    主力资金综合过滤器
    
    综合多个主力资金指标:
    - 龙虎榜机构买入
    - 资金流向主力净流入
    - 北向资金持股
    """
    
    def __init__(self, name: str = "main_force_filter", params: Dict[str, Any] = None):
        super().__init__(name, params, "主力资金综合过滤器")
        self.min_institution_net = self.params.get("min_institution_net", 500)
        self.min_main_net = self.params.get("min_main_net", 1000)
        self.min_main_ratio = self.params.get("min_main_ratio", 5.0)
        self.require_northbound = self.params.get("require_northbound", False)
    
    def filter(self, stock_list: pl.DataFrame) -> pl.DataFrame:
        """过滤主力资金关注的股票"""
        if not self.enabled:
            return stock_list
        
        if stock_list.is_empty():
            return stock_list
        
        codes = stock_list['code'].to_list()
        
        # 获取龙虎榜数据
        dragon_tiger_df = self.dragon_tiger_fetcher.fetch_daily_list()
        dragon_tiger_codes = set()
        
        if not dragon_tiger_df.empty:
            # 获取机构买入数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
            institution_df = self.dragon_tiger_fetcher.fetch_institution_trading(start_date, end_date)
            
            if not institution_df.empty:
                filtered_inst = institution_df[institution_df['institution_net'] >= self.min_institution_net]
                dragon_tiger_codes = set(filtered_inst['code'].unique().tolist())
        
        # 获取资金流向数据
        money_flow_codes = set()
        for code in codes[:100]:  # 限制检查数量
            try:
                money_flow = self.money_flow_fetcher.fetch_stock_money_flow(code)
                if money_flow:
                    main_net = money_flow.main_net_flow or 0
                    main_ratio = money_flow.main_net_ratio or 0
                    if main_net >= self.min_main_net and main_ratio >= self.min_main_ratio:
                        money_flow_codes.add(code)
            except:
                pass
        
        # 获取北向数据
        northbound_codes = set()
        if self.require_northbound:
            holdings_df = self.money_flow_fetcher.fetch_northbound_holdings()
            if not holdings_df.empty and 'hold_ratio' in holdings_df.columns:
                filtered_holdings = holdings_df[holdings_df['hold_ratio'] >= 1.0]
                northbound_codes = set(filtered_holdings['code'].unique().tolist())
        
        # 综合筛选: 满足龙虎榜或资金流向条件
        if self.require_northbound:
            # 需要同时满足北向条件
            filtered_codes = list(
                (dragon_tiger_codes | money_flow_codes) & northbound_codes
            )
        else:
            filtered_codes = list(dragon_tiger_codes | money_flow_codes)
        
        filtered = stock_list.filter(pl.col('code').is_in(filtered_codes))
        
        removed_count = len(stock_list) - len(filtered)
        if removed_count > 0:
            self.logger.info(
                f"主力资金过滤器: 从 {len(stock_list)} 只中筛选出 {len(filtered)} 只"
            )
        
        return filtered


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 市场行为选股过滤器")
    print("=" * 60)
    
    # 创建测试数据
    test_stocks = pl.DataFrame({
        'code': ['000001', '000002', '600000', '000858', '600519'],
        'name': ['平安银行', '万科A', '浦发银行', '五粮液', '贵州茅台'],
    })
    
    print(f"\n测试股票列表: {len(test_stocks)} 只")
    print(test_stocks)
    
    # 测试龙虎榜过滤器
    print("\n1. 测试龙虎榜过滤器:")
    dt_filter = DragonTigerFilter(params={"min_institution_net": 500})
    filtered = dt_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试资金流向过滤器
    print("\n2. 测试资金流向过滤器:")
    mf_filter = MoneyFlowFilter(params={"min_main_net": 1000})
    filtered = mf_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
    
    # 测试主力资金综合过滤器
    print("\n3. 测试主力资金综合过滤器:")
    main_filter = MainForceFilter(params={"min_institution_net": 500, "min_main_net": 1000})
    filtered = main_filter.filter(test_stocks)
    print(f"过滤后: {len(filtered)} 只")
