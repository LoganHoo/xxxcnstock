"""
过滤器模块
提供股票筛选和过滤功能
"""
from filters.base_filter import BaseFilter, FilterRegistry, register_filter
from filters.stock_filter import STFilter, NewStockFilter, DelistingFilter
from filters.market_filter import MarketCapFilter, SuspensionFilter, PriceFilter, VolumeFilter
from filters.fundamental_filter import IllegalFilter, PerformanceFilter, MarketCrashFilter
from filters.technical_filter import TrendFilter, MaPositionFilter, MonthlyMaFilter, MacdCrossFilter
from filters.liquidity_filter import (
    VolumeRatioFilter, TurnoverRateFilter,
    VolumeStabilityFilter, ContinuousLowTurnoverFilter
)
from filters.valuation_filter import FloatMarketCapFilter, PriceRangeFilter, ValuationFilter
from filters.pattern_filter import (
    LimitUpTrapFilter, OverHypedFilter,
    PullbackSignalFilter, InstitutionSignalFilter, LimitUpAfterFilter
)
from filters.financial_filter import (
    ROEFilter, ProfitabilityFilter, SolvencyFilter,
    GrowthFilter, CashFlowFilter, FinancialCompositeFilter
)
from filters.market_behavior_filter import (
    DragonTigerFilter, MoneyFlowFilter,
    NorthboundFilter, MainForceFilter
)
from filters.announcement_filter import (
    PerformanceForecastFilter, MajorEventFilter,
    EquityChangeFilter, TradingResumeFilter, AnnouncementCompositeFilter
)
from filters.filter_engine import FilterEngine

__all__ = [
    "BaseFilter",
    "FilterRegistry",
    "register_filter",
    "STFilter",
    "NewStockFilter",
    "DelistingFilter",
    "MarketCapFilter",
    "SuspensionFilter",
    "PriceFilter",
    "VolumeFilter",
    "IllegalFilter",
    "PerformanceFilter",
    "MarketCrashFilter",
    "TrendFilter",
    "MaPositionFilter",
    "MonthlyMaFilter",
    "MacdCrossFilter",
    "VolumeRatioFilter",
    "TurnoverRateFilter",
    "VolumeStabilityFilter",
    "ContinuousLowTurnoverFilter",
    "FloatMarketCapFilter",
    "PriceRangeFilter",
    "ValuationFilter",
    "LimitUpTrapFilter",
    "OverHypedFilter",
    "PullbackSignalFilter",
    "InstitutionSignalFilter",
    "LimitUpAfterFilter",
    # 财务指标过滤器
    "ROEFilter",
    "ProfitabilityFilter",
    "SolvencyFilter",
    "GrowthFilter",
    "CashFlowFilter",
    "FinancialCompositeFilter",
    # 市场行为过滤器
    "DragonTigerFilter",
    "MoneyFlowFilter",
    "NorthboundFilter",
    "MainForceFilter",
    # 公告事件过滤器
    "PerformanceForecastFilter",
    "MajorEventFilter",
    "EquityChangeFilter",
    "TradingResumeFilter",
    "AnnouncementCompositeFilter",
    "FilterEngine",
]
