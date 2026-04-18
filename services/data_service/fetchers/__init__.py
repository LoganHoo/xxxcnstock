#!/usr/bin/env python3
"""
数据获取器模块

提供统一的数据获取接口，包括：
- unified_fetcher: 统一数据获取器，支持主备数据源自动切换
- kline_fetcher: K线数据获取器，支持多进程并行获取
- stock_list_fetcher: 股票列表获取器
- fundamental_fetcher: 基本面数据获取器
- foreign_index_fetcher: 外盘指数获取器（美股、亚洲股指）
- commodity_fetcher: 大宗商品获取器（黄金、原油、美元指数）
- domestic_index_fetcher: 国内指数获取器（上证指数、深证成指等）
- financial_fetcher: 财务数据获取器（盈利能力、营运能力、成长能力、偿债能力、现金流量、杜邦指数）
- index_constituent_fetcher: 指数成份股获取器（上证50、沪深300、中证500）
- trade_date_fetcher: 交易日查询模块
"""

from .unified_fetcher import UnifiedFetcher, get_unified_fetcher, StockFundamental, StockInfo
from .kline_fetcher import (
    fetch_kline_data_parallel,
    fetch_kline_for_stock,
    validate_kline_data,
    save_with_verification
)
from .stock_list_fetcher import (
    fetch_stock_list,
    save_stock_list_to_parquet,
    get_exchange_statistics
)
from .fundamental_fetcher import (
    fetch_valuation_data,
    fetch_valuation_data_parallel,
    fetch_industry_data,
    fetch_fundamental_for_stock
)
from .foreign_index_fetcher import (
    ForeignIndexFetcher,
    ForeignIndexData,
    fetch_foreign_indices,
    fetch_foreign_indices_via_service
)
from .commodity_fetcher import (
    CommodityFetcher,
    CommodityData,
    fetch_commodities,
    fetch_commodities_via_service
)
from .domestic_index_fetcher import (
    DomesticIndexFetcher,
    DomesticIndexData,
    fetch_domestic_indices,
    fetch_domestic_indices_via_service
)
from .financial_fetcher import (
    FinancialFetcher,
    ProfitData,
    OperationData,
    GrowthData,
    BalanceData,
    CashFlowData,
    DupontData,
    fetch_financial_data,
    fetch_profit_data,
    fetch_growth_data,
)
from .index_constituent_fetcher import (
    IndexConstituentFetcher,
    IndexConstituent,
    IndustryInfo,
    IndexType,
    fetch_sz50_stocks,
    fetch_hs300_stocks,
    fetch_zz500_stocks,
    fetch_all_index_constituents,
    fetch_stock_industry,
)
from .trade_date_fetcher import (
    TradeDateFetcher,
    TradeDate,
    fetch_trade_dates,
    get_trading_days,
    get_last_trading_day,
    is_trading_day,
)

__all__ = [
    # Unified Fetcher
    'UnifiedFetcher',
    'get_unified_fetcher',
    'StockFundamental',
    'StockInfo',
    # Kline Fetcher
    'fetch_kline_data_parallel',
    'fetch_kline_for_stock',
    'validate_kline_data',
    'save_with_verification',
    # Stock List Fetcher
    'fetch_stock_list',
    'save_stock_list_to_parquet',
    'get_exchange_statistics',
    # Fundamental Fetcher
    'fetch_valuation_data',
    'fetch_valuation_data_parallel',
    'fetch_industry_data',
    'fetch_fundamental_for_stock',
    # Foreign Index Fetcher
    'ForeignIndexFetcher',
    'ForeignIndexData',
    'fetch_foreign_indices',
    'fetch_foreign_indices_via_service',
    # Commodity Fetcher
    'CommodityFetcher',
    'CommodityData',
    'fetch_commodities',
    'fetch_commodities_via_service',
    # Domestic Index Fetcher
    'DomesticIndexFetcher',
    'DomesticIndexData',
    'fetch_domestic_indices',
    'fetch_domestic_indices_via_service',
    # Financial Fetcher
    'FinancialFetcher',
    'ProfitData',
    'OperationData',
    'GrowthData',
    'BalanceData',
    'CashFlowData',
    'DupontData',
    'fetch_financial_data',
    'fetch_profit_data',
    'fetch_growth_data',
    # Index Constituent Fetcher
    'IndexConstituentFetcher',
    'IndexConstituent',
    'IndustryInfo',
    'IndexType',
    'fetch_sz50_stocks',
    'fetch_hs300_stocks',
    'fetch_zz500_stocks',
    'fetch_all_index_constituents',
    'fetch_stock_industry',
    # Trade Date Fetcher
    'TradeDateFetcher',
    'TradeDate',
    'fetch_trade_dates',
    'get_trading_days',
    'get_last_trading_day',
    'is_trading_day',
]
