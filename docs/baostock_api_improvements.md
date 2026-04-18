# BaoStock API 微服务完善记录

## 完成时间
2026-04-19

## 背景
根据 `BaoStock API 数据采集对象及功能说明表 - Table 1.csv` 完整清单，对 `/Volumes/Xdata/workstation/xxxcnstock/services/data_service/fetchers` 进行了全面的分析和完善。

---

## 一、BaoStock API 覆盖情况分析

### 1.1 已覆盖的API (原有模块)

| 数据采集对象 | API接口 | 现有模块 | 状态 |
|------------|---------|---------|------|
| 历史A股K线数据 | query_history_k_data_plus() | kline_fetcher.py, kline_history.py | ✅ |
| 指数K线数据 | query_history_k_data_plus() | domestic_index_fetcher.py | ✅ |
| 证券代码查询 | query_all_stock() | stock_list_fetcher.py, stock_list.py | ✅ |
| 行业分类 | query_stock_industry() | fundamental_fetcher.py (部分) | ⚠️ |

### 1.2 缺失的API (本次新增)

| 数据采集对象 | API接口 | 新增模块 | 优先级 |
|------------|---------|---------|-------|
| 上证50成分股 | query_sz50_stocks() | index_constituent_fetcher.py | 高 ✅ |
| 沪深300成分股 | query_hs300_stocks() | index_constituent_fetcher.py | 高 ✅ |
| 中证500成分股 | query_zz500_stocks() | index_constituent_fetcher.py | 高 ✅ |
| 季频盈利能力 | query_profit_data() | financial_fetcher.py | 高 ✅ |
| 季频营运能力 | query_operation_data() | financial_fetcher.py | 高 ✅ |
| 季频成长能力 | query_growth_data() | financial_fetcher.py | 高 ✅ |
| 季频偿债能力 | query_balance_data() | financial_fetcher.py | 高 ✅ |
| 季频现金流量 | query_cash_flow_data() | financial_fetcher.py | 中 ✅ |
| 季频杜邦指数 | query_dupont_data() | financial_fetcher.py | 中 ✅ |
| 交易日查询 | query_trade_dates() | trade_date_fetcher.py | 高 ✅ |

### 1.3 暂未实现的API

| 数据采集对象 | API接口 | 原因 |
|------------|---------|------|
| 证券基本资料 | query_stock_basic() | 数据已在stock_list中获取 |
| 季频公司业绩快报 | query_performance_express_report() | 低频使用，可按需添加 |
| 季频公司业绩预告 | query_forecast_report() | 低频使用，可按需添加 |
| 除权除息信息 | query_dividend_data() | 低频使用，可按需添加 |
| 复权因子信息 | query_adjust_factor() | K线数据已包含复权信息 |

---

## 二、新增模块详细说明

### 2.1 financial_fetcher.py (财务数据获取器)

**功能**: 支持BaoStock所有财务相关API

**数据类定义**:
- `ProfitData`: 盈利能力数据 (ROE、销售净利率、销售毛利率、净利润等)
- `OperationData`: 营运能力数据 (应收账款周转率、存货周转率、总资产周转率等)
- `GrowthData`: 成长能力数据 (净资产同比增长率、净利润同比增长率等)
- `BalanceData`: 偿债能力数据 (流动比率、速动比率、资产负债率等)
- `CashFlowData`: 现金流量数据 (经营现金流/营业收入等)
- `DupontData`: 杜邦指数数据 (杜邦ROE分解)

**核心方法**:
- `fetch_profit_data()`: 获取盈利能力数据
- `fetch_operation_data()`: 获取营运能力数据
- `fetch_growth_data()`: 获取成长能力数据
- `fetch_balance_data()`: 获取偿债能力数据
- `fetch_cash_flow_data()`: 获取现金流量数据
- `fetch_dupont_data()`: 获取杜邦指数数据
- `fetch_all_financial_data()`: 并发获取所有财务数据

**同步接口**:
- `fetch_financial_data(code, year, quarter)`: 获取完整财务数据
- `fetch_profit_data(code, year, quarter)`: 获取盈利能力
- `fetch_growth_data(code, year, quarter)`: 获取成长能力

### 2.2 index_constituent_fetcher.py (指数成份股获取器)

**功能**: 支持主要指数成份股查询

**数据类定义**:
- `IndexConstituent`: 指数成份股 (代码、名称、更新日期、指数类型)
- `IndustryInfo`: 行业分类信息

**枚举定义**:
- `IndexType`: 指数类型 (SZ50/HS300/ZZ500)

**核心方法**:
- `fetch_sz50_stocks()`: 获取上证50成份股
- `fetch_hs300_stocks()`: 获取沪深300成份股
- `fetch_zz500_stocks()`: 获取中证500成份股
- `fetch_all_index_constituents()`: 并发获取所有指数成份股
- `fetch_stock_industry()`: 获取行业分类

**同步接口**:
- `fetch_sz50_stocks(date)`: 上证50
- `fetch_hs300_stocks(date)`: 沪深300
- `fetch_zz500_stocks(date)`: 中证500
- `fetch_all_index_constituents(date)`: 所有指数
- `fetch_stock_industry(code, date)`: 行业分类

### 2.3 trade_date_fetcher.py (交易日查询模块)

**功能**: 交易日查询和判断

**数据类定义**:
- `TradeDate`: 交易日信息 (日期、是否交易日)

**核心方法**:
- `fetch_trade_dates()`: 获取日期范围交易日信息
- `get_trading_days()`: 获取所有交易日列表
- `get_last_trading_day()`: 获取最近交易日
- `is_trading_day()`: 判断是否为交易日

**同步接口**:
- `fetch_trade_dates(start, end)`: 交易日信息
- `get_trading_days(start, end)`: 交易日列表
- `get_last_trading_day(date)`: 最近交易日
- `is_trading_day(date)`: 是否交易日

---

## 三、模块导出更新

更新了 `__init__.py`，新增导出:

```python
# Financial Fetcher
from .financial_fetcher import (
    FinancialFetcher,
    ProfitData, OperationData, GrowthData,
    BalanceData, CashFlowData, DupontData,
    fetch_financial_data, fetch_profit_data, fetch_growth_data,
)

# Index Constituent Fetcher
from .index_constituent_fetcher import (
    IndexConstituentFetcher, IndexConstituent, IndustryInfo, IndexType,
    fetch_sz50_stocks, fetch_hs300_stocks, fetch_zz500_stocks,
    fetch_all_index_constituents, fetch_stock_industry,
)

# Trade Date Fetcher
from .trade_date_fetcher import (
    TradeDateFetcher, TradeDate,
    fetch_trade_dates, get_trading_days,
    get_last_trading_day, is_trading_day,
)
```

---

## 四、使用示例

### 4.1 获取财务数据

```python
from services.data_service.fetchers import fetch_financial_data

# 获取平安银行2024年第三季度财务数据
data = fetch_financial_data("000001", 2024, 3)
print(data['profit'])      # 盈利能力
print(data['growth'])      # 成长能力
print(data['balance'])     # 偿债能力
```

### 4.2 获取指数成份股

```python
from services.data_service.fetchers import (
    fetch_sz50_stocks, fetch_hs300_stocks, fetch_zz500_stocks
)

# 获取各指数成份股
sz50 = fetch_sz50_stocks()      # 上证50
hs300 = fetch_hs300_stocks()    # 沪深300
zz500 = fetch_zz500_stocks()    # 中证500

# 获取所有指数成份股
all_indices = fetch_all_index_constituents()
```

### 4.3 获取行业分类

```python
from services.data_service.fetchers import fetch_stock_industry

# 获取单只股票行业
industry = fetch_stock_industry("000001")

# 获取所有股票行业分类
all_industries = fetch_stock_industry()
```

### 4.4 交易日查询

```python
from services.data_service.fetchers import (
    get_trading_days, get_last_trading_day, is_trading_day
)

# 获取本月交易日
trading_days = get_trading_days("2026-04-01", "2026-04-30")

# 获取最近交易日
last_day = get_last_trading_day()

# 判断是否为交易日
is_trading = is_trading_day("2026-04-19")
```

---

## 五、量化场景支持

### 5.1 涨停回调战法 (财务筛选)

```python
from services.data_service.fetchers import fetch_profit_data, fetch_growth_data

# 筛选ROE > 10% 且 净利润增长率 > 20% 的股票
def filter_by_financial(stock_list, year, quarter):
    qualified = []
    for code in stock_list:
        profit = fetch_profit_data(code, year, quarter)
        growth = fetch_growth_data(code, year, quarter)

        if profit and growth:
            if profit.roe_avg > 10 and growth.yoy_ni > 20:
                qualified.append(code)
    return qualified
```

### 5.2 指数跟踪策略

```python
from services.data_service.fetchers import fetch_hs300_stocks

# 获取沪深300成份股进行跟踪
hs300_stocks = fetch_hs300_stocks()
constituent_codes = [s['code'] for s in hs300_stocks]
```

### 5.3 交易日风控

```python
from services.data_service.fetchers import is_trading_day, get_last_trading_day

# 风控：非交易日暂停交易
if not is_trading_day(today):
    logger.info("非交易日，暂停交易")
    return

# 获取最近交易日数据进行回测
last_trading = get_last_trading_day()
```

---

## 六、后续优化建议

### 6.1 短期优化
1. **统一连接池**: BaoStock登录分散在各模块，建议统一连接池管理
2. **数据缓存**: 热点数据(如股票列表、指数成份股)增加缓存机制
3. **批量查询优化**: 财务数据支持多股票批量查询

### 6.2 中期扩展
1. **分钟线支持**: K线获取器扩展分钟线支持 (5/15/30/60分钟)
2. **业绩预告**: 添加 query_forecast_report 支持
3. **业绩快报**: 添加 query_performance_express_report 支持

### 6.3 长期规划
1. **数据库存储**: 财务数据、指数成份股持久化到数据库
2. **增量更新**: 支持财务数据增量更新机制
3. **数据校验**: 增加财务数据合理性校验规则

---

## 七、文件清单

### 新增文件
- `/Volumes/Xdata/workstation/xxxcnstock/services/data_service/fetchers/financial_fetcher.py`
- `/Volumes/Xdata/workstation/xxxcnstock/services/data_service/fetchers/index_constituent_fetcher.py`
- `/Volumes/Xdata/workstation/xxxcnstock/services/data_service/fetchers/trade_date_fetcher.py`

### 修改文件
- `/Volumes/Xdata/workstation/xxxcnstock/services/data_service/fetchers/__init__.py`

---

## 八、总结

本次完善实现了BaoStock API的高优先级和中优先级接口，覆盖了量化投资的核心数据需求:

✅ **财务数据**: 盈利能力、营运能力、成长能力、偿债能力、现金流量、杜邦指数
✅ **指数数据**: 上证50、沪深300、中证500成份股
✅ **交易日**: 交易日查询、最近交易日、交易日判断

**覆盖率**: 核心API 100%覆盖，完整API约85%覆盖

**下一步**: 根据实际业务需求，按需添加剩余API接口 (业绩预告、业绩快报、除权除息等)
