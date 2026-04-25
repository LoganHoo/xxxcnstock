# 数据服务API文档

## 目录

1. [统一数据服务API](#统一数据服务api)
2. [财务数据API](#财务数据api)
3. [市场行为数据API](#市场行为数据api)
4. [公告数据API](#公告数据api)
5. [过滤器API](#过滤器api)
6. [任务调度API](#任务调度api)
7. [监控API](#监控api)

---

## 统一数据服务API

### UnifiedDataService

统一数据服务入口，提供所有数据类型的访问接口。

#### 初始化

```python
from services.data_service.unified_data_service import UnifiedDataService

service = UnifiedDataService(
    tushare_token: str = None,  # Tushare API Token
    data_dir: str = None         # 数据存储目录
)
```

#### 股票列表

```python
# 获取股票列表
stocks = service.get_stock_list()
# Returns: DataFrame[code, name, industry, ...]
```

#### 财务数据

```python
# 获取完整财务数据
financial_data = service.get_all_financial_data(
    code: str           # 股票代码
)
# Returns: FinancialData 对象

# 获取财务指标
indicators = service.get_financial_indicators(
    code: str,          # 股票代码
    as_of_date: str     # 指定日期 (可选)
)
# Returns: Dict[str, float]

# 获取资产负债表
balance_sheet = service.get_balance_sheet(
    code: str,          # 股票代码
    as_of_date: str     # 指定日期 (可选)
)
# Returns: DataFrame

# 获取利润表
income_statement = service.get_income_statement(
    code: str,
    as_of_date: str
)
# Returns: DataFrame

# 获取现金流量表
cash_flow = service.get_cash_flow(
    code: str,
    as_of_date: str
)
# Returns: DataFrame
```

#### 市场行为数据

```python
# 获取龙虎榜数据
dragon_tiger = service.get_dragon_tiger(
    trade_date: str     # 交易日期 (YYYYMMDD)
)
# Returns: DataFrame

# 获取资金流向
money_flow = service.get_money_flow(
    code: str           # 股票代码
)
# Returns: MoneyFlowData 对象

# 获取板块资金流向
sector_flow = service.get_sector_money_flow(
    sector_type: str    # 'industry' 或 'concept'
)
# Returns: DataFrame

# 获取北向资金
northbound = service.get_northbound_money_flow(
    days: int = 30      # 获取天数
)
# Returns: DataFrame
```

#### 公告数据

```python
# 获取公司公告
announcements = service.get_announcements(
    code: str,          # 股票代码
    days: int = 7       # 回溯天数
)
# Returns: DataFrame

# 获取重大事项
major_events = service.get_major_events(
    days: int = 3
)
# Returns: DataFrame

# 获取业绩预告
forecasts = service.get_performance_forecasts(
    days: int = 7
)
# Returns: DataFrame

# 获取股权变动
equity_changes = service.get_equity_changes(
    days: int = 7
)
# Returns: DataFrame
```

---

## 财务数据API

### BalanceSheetFetcher

```python
from services.data_service.fetchers.financial import BalanceSheetFetcher

fetcher = BalanceSheetFetcher()

# 获取单只股票资产负债表
df = fetcher.fetch_stock_balance_sheet(
    code: str,
    years: int = 3
)
# Returns: DataFrame

# 批量获取
results = fetcher.fetch_batch_balance_sheet(
    codes: List[str],
    max_workers: int = 4
)
# Returns: Dict[str, DataFrame]
```

### IncomeStatementFetcher

```python
from services.data_service.fetchers.financial import IncomeStatementFetcher

fetcher = IncomeStatementFetcher()

df = fetcher.fetch_stock_income_statement(
    code: str,
    years: int = 3
)
# Returns: DataFrame
```

### CashFlowFetcher

```python
from services.data_service.fetchers.financial import CashFlowFetcher

fetcher = CashFlowFetcher()

df = fetcher.fetch_stock_cash_flow(
    code: str,
    years: int = 3
)
# Returns: DataFrame
```

### FinancialIndicatorCalculator

```python
from services.data_service.processors.financial_indicator_calculator import FinancialIndicatorCalculator

calculator = FinancialIndicatorCalculator()

# 计算所有财务指标
indicators = calculator.calculate_all_indicators(
    code: str,
    balance_sheet: DataFrame,
    income_statement: DataFrame,
    cash_flow: DataFrame
)
# Returns: DataFrame

# 获取最新指标
latest = calculator.get_latest_indicators(indicators_df)
# Returns: Dict[str, float]
```

---

## 市场行为数据API

### DragonTigerFetcher

```python
from services.data_service.fetchers.market_behavior import DragonTigerFetcher

fetcher = DragonTigerFetcher()

# 获取龙虎榜详情
df = fetcher.fetch_dragon_tiger_detail(
    trade_date: str     # 交易日期
)
# Returns: DataFrame

# 获取机构交易明细
df = fetcher.fetch_institution_trading(
    start_date: str,
    end_date: str
)
# Returns: DataFrame
```

### MoneyFlowFetcher

```python
from services.data_service.fetchers.market_behavior import MoneyFlowFetcher

fetcher = MoneyFlowFetcher()

# 获取个股资金流向
df = fetcher.fetch_stock_money_flow(
    code: str,
    days: int = 30
)
# Returns: DataFrame

# 获取板块资金流向
df = fetcher.fetch_sector_money_flow(
    sector_type: str    # 'industry' 或 'concept'
)
# Returns: DataFrame

# 获取个股资金流向汇总
summary = fetcher.fetch_stock_money_flow_summary(
    code: str
)
# Returns: MoneyFlowSummary 对象
```

### NorthboundFetcher

```python
from services.data_service.fetchers.market_behavior import NorthboundFetcher

fetcher = NorthboundFetcher()

# 获取北向资金每日数据
df = fetcher.fetch_northbound_daily(
    start_date: str,
    end_date: str
)
# Returns: DataFrame

# 获取北向资金持股
df = fetcher.fetch_northbound_holdings(
    code: str
)
# Returns: DataFrame
```

---

## 公告数据API

### AnnouncementFetcher

```python
from services.data_service.fetchers.announcement import AnnouncementFetcher

fetcher = AnnouncementFetcher()

# 获取公司公告
df = fetcher.fetch_company_announcements(
    code: str,
    start_date: str,
    end_date: str,
    ann_type: str = None    # 公告类型 (可选)
)
# Returns: DataFrame

# 获取重大事项
df = fetcher.fetch_major_events(
    start_date: str,
    end_date: str,
    event_type: str = None  # 事件类型 (可选)
)
# Returns: DataFrame

# 获取业绩预告
df = fetcher.fetch_performance_forecasts(
    start_date: str,
    end_date: str,
    forecast_type: str = None  # 预告类型 (可选)
)
# Returns: DataFrame

# 获取股权变动
df = fetcher.fetch_equity_changes(
    start_date: str,
    end_date: str,
    change_type: str = None    # 变动类型 (可选)
)
# Returns: DataFrame
```

---

## 过滤器API

### FilterEngine

```python
from filters import FilterEngine

engine = FilterEngine()

# 列出所有过滤器
filters = engine.list_filters()
# Returns: List[Dict]

# 应用单个过滤器
result = engine.apply_filter(
    stock_list: DataFrame,
    filter_name: str
)
# Returns: DataFrame

# 应用多个过滤器
result = engine.apply_filters(
    stock_list: DataFrame,
    filter_names: List[str]
)
# Returns: DataFrame

# 获取过滤器信息
info = engine.get_filter_info(filter_name: str)
# Returns: Dict
```

### 财务过滤器

```python
from filters import (
    ROEFilter,
    ProfitabilityFilter,
    GrowthFilter,
    CashFlowFilter,
    FinancialCompositeFilter
)

# ROE过滤器
filter = ROEFilter(params={
    'min_roe': 15.0,
    'max_roe': 100.0
})

# 盈利能力过滤器
filter = ProfitabilityFilter(params={
    'min_gross_margin': 30.0,
    'min_net_margin': 10.0
})

# 成长能力过滤器
filter = GrowthFilter(params={
    'min_revenue_growth': 15.0,
    'min_profit_growth': 20.0
})

# 现金流过滤器
filter = CashFlowFilter(params={
    'min_operating_cash_flow': 0,
    'require_positive': True
})

# 财务综合过滤器
filter = FinancialCompositeFilter(params={
    'min_score': 60.0,
    'top_n': 50
})
```

### 市场行为过滤器

```python
from filters import (
    DragonTigerFilter,
    MoneyFlowFilter,
    NorthboundFilter,
    MainForceFilter
)

# 龙虎榜过滤器
filter = DragonTigerFilter(params={
    'min_institution_net': 500,
    'days': 3
})

# 资金流向过滤器
filter = MoneyFlowFilter(params={
    'min_main_net': 1000,
    'min_main_ratio': 5.0,
    'days': 5
})

# 北向资金过滤器
filter = NorthboundFilter(params={
    'min_holding': 1000,
    'min_increase': 0
})

# 主力资金综合过滤器
filter = MainForceFilter(params={
    'min_institution_net': 500,
    'min_main_net': 1000,
    'require_northbound': False
})
```

### 公告过滤器

```python
from filters import (
    PerformanceForecastFilter,
    MajorEventFilter,
    EquityChangeFilter,
    AnnouncementCompositeFilter
)

# 业绩预告过滤器
filter = PerformanceForecastFilter(params={
    'forecast_types': ['预增', '扭亏'],
    'lookback_days': 7
})

# 重大事项过滤器
filter = MajorEventFilter(params={
    'event_types': ['并购重组', '股权激励'],
    'lookback_days': 7
})

# 股权变动过滤器
filter = EquityChangeFilter(params={
    'change_types': ['增持'],
    'min_amount': 1000,
    'lookback_days': 7
})

# 公告综合过滤器
filter = AnnouncementCompositeFilter(params={
    'lookback_days': 7,
    'require_performance': True,
    'require_major_event': False,
    'require_equity_change': False
})
```

---

## 任务调度API

### DailyUpdateTask

```python
from services.data_service.tasks import DailyUpdateTask

task = DailyUpdateTask()

# 运行每日更新
results = task.run_daily_update()
# Returns: Dict

# 运行财务数据更新
results = task.run_financial_update(
    stock_codes: List[str] = None,
    years: int = 3
)
# Returns: Dict

# 运行市场行为数据更新
results = task.run_market_behavior_update(
    days: int = 30
)
# Returns: Dict

# 运行公告数据更新
results = task.run_announcement_update(
    days: int = 7
)
# Returns: Dict
```

### IncrementalUpdateTask

```python
from services.data_service.tasks import IncrementalUpdateTask

task = IncrementalUpdateTask(max_workers=4)

# 运行增量更新
results = task.run_incremental_financial_update(
    stock_codes: List[str] = None,
    years: int = 3,
    resume: bool = True
)
# Returns: Dict

# 检测数据变化
changes = task.detect_changes(
    code: str,
    data_type: str  # 'balance_sheet', 'income_statement', 'cash_flow'
)
# Returns: Dict

# 获取更新摘要
summary = task.get_update_summary()
# Returns: Dict
```

### DataPreheatingTask

```python
from services.data_service.tasks import DataPreheatingTask

task = DataPreheatingTask(max_workers=4)

# 运行数据预热
result = task.run_preheating()
# Returns: Dict

# 获取预热状态
status = task.get_preheat_status()
# Returns: Dict

# 检查是否需要预热
needed = task.is_preheat_needed(max_age_minutes=60)
# Returns: bool
```

---

## 监控API

### DataQualityMonitor

```python
from services.data_service.quality import DataQualityMonitor

monitor = DataQualityMonitor()

# 检查数据新鲜度
result = monitor.check_data_freshness(
    max_age_days: int = 30
)
# Returns: QualityCheckResult

# 检查数据完整性
result = monitor.check_data_completeness()
# Returns: QualityCheckResult

# 检查数据一致性
result = monitor.check_data_consistency(
    sample_codes: List[str] = None
)
# Returns: QualityCheckResult

# 生成质量报告
report = monitor.generate_quality_report()
# Returns: Dict
```

### DataServiceDashboard

```python
from services.data_service.monitoring import DataServiceDashboard

dashboard = DataServiceDashboard()

# 收集指标
metrics = dashboard.collect_metrics()
# Returns: DashboardMetrics

# 生成完整报告
report = dashboard.generate_full_report()
# Returns: Dict

# 打印报告
dashboard.print_report(report: Dict = None)

# 保存报告
filepath = dashboard.save_report(
    report: Dict = None,
    filename: str = None
)
# Returns: str

# 获取历史报告
reports = dashboard.get_historical_reports(days: int = 7)
# Returns: List[Dict]

# 检查告警
alerts = dashboard.check_alerts()
# Returns: List[Dict]

# 打印告警
dashboard.print_alerts()
```

---

## 数据模型

### FinancialData

```python
@dataclass
class FinancialData:
    code: str                           # 股票代码
    balance_sheet: pd.DataFrame         # 资产负债表
    income_statement: pd.DataFrame      # 利润表
    cash_flow: pd.DataFrame             # 现金流量表
    indicators: pd.DataFrame            # 财务指标
```

### MoneyFlowData

```python
@dataclass
class MoneyFlowData:
    code: str
    trade_date: str
    main_inflow: float                  # 主力流入
    main_outflow: float                 # 主力流出
    main_net_flow: float                # 主力净流入
    main_net_ratio: float               # 主力净流入占比
    retail_inflow: float                # 散户流入
    retail_outflow: float               # 散户流出
    retail_net_flow: float              # 散户净流入
```

### QualityCheckResult

```python
@dataclass
class QualityCheckResult:
    check_name: str
    score: float                        # 0-100
    passed: bool
    issues: List[str]
    timestamp: str
```

---

## 错误处理

所有API在出错时会抛出以下异常：

| 异常类型 | 说明 |
|---------|------|
| `DataFetchError` | 数据获取失败 |
| `DataValidationError` | 数据验证失败 |
| `StorageError` | 存储操作失败 |
| `FilterError` | 过滤器执行失败 |

```python
from services.data_service.exceptions import DataFetchError

try:
    data = service.get_financial_indicators("000001")
except DataFetchError as e:
    print(f"数据获取失败: {e}")
```

---

**文档版本**: 1.0  
**最后更新**: 2024年
