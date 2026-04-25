# XCNStock 项目规范文档

## 项目概述

XCNStock 是一个综合性的 A 股量化交易分析和数据流水线系统，专为量化交易研究和策略回测而设计。

**项目类型**: 量化交易分析平台  
**领域**: 中国 A 股市场 (沪深A股)  
**主要目的**: 数据流水线、选股策略和策略回测（仅 SOP，不做实盘交易）

### 项目边界

```
┌─────────────────────────────────────────────────────────────────┐
│                      项目范围界定                                │
├─────────────────────────────────────────────────────────────────┤
│  ✅ 包含的功能                                                   │
│     • A 股历史 K 线数据采集（日线级别）                         │
│     • 数据流水线 SOP 流程                                       │
│     • 多因子选股策略                                            │
│     • 策略回测引擎                                              │
│     • 数据质量监控                                              │
│     • 双调度器架构（Kestra + APScheduler）                      │
├─────────────────────────────────────────────────────────────────┤
│  ❌ 不包含的功能                                                 │
│     • 实盘交易执行                                              │
│     • 实时行情数据存储                                          │
│     • 高频交易支持                                              │
└─────────────────────────────────────────────────────────────────┘
```

### 核心目标

1. **数据完整性**: 确保历史数据的准确性和完整性
2. **策略可回测**: 所有策略必须可验证、可复盘
3. **流程自动化**: 数据采集到报告生成全自动
4. **风险可控**: 严格的数据质量检查和过滤机制

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         XCNStock System Architecture                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Dual-Scheduler Layer                             │   │
│  │  ┌──────────────┐         ┌──────────────┐         ┌─────────────┐  │   │
│  │  │    Kestra    │◄───────►│  APScheduler │◄───────►│    Redis    │  │   │
│  │  │   (Primary)  │  Sync   │   (Backup)   │  State  │   (State)   │  │   │
│  │  └──────────────┘         └──────────────┘         └─────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Workflow Orchestration                          │   │
│  │   Morning Pipeline │ Post-Market Pipeline │ Evening Pipeline        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Core Services Layer                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐   │   │
│  │  │Data Service │ │Stock Service│ │Risk Service │ │Backtest Eng │   │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘   │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐   │   │
│  │  │ML Service   │ │Notify Svc   │ │Analysis Svc │ │Report Svc   │   │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                       Data Processing Layer                          │   │
│  │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐        │   │
│  │   │ Fetchers │──►│Processors│──►│ Filters  │──►│ Factors  │        │   │
│  │   └──────────┘   └──────────┘   └──────────┘   └──────────┘        │   │
│  │   - K-line      - Validation   - Market    - Technical             │   │
│  │   - Financial   - Transform    - Stock     - Volume/Price          │   │
│  │   - Index       - Enrich       - Pattern    - Market               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Storage Layer                                 │   │
│  │   ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────┐  │   │
│  │   │   Parquet   │   │   SQLite    │   │        DataHub          │  │   │
│  │   │  (Primary)  │   │  (Reports)  │   │    (Metadata/Lineage)   │  │   │
│  │   └─────────────┘   └─────────────┘   └─────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 技术栈

### 核心技术

| 类别 | 技术 | 版本 | 用途 |
|------|------|------|------|
| **编程语言** | Python | 3.11+ | 主要开发语言 |
| **数据处理** | Polars | >=0.20.0 | 高性能 DataFrame 操作（比 Pandas 快 10-100 倍）|
| **数据处理** | DuckDB | >=0.10.0 | 嵌入式分析 SQL 数据库 |
| **数据存储** | Parquet | - | 列式存储格式（高效压缩、快速查询）|
| **数据存储** | SQLite | 3.x | 报告和元数据存储 |
| **缓存** | Redis | >=4.5.0 | 状态管理和缓存 |
| **Web 框架** | FastAPI | >=0.100.0 | 现代异步 Web 框架 |
| **服务器** | Uvicorn | >=0.23.0 | ASGI 服务器 |
| **HTTP 客户端** | HTTPX | >=0.24.0 | 异步 HTTP 请求 |

### 工作流与调度

| 组件 | 技术 | 角色 |
|------|------|------|
| **主调度器** | Kestra | 主要工作流编排（YAML 定义）|
| **备用调度器** | APScheduler | 备用任务调度（Python 定义）|
| **状态同步** | Redis | 跨调度器状态管理 |
| **消息队列** | Kafka | 事件流处理 |

### 数据源

| 数据源 | 类型 | 提供数据 |
|--------|------|----------|
| **Tushare** | API | K 线、基本面、市场数据 |
| **AKShare** | API | 替代市场数据源 |
| **Baostock** | API | 历史 K 线数据 |

### 数据科学与分析

| 技术 | 用途 |
|------|------|
| **Pandas** | >=2.0.0 - 数据处理（兼容层）|
| **NumPy** | >=1.24.0 - 数值计算 |
| **PyArrow** | >=14.0.0 - Parquet 文件处理 |

### 配置与工具

| 技术 | 用途 |
|------|------|
| **Pydantic** | >=2.0.0 - 数据验证和配置管理 |
| **PyYAML** | >=6.0 - YAML 配置解析 |
| **python-dotenv** | >=1.0.0 - 环境变量管理 |
| **Jinja2** | >=3.0.0 - 模板引擎 |
| **Loguru** | >=0.7.0 - 日志记录 |
| **tqdm** | >=4.65.0 - 进度条显示 |
| **psutil** | >=5.9.0 - 系统监控 |

### 数据库

| 技术 | 用途 |
|------|------|
| **SQLAlchemy** | >=2.0.0 - ORM 框架 |
| **PyMySQL** | >=1.1.0 - MySQL 连接 |

### 测试与质量

| 工具 | 用途 |
|------|------|
| **pytest** | >=7.0.0 - 单元和集成测试 |
| **pytest-asyncio** | >=0.21.0 - 异步测试支持 |
| **flake8** | 代码风格检查 |
| **mypy** | 类型检查 |

---

## 项目结构

```
xxxcnstock/
├── openspec/                    # OpenSpec configuration
│   ├── config.yaml             # OpenSpec settings
│   ├── GUIDE.md                # Usage guide
│   └── project.md              # This file
│
├── config/                      # Configuration files
│   ├── main.yaml               # Main strategy config
│   ├── cron_tasks.yaml         # Scheduled tasks
│   ├── dual_scheduler.yaml     # Scheduler configuration
│   ├── factors/                # Factor configurations
│   ├── filters/                # Filter configurations
│   └── strategies/             # Strategy configurations
│
├── core/                        # Core business logic
│   ├── data_loader.py          # Data loading utilities
│   ├── factor_engine.py        # Factor calculation engine
│   ├── strategy_engine.py      # Strategy execution
│   ├── backtest_engine.py      # Backtesting engine
│   ├── market_guardian.py      # Market hours validation
│   ├── pipeline_state.py       # Pipeline state management
│   └── indicators/             # Technical indicators
│
├── factors/                     # Factor implementations
│   ├── technical/              # Technical factors (MACD, RSI, etc.)
│   ├── volume_price/           # Volume/Price factors
│   └── market/                 # Market factors
│
├── filters/                     # Stock filters
│   ├── market_filter.py        # Market condition filters
│   ├── stock_filter.py         # Stock-specific filters
│   └── technical_filter.py     # Technical filters
│
├── services/                    # Service layer
│   ├── data_service/           # Data collection & management
│   ├── stock_service/          # Stock selection service
│   ├── backtest_service/       # Backtesting service
│   ├── analysis_service/       # Analysis services
│   ├── risk_service/           # Risk management
│   ├── ml_service/             # Machine learning
│   └── notify_service/         # Notification service
│
├── workflows/                   # Workflow definitions
│   ├── data_collection_workflow.py
│   ├── stock_selection_workflow.py
│   ├── backtest_workflow.py
│   └── daily_operation_workflow.py
│
├── kestra/                      # Kestra workflow files
│   ├── flows/                  # YAML workflow definitions
│   ├── deploy.py               # Deployment script
│   └── monitor.py              # Monitoring script
│
├── scripts/                     # Utility scripts
│   ├── data_collect.py         # Data collection entry
│   ├── dual_scheduler_manager.py
│   └── scheduler_monitor.py
│
├── tests/                       # Test suite
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── performance/            # Performance tests
│
├── web/                         # Web application
│   ├── app.py                  # Flask application
│   └── app_v2.py               # Enhanced version
│
├── gateway/                     # API Gateway
│   └── main.py
│
├── data/                        # Data storage (gitignored)
│   ├── kline/                  # K-line Parquet files
│   └── stock_list.parquet
│
├── logs/                        # Log files (gitignored)
├── docs/                        # Documentation
└── docker-compose.yml          # Docker configuration
```

---

## 核心业务流程

### 1. 每日数据流水线

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  08:30      │───►│  09:00      │───►│  15:30      │───►│  20:00      │
│  盘前预热    │    │  开盘       │    │  收盘       │    │  晚间分析   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ 加载昨日    │    │ 实时监控    │    │ 采集日K     │    │ 计算因子    │
│ 数据预热    │    │ 市场状态    │    │ 数据        │    │ 生成报告    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

**关键时间点**:
- **08:30** - 盘前预热：加载前一日数据，预计算指标
- **09:00-15:00** - 交易时段：实时监控，禁止数据采集
- **15:30** - 收盘后：采集当日完整K线数据
- **20:00** - 晚间：因子计算，选股，生成报告

### 2. 选股流程

```
原始股票池 (5000+ 只)
         │
         ▼
┌─────────────────┐
│  市场过滤器      │  剔除停牌、ST、退市股票
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  质量过滤器      │  数据新鲜度、流动性检查
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  因子评分       │  多因子评分模型
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  策略过滤器      │  应用策略特定规则
└────────┬────────┘
         │
         ▼
   最终选股结果 (20-50 只)
```

**过滤层级**:
1. **市场过滤**: 剔除停牌、ST、*ST、退市股票
2. **质量过滤**: 数据新鲜度(<30天)、流动性检查
3. **技术过滤**: 均线位置、MACD、趋势等
4. **因子评分**: 量价因子、技术因子、市场因子加权评分
5. **策略过滤**: 根据具体策略规则精选

### 3. 回测流程

```
历史数据 (3年)
         │
         ▼
┌─────────────────┐
│   数据加载       │  加载K线，复权处理
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   信号生成       │  计算因子，生成交易信号
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   交易模拟       │  执行交易，考虑滑点和成本
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   结果分析       │  收益率、夏普比率、最大回撤
└─────────────────┘
```

**回测参数**:
- 初始资金: 1,000,000
- 交易成本: 0.03%
- 滑点: 0.1%
- 回测周期: 2023-01-01 至 2023-12-31

---

## 数据管理

### 数据存储格式

| 数据类型 | 格式 | 位置 | 更新频率 | 大小估算 |
|----------|------|------|----------|----------|
| K 线数据 | Parquet | `data/kline/*.parquet` | 每日收盘后 | ~20KB/股票，总计 ~120MB |
| 股票列表 | Parquet | `data/stock_list.parquet` | 每日 | ~500KB |
| 报告数据 | SQLite | `data/reports.db` | 每次选股 | 动态增长 |
| 元数据 | DataHub | 外部服务 | 实时 | - |

### Parquet 数据字段

```python
# K 线数据字段结构
{
    "code": str,           # 股票代码
    "trade_date": date,    # 交易日期
    "open": float,         # 开盘价
    "close": float,        # 收盘价
    "high": float,         # 最高价
    "low": float,          # 最低价
    "volume": int,         # 成交量
    "amount": float,       # 成交额
    "turnover": float,     # 换手率
}
```

### 数据采集规则

**铁律：交易日盘中（9:30-15:00）禁止采集当日 K 线数据**

```
┌─────────────────────────────────────────────────────────────┐
│                    数据采集时间表                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ✅ 允许采集：                                                │
│     • 交易日 15:30 后 → 采集当日收盘数据                     │
│     • 非交易日任何时间 → 采集历史数据                        │
│     • 强制指定 --date 历史日期 → 采集指定日期                │
│                                                              │
│  ❌ 禁止采集：                                                │
│     • 交易日 9:30-15:00 采集当日数据（数据不完整）           │
│     • 交易日 15:00-15:30（数据可能未稳定）                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**关键区分原则**：
- **当日数据**：数据日期 == 今天 → 必须收盘后采集
- **历史数据**：数据日期 < 今天 → 任何时间都可采集

| 场景 | 当前时间 | 目标数据日期 | 是否允许 | 说明 |
|------|----------|--------------|----------|------|
| 周五盘中采集周五数据 | 周五 10:00 | 2026-04-17 | ❌ 禁止 | 盘中数据不完整 |
| 周五收盘后采集周五数据 | 周五 15:30 | 2026-04-17 | ✅ 允许 | 正常采集 |
| 周六采集周五数据 | 周六 10:00 | 2026-04-17 | ✅ 允许 | 非交易日可采历史 |
| 盘中强制补采上周数据 | 周五 10:00 | 2026-04-10 | ✅ 允许 | 历史数据不受限制 |

**技术实现**：
```python
# core/market_guardian.py
enforce_market_closed(target_date=today)      # 当日数据盘中会退出
enforce_market_closed(target_date=history_date)  # 历史数据随时可采
```

### 数据质量检查

| 检查项 | 要求 | 实现方式 |
|--------|------|----------|
| **新鲜度检查** | 最新数据日期必须在 30 天内 | `data_freshness_filter` |
| **完整性检查** | 所有必需字段必须存在 | `completeness_validator` |
| **有效性检查** | 价格/成交量在合理范围内 | `range_validator` |
| **退市检测** | 名称含"退市"关键词 | `delisting_filter` |
| **ST 检测** | 名称含"ST"关键词 | `st_filter` |
| **停牌检测** | 成交量 > 0 为必要条件 | `suspension_filter` |

**配置位置**：
- 过滤器配置：`config/filters/fund_behavior_filters.yaml`
- 过滤器实现：`filters/market_filter.py` → `DataFreshnessFilter`

**强制要求**：
1. 所有选股流程必须在输出前应用数据新鲜度检查
2. `scan_mainforce_signals()` 函数新增 `max_data_age_days` 参数，默认 30 天
3. 数据过旧的股票不得出现在任何推荐报告中

---

## 配置规范

### YAML 配置结构

```yaml
# 标准配置格式
component_name:
  enabled: true/false
  description: "人类可读的描述"
  version: "1.0.0"
  
  params:
    key1: value1
    key2: value2
  
  rules:
    - name: rule_name
      condition: "expression"
      action: "action_name"
```

### 配置文件层级

```
config/
├── main.yaml                    # 主策略配置
├── cron_tasks.yaml             # 定时任务配置
├── dual_scheduler.yaml         # 双调度器配置
├── datasource.yaml             # 数据源配置
├── factors_config.yaml         # 因子总配置
├── filters_config.yaml         # 过滤器总配置
├── factors/                    # 因子详细配置
│   ├── technical/              # 技术因子
│   ├── volume_price/           # 量价因子
│   └── market/                 # 市场因子
├── filters/                    # 过滤器详细配置
│   ├── market/                 # 市场过滤器
│   ├── stock/                  # 股票过滤器
│   ├── technical/              # 技术过滤器
│   ├── pattern/                # 形态过滤器
│   ├── liquidity/              # 流动性过滤器
│   ├── valuation/              # 估值过滤器
│   └── fundamental/            # 基本面过滤器
└── strategies/                 # 策略配置
    ├── fund_behavior_config.yaml
    ├── multi_factor.yaml
    └── champion.yaml
```

### 环境变量

| 变量名 | 用途 | 示例 |
|--------|------|------|
| `TUSHARE_TOKEN` | Tushare API 访问令牌 | `your_token_here` |
| `REDIS_HOST` | Redis 连接地址 | `localhost` |
| `REDIS_PORT` | Redis 端口 | `6379` |
| `REDIS_PASSWORD` | Redis 密码 | `password` |
| `KESTRA_URL` | Kestra API 端点 | `http://localhost:8080` |
| `DATA_PATH` | 数据存储路径 | `./data` |
| `LOG_LEVEL` | 日志级别 | `INFO` |
| `TZ` | 时区 | `Asia/Shanghai` |

---

## 编码规范

### Python 风格指南

本项目遵循 **PEP 8** 规范，并强制执行 `flake8` 检查。

#### 1. 类型提示 (Type Hints)

所有函数参数和返回值必须添加类型提示：

```python
from typing import Dict, List, Optional, Union
import polars as pl

def calculate_factor(
    data: pl.DataFrame, 
    window: int = 20,
    factor_name: Optional[str] = None
) -> pl.Series:
    """计算技术指标因子。
    
    Args:
        data: 包含 OHLCV 数据的 DataFrame
        window: 计算窗口大小
        factor_name: 因子名称（可选）
        
    Returns:
        计算得到的因子序列
        
    Raises:
        ValueError: 当输入数据格式不正确时
    """
    if data.is_empty():
        raise ValueError("输入数据不能为空")
    
    # 实现逻辑
    return result
```

#### 2. 文档字符串 (Docstrings)

使用 **Google Style** 文档字符串：

```python
def process_stock_data(
    codes: List[str],
    start_date: str,
    end_date: str
) -> Dict[str, pl.DataFrame]:
    """处理多只股票的历史数据。
    
    该函数会并行获取多只股票的数据，并进行标准化处理。
    
    Args:
        codes: 股票代码列表，如 ['000001.SZ', '600000.SH']
        start_date: 开始日期，格式 'YYYY-MM-DD'
        end_date: 结束日期，格式 'YYYY-MM-DD'
        
    Returns:
        以股票代码为键，DataFrame 为值的字典
        
    Raises:
        ValueError: 当日期格式不正确时
        DataFetchError: 当数据获取失败时
        
    Example:
        >>> data = process_stock_data(
        ...     codes=['000001.SZ'],
        ...     start_date='2024-01-01',
        ...     end_date='2024-12-31'
        ... )
        >>> print(len(data))
        1
    """
```

#### 3. 命名规范

| 类型 | 规范 | 示例 |
|------|------|------|
| **函数** | `snake_case` | `calculate_factor`, `get_stock_data` |
| **变量** | `snake_case` | `stock_list`, `data_frame` |
| **类** | `PascalCase` | `DataLoader`, `FactorEngine` |
| **常量** | `UPPER_CASE` | `MAX_STOCKS`, `DEFAULT_WINDOW` |
| **私有方法** | `_` 前缀 | `_validate_input`, `_process_raw` |
| **模块** | `snake_case` | `data_loader.py`, `market_filter.py` |

#### 4. 错误处理

```python
import logging
from loguru import logger

# ✅ 好的做法：使用具体异常
def fetch_data(code: str) -> pl.DataFrame:
    try:
        data = api.get_daily(code=code)
    except ConnectionError as e:
        logger.error(f"网络连接失败: {e}")
        raise DataFetchError(f"无法获取 {code} 的数据") from e
    except TimeoutError as e:
        logger.warning(f"请求超时，重试中...")
        # 重试逻辑
    except Exception as e:
        logger.exception(f"未知错误: {e}")
        raise
    
    return data

# ❌ 避免：裸 except
try:
    do_something()
except:  # 不要这样做！
    pass
```

#### 5. 日志记录

使用 **Loguru** 进行日志记录：

```python
from loguru import logger

# 配置日志
logger.add(
    "logs/app.log",
    rotation="10 MB",
    retention="30 days",
    level="INFO",
    encoding="utf-8"
)

# 使用日志
logger.info("开始数据采集任务")
logger.debug(f"处理股票: {stock_code}")
logger.warning(f"数据缺失: {date}")
logger.error(f"计算失败: {e}")
```

### 导入排序

```python
# 1. 标准库
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

# 2. 第三方库
import duckdb
import numpy as np
import polars as pl
import yaml
from loguru import logger

# 3. 本地模块
from core.config import Config
from core.data_loader import DataLoader
from core.market_guardian import enforce_market_closed
from factors.technical import MACD, RSI
from filters.market_filter import MarketFilter
```

### 代码质量门禁

```bash
# 运行代码检查
flake8 core/ factors/ filters/ services/ --max-line-length=100

# 运行类型检查
mypy core/ --ignore-missing-imports

# 运行测试
pytest tests/ -v --cov=core --cov-report=html
```

---

## 测试策略

### 测试分类

| 类型 | 位置 | 目的 | 命名规范 |
|------|------|------|----------|
| **单元测试** | `tests/unit/` | 测试单个组件 | `test_<module>.py` |
| **集成测试** | `tests/integration/` | 测试组件交互 | `test_<feature>_integration.py` |
| **性能测试** | `tests/performance/` | 基准关键路径 | `test_<module>_performance.py` |
| **验收测试** | `tests/test_acceptance.py` | 端到端验证 | `test_<feature>_acceptance.py` |

### 测试文件结构

```
tests/
├── conftest.py                 # pytest 共享配置和 fixtures
├── __init__.py
├── unit/                       # 单元测试
│   ├── test_factors_technical.py
│   ├── test_factors_volume_price.py
│   ├── test_market_filter.py
│   ├── test_stock_filter.py
│   └── test_data_loader.py
├── integration/                # 集成测试
│   ├── test_data_collection.py
│   ├── test_workflow_integration.py
│   ├── test_kestra_integration.py
│   └── test_data_flow.py
├── performance/                # 性能测试
│   └── test_data_service_performance.py
└── test_acceptance.py          # 验收测试
```

### 编写测试示例

```python
# tests/unit/test_factors_technical.py
import pytest
import polars as pl
from factors.technical.macd import MACDFactor


class TestMACDFactor:
    """MACD 因子测试类"""
    
    @pytest.fixture
    def sample_data(self) -> pl.DataFrame:
        """提供测试用的样本数据"""
        return pl.DataFrame({
            'close': [10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 12.8, 13.5],
            'volume': [1000, 1200, 1100, 1300, 1250, 1400, 1350, 1500]
        })
    
    def test_macd_calculation(self, sample_data: pl.DataFrame):
        """测试 MACD 计算是否正确"""
        factor = MACDFactor(fast=12, slow=26, signal=9)
        result = factor.calculate(sample_data)
        
        assert isinstance(result, pl.Series)
        assert len(result) == len(sample_data)
        assert not result.is_null().all()
    
    def test_macd_with_insufficient_data(self):
        """测试数据不足时的处理"""
        short_data = pl.DataFrame({'close': [10.0, 11.0]})
        factor = MACDFactor()
        
        with pytest.raises(ValueError, match="数据长度不足"):
            factor.calculate(short_data)
    
    @pytest.mark.parametrize("fast,slow,expected", [
        (12, 26, True),
        (5, 10, True),
        (26, 12, False),  # fast > slow 应该失败
    ])
    def test_macd_parameters(self, fast: int, slow: int, expected: bool):
        """测试不同参数组合"""
        if expected:
            factor = MACDFactor(fast=fast, slow=slow)
            assert factor.fast == fast
            assert factor.slow == slow
        else:
            with pytest.raises(ValueError):
                MACDFactor(fast=fast, slow=slow)
```

### 运行测试

```bash
# 运行所有测试
pytest

# 运行并生成覆盖率报告
pytest --cov=core --cov-report=html --cov-report=term

# 运行特定测试文件
pytest tests/unit/test_factors_technical.py -v

# 运行特定测试类
pytest tests/unit/test_factors_technical.py::TestMACDFactor -v

# 运行特定测试方法
pytest tests/unit/test_factors_technical.py::TestMACDFactor::test_macd_calculation -v

# 只运行集成测试
pytest tests/integration/ -v --tb=short

# 运行性能测试（标记为 slow）
pytest tests/performance/ -v -m slow

# 并行运行测试（需要 pytest-xdist）
pytest -n auto

# 失败时自动进入 pdb
pytest --pdb

# 只运行上次失败的测试
pytest --lf
```

### 测试配置 (conftest.py)

```python
# tests/conftest.py
import pytest
import polars as pl
from typing import Generator
from unittest.mock import Mock


@pytest.fixture(scope="session")
def test_data_dir() -> str:
    """测试数据目录"""
    return "tests/data"


@pytest.fixture
def mock_tushare_api() -> Mock:
    """模拟 Tushare API"""
    mock = Mock()
    mock.daily.return_value = pl.DataFrame({
        'ts_code': ['000001.SZ'] * 10,
        'trade_date': ['20240101'] * 10,
        'open': [10.0] * 10,
        'high': [11.0] * 10,
        'low': [9.0] * 10,
        'close': [10.5] * 10,
        'vol': [10000] * 10
    })
    return mock


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """每个测试前自动设置环境"""
    monkeypatch.setenv("TEST_MODE", "true")
    monkeypatch.setenv("LOG_LEVEL", "ERROR")


# 自定义标记
def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
```

---

## 部署指南

### Docker Compose 服务

```yaml
# 核心服务
- xcnstock-fetcher      # 数据采集服务
- kestra               # 工作流编排
- redis                # 状态管理
- mysql                # 数据库（可选）
```

### 部署架构

```
┌─────────────────────────────────────────────────────────────┐
│                      部署架构                                │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│   │   Kestra     │    │  APScheduler │    │    Redis     │ │
│   │   (Docker)   │    │   (Docker)   │    │   (Docker)   │ │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘ │
│          │                   │                   │         │
│          └───────────────────┼───────────────────┘         │
│                              │                             │
│                              ▼                             │
│                    ┌──────────────────┐                   │
│                    │  Dual Scheduler  │                   │
│                    │     Manager      │                   │
│                    └────────┬─────────┘                   │
│                             │                             │
│                             ▼                             │
│   ┌─────────────────────────────────────────────────────┐ │
│   │              XCNStock Application                    │ │
│   │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │ │
│   │  │  Data   │ │  Stock  │ │  Backtest│ │ Report  │   │ │
│   │  │ Service │ │ Service │ │ Service  │ │ Service │   │ │
│   │  └─────────┘ └─────────┘ └─────────┘ └─────────┘   │ │
│   └─────────────────────────────────────────────────────┘ │
│                             │                             │
│                             ▼                             │
│   ┌─────────────────────────────────────────────────────┐ │
│   │                    Storage Layer                     │ │
│   │         Parquet    SQLite    DataHub                │ │
│   └─────────────────────────────────────────────────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 部署命令

```bash
# 1. 启动所有服务
docker-compose up -d

# 2. 检查服务状态
docker-compose ps

# 3. 部署 Kestra 工作流
python kestra/deploy.py --namespace xxxcnstock

# 4. 验证部署
python kestra/check_status.py

# 5. 检查执行状态
python kestra/check_executions.py
```

### 环境配置

#### 开发环境

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入实际的 API 密钥
```

#### 生产环境

```bash
# 使用 Docker Compose 部署
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 查看日志
docker-compose logs -f xcnstock-fetcher

# 扩展服务
docker-compose up -d --scale xcnstock-fetcher=3
```

### 监控与告警

#### 关键指标

| 指标 | 描述 | 告警阈值 |
|------|------|----------|
| **数据新鲜度** | 最新 K 线数据日期 | > 30 天 |
| **流水线成功率** | 成功执行的比例 | < 95% |
| **因子计算时间** | 计算所有因子的时间 | > 10 分钟 |
| **回测准确性** | 与已知结果的验证 | < 99% |
| **Kestra 健康** | Kestra 服务状态 | 不可用 |
| **Redis 连接** | Redis 连接状态 | 断开 |

#### 健康检查端点

| 端点 | 用途 |
|------|------|
| `/health` | 整体系统健康 |
| `/health/data` | 数据新鲜度状态 |
| `/health/kestra` | Kestra 连接状态 |
| `/health/redis` | Redis 连接状态 |

```bash
# 检查健康状态
curl http://localhost:8000/health

# 检查数据新鲜度
curl http://localhost:8000/health/data
```

---

## 安全考虑

### 数据保护

- **禁止硬编码凭证**：所有 API 密钥、密码必须通过 `.env` 文件管理
- **环境变量管理**：使用 `python-dotenv` 加载环境变量
- **敏感数据加密**：敏感数据在存储时加密
- **访问日志**：维护审计日志

```python
# ✅ 好的做法：使用环境变量
import os
from dotenv import load_dotenv

load_dotenv()

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TUSHARE_TOKEN:
    raise ValueError("TUSHARE_TOKEN 环境变量未设置")

# ❌ 禁止：硬编码凭证
TUSHARE_TOKEN = "abc123def456"  # 永远不要这样做！
```

### 网络安全

- 内部服务通过 Docker 网络通信
- 外部 API 调用使用 HTTPS
- 公共端点实施速率限制

### .gitignore 配置

```gitignore
# 环境变量
.env
.env.local
.env.production

# 敏感数据
*.pem
*.key
secrets/

# 日志
logs/
*.log

# 数据
data/kline/*.parquet
!data/kline/.gitkeep
```

---

## 开发工作流

### 使用 OpenSpec

```bash
# 1. 探索新功能想法
/openspec:explore new-factor-idea

# 2. 创建正式的变更提案
/openspec:propose add-new-factor

# 3. 审查生成的设计和任务
# (AI 生成 design.md 和 tasks.md)

# 4. 实施变更
/openspec:apply-change add-new-factor

# 5. 完成并归档
/openspec:archive-change add-new-factor
```

### 6A 标准执行流程

| 阶段 | 目标 | 交付物 |
|------|------|--------|
| **1. Align** | 需求对齐 | `ALIGNMENT.md`, `CONSENSUS.md` |
| **2. Architect** | 架构设计 | `DESIGN.md` |
| **3. Atomize** | 任务原子化 | `TASK_FLOW.md` |
| **4. Approve** | 方案审批 | 用户确认 |
| **5. Automate** | 自动化执行 | 业务代码 + `ACCEPTANCE.md` |
| **6. Assess** | 交付评估 | `FINAL.md`, `TODO.md` |

### Git 工作流

1. 从 `main` 分支创建功能分支
2. 按照规范进行更改
3. 运行测试：`pytest`
4. 运行代码检查：`flake8`
5. 创建 PR 并添加描述
6. 审查后合并

```bash
# 创建功能分支
git checkout -b feature/add-new-factor

# 提交更改
git add .
git commit -m "feat: 添加新的量价因子"

# 推送分支
git push origin feature/add-new-factor

# 创建 PR（通过 GitHub CLI 或网页）
gh pr create --title "feat: 添加新的量价因子" --body "..."
```

---

## 附录

### A. 关键文件参考

| 文件 | 用途 |
|------|------|
| `core/market_guardian.py` | 市场时间验证 |
| `core/factor_engine.py` | 因子计算编排 |
| `core/strategy_engine.py` | 策略执行逻辑 |
| `core/data_loader.py` | 数据加载器 |
| `services/data_service/main.py` | 数据服务入口 |
| `config/main.yaml` | 主配置 |
| `kestra/flows/*.yml` | 工作流定义 |
| `filters/market_filter.py` | 市场过滤器 |
| `factors/technical/macd.py` | MACD 因子 |

### B. 外部依赖

| 服务 | 版本 | 用途 |
|------|------|------|
| Kestra | Latest | 工作流编排 |
| Redis | 7.x | 缓存和状态 |
| DataHub | Latest | 元数据管理 |

### C. 常用命令速查

```bash
# 数据采集
data_collect.py --date 2024-01-01 --codes 000001.SZ,600000.SH

# 因子计算
factor_calc.py --date 2024-01-01 --factor macd

# 选股扫描
stock_scan.py --strategy fund_behavior --date 2024-01-01

# 回测
backtest.py --strategy fund_behavior --start 2023-01-01 --end 2023-12-31

# 生成报告
report_gen.py --date 2024-01-01 --output reports/
```

### D. 故障排查

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 数据采集失败 | 盘中采集当日数据 | 等待收盘后重试 |
| Kestra 连接失败 | 服务未启动 | `docker-compose up -d kestra` |
| 因子计算超时 | 数据量过大 | 分批处理或增加内存 |
| 回测结果异常 | 数据不完整 | 检查数据新鲜度 |
| 选股结果为空 | 过滤器过于严格 | 调整过滤条件 |

- [OpenSpec Guide](./GUIDE.md)
- [Kestra Documentation](../docs/kestra_workflows_documentation.md)
- [Dual Scheduler Architecture](../docs/dual_scheduler_architecture.md)

---

*Last Updated: 2026-04-25*  
*Version: 1.0.0*
