<!-- refreshed: 2026-05-12 -->
# Architecture

**Analysis Date:** 2026-05-12

## System Overview

XCNStock is a Chinese A-share quantitative analysis platform that collects market data, computes technical/market factors, filters and scores stocks, generates reports, and delivers notifications on a scheduled basis. It is a monolithic Python codebase with a microservice-oriented FastAPI layer and a cron-driven pipeline runner.

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                        API Gateway (FastAPI)                            │
│                        `gateway/main.py` :8000                          │
├──────────────┬──────────────────┬──────────────────┬────────────────────┤
│  Data Service│  Stock Service   │  Limit Service   │  Notify Service    │
│  :8001       │  :8002           │  :8003           │  :8004             │
│ `services/   │ `services/       │ `services/       │ `services/         │
│  data_service│  stock_service/  │  limit_service/  │  notify_service/   │
│  /main.py`   │  main.py`        │  main.py`        │  main.py`          │
├──────────────┴──────────────────┴──────────────────┴────────────────────┤
│                    Core Library (`core/`)                                │
│  ┌───────────┐ ┌────────────┐ ┌────────────┐ ┌──────────┐ ┌─────────┐ │
│  │ FactorEngine│ │ FilterEngine│ │ StrategyEng│ │ DataLoader│ │ Workflow │ │
│  │ `core/      │ │ `filters/   │ │ `core/     │ │ `core/    │ │ Framework│ │
│  │ factor_     │ │ filter_     │ │ strategy_  │ │ data_     │ │ `core/   │ │
│  │ engine.py`  │ │ engine.py`  │ │ engine.py` │ │ loader.py`│ │ workflow_│ │
│  │             │ │             │ │            │ │           │ │ framework│ │
│  │             │ │             │ │            │ │           │ │ .py`     │ │
│  └───────────┘ └────────────┘ └────────────┘ └──────────┘ └─────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│                    Pipeline Scripts (`scripts/pipeline/`)                │
│  data_collect -> data_audit -> precompute -> stock_pick -> reports      │
│  Scheduled by APScheduler (`scripts/scheduler.py`) via cron              │
├─────────────────────────────────────────────────────────────────────────┤
│                    Infrastructure                                        │
│  ┌──────────┐ ┌──────────┐ ┌───────────┐ ┌───────────┐ ┌────────────┐ │
│  │ MySQL 8.0│ │ Redis 7  │ │ Parquet   │ │ Nacos     │ │ Prometheus │ │
│  │ (RDBMS)  │ │ (Cache/  │ │ (File     │ │ (Config)  │ │ (Metrics)  │ │
│  │          │ │  Locks)  │ │  Storage) │ │           │ │            │ │
│  └──────────┘ └──────────┘ └───────────┘ └───────────┘ └────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | Key Files |
|-----------|----------------|-----------|
| **Gateway** | API reverse proxy routing requests to downstream services | `gateway/main.py` |
| **Data Service** | Market data fetching (quotes, klines, fundamentals, limit-up data) | `services/data_service/main.py` |
| **Stock Service** | Stock screening, multi-dimensional scoring, ranking | `services/stock_service/main.py`, `services/stock_service/engine.py` |
| **Limit Service** | Limit-up (zhangting) analysis, next-day prediction | `services/limit_service/main.py`, `services/limit_service/engine.py` |
| **Notify Service** | Multi-channel push: WeChat, DingTalk, Email, Kafka | `services/notify_service/main.py`, `services/notify_service/signal_hub.py` |
| **Core Library** | Shared domain logic: factors, filters, strategies, data loading, caching | `core/*.py`, `filters/`, `factors/`, `patterns/` |
| **Pipeline Scripts** | Cron-executed tasks: data collection, auditing, scoring, report generation | `scripts/pipeline/*.py` |
| **Scheduler** | APScheduler-based cron runner reading from `config/scheduler.yaml` | `scripts/scheduler.py` |
| **Workflows** | Higher-level workflow orchestration (data collection, stock selection, backtest, daily ops) | `workflows/*.py` |
| **Config** | YAML-driven configuration for factors, filters, strategies, scheduling | `config/*.yaml` |

## Pattern Overview

**Overall:** Plugin-registry pattern with YAML configuration driving factor/filter/strategy composition.

**Key Characteristics:**
- **Registry pattern** for factors and filters: classes self-register via decorators (`@register_factor`, `@register_filter`) into singleton registries (`FactorRegistry`, `FilterRegistry`)
- **Adapter pattern** for data sources: `DataSourceAdapter` base class with `TushareAdapter` and `BaostockAdapter` implementations
- **Strategy pattern** for stock selection: YAML config files define factor weights and filter thresholds
- **Pipeline pattern** for scheduled tasks: sequential cron jobs pass data through stages (collect -> audit -> compute -> score -> report)
- **Multi-level cache**: L1 in-memory + L2 Redis, coordinated by `core/cache/multi_level_cache.py`

## Layers

### 1. Presentation Layer (API Services)
- Purpose: Expose REST API endpoints for external consumers
- Location: `services/*/main.py`, `gateway/main.py`
- Contains: FastAPI route definitions, Pydantic request/response models
- Depends on: Core library, service-specific engines
- Used by: External HTTP clients (web frontend, mobile app, monitoring tools)

### 2. Service Layer
- Purpose: Business logic orchestration within each microservice domain
- Location: `services/*/engine.py`, `services/*/scorer.py`, `services/*/analyzers/`
- Contains: Domain-specific business logic (stock scoring, limit-up analysis, notification dispatch)
- Depends on: Core library, infrastructure (MySQL, Redis, Parquet files)
- Used by: Presentation layer (API routes)

### 3. Domain/Core Layer
- Purpose: Reusable domain abstractions -- factors, filters, patterns, strategies
- Location: `core/`, `factors/`, `filters/`, `patterns/`
- Contains: Abstract base classes, registry implementations, computation engines
- Depends on: Polars, configuration files, external data APIs (Tushare, Baostock)
- Used by: Service layer, pipeline scripts, workflows

### 4. Infrastructure Layer
- Purpose: Data access, caching, locking, configuration, logging
- Location: `core/cache/`, `core/storage/`, `core/config.py`, `core/distributed_lock.py`, `core/logger.py`
- Contains: MySQL connection pools, Redis clients, Parquet file utilities, multi-level cache
- Depends on: External services (MySQL, Redis, Nacos)
- Used by: All upper layers

### 5. Pipeline Layer
- Purpose: Scheduled task execution with monitoring and retry logic
- Location: `scripts/pipeline/`, `scripts/scheduler.py`
- Contains: Individual task scripts, APScheduler configuration, health checks
- Depends on: Core library, services layer
- Used by: Cron daemon or manual invocation

## Data Flow

### Primary Pipeline: End-of-Day Processing (Weekdays)

1. **16:00 Data Collection** (`scripts/pipeline/data_collect_with_validation.py`) -- fetches all A-share daily OHLCV data via Tencent/Baostock APIs
2. **16:08 Limit-Up Data** (`scripts/pipeline/limitup_data_collect.py`) -- collects limit-up stock list and consecutive board data
3. **16:18 Fund Flow** (`scripts/pipeline/fund_flow_collect.py`) -- collects money flow data (main force vs retail)
4. **17:00 Data Audit** (`scripts/pipeline/data_audit_unified.py`) -- validates data freshness, completeness, quality (with circuit breaker)
5. **17:40 CVD Calculation** (`scripts/pipeline/calculate_cvd.py`) -- computes 60-day cumulative volume difference indicator
6. **17:45 Dragon Tiger** (`scripts/pipeline/dragon_tiger_collect.py`) -- collects dragon-tiger board (institutional trading) data
7. **18:00 Stock Selection** (`scripts/pipeline/stock_pick.py`) -- runs factor computation, filtering, scoring, and generates picks
8. **18:10 Picks Review** (`scripts/pipeline/stock_pick.py` via different args) -- validates yesterday's picks performance
9. **18:20 Drawdown Analysis** (`scripts/pipeline/drawdown_analyzer.py`) -- analyzes drawdown of selected stocks
10. **18:25 Daily Selection Review** (`scripts/pipeline/daily_stock_selection_review.py`) -- updates multi-period performance
11. **18:30 Review Report** (`scripts/pipeline/send_review_report.py`) -- pushes comprehensive evening review report
12. **20:00 Precompute** (`scripts/pipeline/precompute.py`) -- precomputes technical indicator scores
13. **20:35 Night Picks** (`scripts/pipeline/night_picks.py`) -- generates evening stock recommendations

### Morning Pipeline (Weekdays)

1. **08:30 Morning Update** (`scripts/pipeline/morning_update.py`) -- updates overnight foreign market data
2. **08:32-08:40 Macro/Sentiment/News** -- collects macro indicators, oil/dollar, commodities, sentiment, news data
3. **08:42 Market Analysis** (`scripts/pipeline/market_analysis.py`) -- computes key levels and CVD for indices
4. **08:45 Morning Report** (`scripts/pipeline/send_morning_shao.py`) -- pushes morning briefing report
5. **09:15-09:26 Core Task** -- resource preparation, validation, and fund behavior strategy execution

### Intra-Service Request Flow (API Gateway)

1. Client sends request to `gateway/main.py` on port 8000
2. Gateway routes to appropriate backend service via `httpx.AsyncClient`:
   - `/api/v1/quote/*` -> Data Service (:8001)
   - `/api/v1/stock/*` -> Stock Service (:8002)
   - `/api/v1/limit/*` -> Limit Service (:8003)
   - `/api/v1/notify/*` -> Notify Service (:8004)
3. Service processes request using its engine
4. Response flows back through gateway

### Factor/Filter Computation Flow

1. `core/data_loader.py` loads K-line data from Parquet files in `data/kline/`
2. `core/factor_engine.py` loads factor configs from `config/factors/*.yaml`
3. Factor modules in `factors/technical/`, `factors/market/`, `factors/volume_price/` self-register via `@register_factor`
4. Each factor's `calculate()` method adds a `factor_{name}` column to the Polars DataFrame
5. `filters/filter_engine.py` loads filter configs from `config/filters/*.yaml`
6. Filter modules in `filters/*.py` self-register via `@register_filter`
7. Each filter's `filter()` method removes disqualified stocks from the DataFrame
8. `core/strategy_engine.py` combines factors with weights and applies filters to produce a final ranked list

**State Management:**
- Primary storage: Parquet files in `data/kline/` (one file per stock, e.g., `000001.parquet`)
- Secondary storage: MySQL 8.0 for stock selections, report tracking, execution history
- Cache: Multi-level (L1 memory + L2 Redis) managed by `core/cache/multi_level_cache.py`
- Scheduling state: SQLite at `data/scheduler_history.db` for execution history
- Distributed locks: Redis-based via `core/distributed_lock.py`

## Key Abstractions

**Factor (Plugin):**
- Purpose: Computes a quantitative indicator from OHLCV data
- Base class: `core/factor_library.BaseFactor`
- Registry: `FactorRegistry` singleton in `core/factor_library.py`
- Registration: `@register_factor("name")` decorator
- Examples: `factors/technical/macd.py` (MacdFactor), `factors/volume_price/volume_ratio.py` (VolumeRatioFactor)
- Pattern: Self-registering plugin

**Filter (Plugin):**
- Purpose: Removes disqualified stocks from the candidate pool
- Base class: `filters/base_filter.BaseFilter`
- Registry: `FilterRegistry` singleton in `filters/base_filter.py`
- Registration: `@register_filter("name")` decorator
- Examples: `filters/technical_filter.py` (TrendFilter), `filters/financial_filter.py` (ROEFilter)
- Pattern: Self-registering plugin with configurable presets (conservative/standard/aggressive)

**Data Source Adapter:**
- Purpose: Abstracts market data retrieval from external APIs
- Base class: `core/data_adapter.DataSourceAdapter`
- Manager: `DataSourceManager` with primary/fallback routing
- Examples: `TushareAdapter` (primary), `BaostockAdapter` (fallback)
- Pattern: Adapter with automatic fallback

**Workflow Step:**
- Purpose: Encapsulates a pipeline step with dependency checks, retry, and checkpoint support
- Base class: `core/workflow_framework.WorkflowStep` (ABC)
- Framework: `core/workflow_framework.py` provides retry config, checkpoint save/restore, dependency status
- Examples: `workflows/data_collection_workflow.py`, `workflows/stock_selection_workflow.py`

**Signal/Notification:**
- Purpose: Standardized message model for notifications
- Models: `core/models.py` -- `StockSelectionSignal`, `LimitUpSignal`, `NotificationMessage`, `SignalLevel` (S/A/B/C)
- Hub: `services/notify_service/signal_hub.py` dispatches to channels
- Channels: `services/notify_service/channels/` (wechat, dingtalk, email, kafka_producer)

## Entry Points

**API Gateway:**
- Location: `gateway/main.py`
- Triggers: HTTP requests on port 8000
- Responsibilities: Route to backend services, health aggregation

**Data Service:**
- Location: `services/data_service/main.py`
- Triggers: HTTP requests on port 8001; also has internal scheduler (`DataScheduler`)
- Responsibilities: Real-time quotes, K-line data, stock list, fundamentals, limit-up data

**Stock Service:**
- Location: `services/stock_service/main.py`
- Triggers: HTTP requests on port 8002
- Responsibilities: Stock screening, scoring, ranking

**Limit Service:**
- Location: `services/limit_service/main.py`
- Triggers: HTTP requests on port 8003
- Responsibilities: Limit-up analysis, next-day prediction

**Notify Service:**
- Location: `services/notify_service/main.py`
- Triggers: HTTP requests on port 8004
- Responsibilities: Multi-channel notification dispatch

**Pipeline Scheduler:**
- Location: `scripts/scheduler.py`
- Triggers: Runs as a long-lived process; reads `config/scheduler.yaml`
- Responsibilities: Cron-based task execution with distributed locks, retry, history tracking, circuit breaker

**Workflow Runner:**
- Location: `workflows/workflow_runner.py`
- Triggers: CLI invocation
- Responsibilities: Run named workflows (data_collection, stock_selection, backtest, daily_operation)

## Architectural Constraints

- **Threading:** APScheduler uses a thread pool (max 4 workers). Individual pipeline scripts are subprocess-based. Distributed locks via Redis prevent concurrent execution of the same task across instances.
- **Global state:** Several singletons exist: `UnifiedConfig` (`core/unified_config.py`), `NacosClientSingleton` (`core/config.py`), `FactorRegistry` (`core/factor_library.py`), `FilterRegistry` (`filters/base_filter.py`), `DataSourceManager` (`core/data_adapter.py`). All are thread-safe via `__new__` or `lru_cache`.
- **Circular imports:** `sys.path.insert(0, ...)` is used in several files (e.g., `core/workflow_framework.py`, `core/data_adapter.py`) to work around import issues. This is a code smell but prevents circular imports at module level.
- **Data format:** Polars DataFrames are the primary in-memory data format (not Pandas). Some legacy scripts use Pandas. All K-line data is stored as Parquet files.
- **Timezone:** All times are in `Asia/Shanghai`. Trading calendar handles Chinese market holidays.
- **Language:** Code comments and log messages are primarily in Chinese.

## Anti-Patterns

### Hardcoded Path Fallback

**What happens:** Several files contain hardcoded fallback paths like `/Volumes/Xdata/workstation/xxxcnstock/` in `sys.path.insert(0, ...)` or as default data directories.
**Why it's wrong:** Breaks on any machine other than the developer's. Deployment to Docker or another host will fail silently or use wrong paths.
**Do this instead:** Use `core/paths.py` `PROJECT_ROOT` consistently, and remove all hardcoded absolute paths. Files affected: `core/workflow_framework.py:13`, `core/data_loader.py:33-34`, `core/data_loader.py:107-108`, `workflows/workflow_runner.py:12`.

### Mock Data in Production API Endpoints

**What happens:** Stock Service and Limit Service endpoints return hardcoded mock data (e.g., `services/stock_service/main.py:42-60`, `services/limit_service/main.py:33-38`).
**Why it's wrong:** These API services are not connected to real data. They are stub implementations.
**Do this instead:** Connect endpoints to the actual engines and data sources. The engines exist but are not wired to the routes.

### Dead Code After Early Return

**What happens:** `core/data_loader.py` has unreachable code at lines 317-336 (an `else` clause after a function body with `return`).
**Why it's wrong:** Python syntax error or logic error -- this code can never execute.
**Do this instead:** Remove the unreachable block or restructure the function.

## Error Handling

**Strategy:** Layered -- exceptions are caught at each boundary and logged.

**Patterns:**
- Pipeline scripts return `bool` (True=success, False=failure) and `sys.exit(0/1)`
- API services catch exceptions per-endpoint and return HTTP 503 with error messages
- Workflow framework provides `RetryConfig` with exponential backoff (default: 3 retries, 2x backoff)
- Scheduler has circuit breaker state persisted to JSON (`data/tasks/circuit_breaker_state.json`)
- Data quality checks have freshness validation with fallback to previous day's data

## Cross-Cutting Concerns

**Logging:** Dual framework -- `core/logger.py` wraps Python `logging` with some modules using `loguru`. Log files are organized by subsystem: `logs/pipeline/`, `logs/signals/`, `logs/scheduler/`, `logs/system/`. Rotation is configured via `RotatingFileHandler`.

**Validation:** Data quality validation uses Great Expectations (`great_expectations/`) and custom validators (`services/data_service/quality/`). Checkpoint-based validation at each pipeline stage.

**Authentication:** Not implemented for internal APIs. The gateway exposes endpoints without auth. External integrations (Tushare, Baostock) use API tokens from environment variables.

**Monitoring:** Prometheus metrics exposed via `core/pipeline_monitor.py` (task duration, success rate, cache hit ratio, data collection metrics). Health check endpoints at `/health` on each service.

**Configuration:** YAML files in `config/` for factors, filters, strategies, scheduling. Environment variables via `.env` for secrets (database, API keys). `core/unified_config.py` provides a singleton that merges YAML + env vars + defaults.

**Notification:** `services/notify_service/` dispatches signals through channels: WeChat (ServerChan), DingTalk webhook, Email (SMTP/API), Kafka producer. Signal levels (S/A/B/C) determine priority and routing.

---

*Architecture analysis: 2026-05-12*
