# Codebase Structure

**Analysis Date:** 2026-05-12

## Directory Layout

```
xxxcnstock/
├── api/                        # Cache API (standalone FastAPI app)
│   └── cache_api.py            # Cache management endpoints
├── backup/                     # Data backups (invalid kline, etc.)
├── config/                     # All YAML configuration files
│   ├── backups/                # Config backups
│   ├── factors/                # Factor definition YAML files
│   ├── filters/                # Filter definition YAML files
│   ├── patterns/               # Pattern definition YAML files
│   ├── strategies/             # Strategy definition YAML files
│   ├── alerts.yaml             # Alert rules
│   ├── cron_tasks.yaml         # Legacy cron task definitions
│   ├── datasource.yaml         # Data source configuration
│   ├── factors_config.yaml     # Top-level factor config
│   ├── filters_config.yaml     # Top-level filter config
│   ├── fund_behavior_config.yaml  # Fund behavior strategy config
│   ├── main.yaml               # Main application config
│   ├── scheduler.yaml          # APScheduler task definitions (primary)
│   ├── stock_selection_strategy.yaml  # Stock selection strategy
│   └── strategy_factors.yaml   # Strategy-factor mapping
├── core/                       # Core library (shared domain logic)
│   ├── alerting/               # Alert dispatch (webhook_notifier.py)
│   ├── cache/                  # Multi-level cache (memory + Redis)
│   ├── indicators/             # Low-level indicator calculations
│   ├── monitoring/             # Monitoring utilities
│   ├── notification/           # Notification base (notifier.py)
│   ├── storage/                # Parquet utilities, lock manager
│   ├── utils/                  # Shared utilities
│   ├── async_http.py           # Async HTTP client for data fetching
│   ├── backtest_engine.py      # Backtesting framework
│   ├── broker_adapter.py       # Broker trading API adapters
│   ├── config.py               # Pydantic Settings + Nacos singleton
│   ├── data_adapter.py         # Tushare/Baostock adapter layer
│   ├── data_availability.py    # Data availability checks
│   ├── data_freshness_checker.py  # Data freshness validation
│   ├── data_loader.py          # Polars-based K-line data loader
│   ├── data_quality_checker.py # Data quality validation
│   ├── data_quality_guardian.py # Quality gate with circuit breaker
│   ├── data_quality_metrics.py # Quality metric computation
│   ├── data_validator.py       # Data validation utilities
│   ├── data_version_manager.py # Data versioning
│   ├── delisting_guard.py      # Delisting stock detection
│   ├── distributed_lock.py     # Redis-based distributed lock
│   ├── factor_analyzer.py      # Factor analysis and backtesting
│   ├── factor_calculator.py    # Factor computation utilities
│   ├── factor_config_loader.py # YAML factor config loader
│   ├── factor_engine.py        # Factor computation engine
│   ├── factor_filter_config.py # Factor-filter configuration bridge
│   ├── factor_library.py       # BaseFactor, FactorRegistry
│   ├── filter_config_loader.py # YAML filter config loader
│   ├── fund_behavior_config.py # Fund behavior config loader
│   ├── fund_behavior_indicator.py  # Fund behavior indicators
│   ├── fund_behavior_strategy.py   # Fund behavior strategy logic
│   ├── incremental_processor.py    # Incremental data processing
│   ├── intraday_collection_detector.py  # Intraday collection timing
│   ├── kelly_calculator.py     # Kelly criterion calculator
│   ├── logger.py               # Logging setup utility
│   ├── market_guardian.py      # Market status guard
│   ├── models.py               # Pydantic models (signals, quotes, etc.)
│   ├── network_config.py       # Network/rate-limit configuration
│   ├── optimized_data_loader.py # Optimized bulk data loader
│   ├── optimized_factor_engine.py # Optimized factor computation
│   ├── parallel_fetcher.py     # Parallel data fetching
│   ├── paths.py                # Centralized path management
│   ├── pipeline_monitor.py     # Prometheus metrics for pipelines
│   ├── pipeline_state.py       # Pipeline state tracking
│   ├── polars_optimizer.py     # Polars performance optimizations
│   ├── report_config.py        # Report configuration
│   ├── report_generator.py     # Report generation base
│   ├── report_validator.py     # Report content validation
│   ├── strategy_engine.py      # Strategy execution engine
│   ├── strategy_executor.py    # Strategy execution wrapper
│   ├── trading_calendar.py     # Chinese trading calendar
│   ├── unified_config.py       # Unified config manager (YAML + env)
│   ├── version_aware.py        # Version-aware data handling
│   ├── workflow_framework.py   # Workflow step framework (retry, checkpoint)
│   ├── workflow_scheduler.py   # Workflow scheduling
│   └── workflow_state_db.py    # Workflow state persistence (SQLite)
├── data/                       # Runtime data directory
│   ├── cache/                  # Cached data files
│   ├── checkpoints/            # Pipeline checkpoint files
│   ├── kline/                  # K-line Parquet files (one per stock)
│   ├── index/                  # Index data Parquet files
│   ├── key_levels/             # Key price level data
│   ├── limitup/                # Limit-up stock data
│   ├── market_snapshots/       # Market snapshot data
│   ├── picks/                  # Stock pick results
│   ├── selection_results/      # Selection result files
│   └── tasks/                  # Task state, progress, circuit breaker
├── docs/                       # Documentation
│   ├── data_service_expansion/ # Data service expansion docs
│   ├── kestra_integration/     # Kestra integration docs (deprecated)
│   ├── plans/                  # Planning documents
│   ├── strategy/               # Strategy documentation
│   └── tasks/                  # Task documentation
├── factors/                    # Factor implementations (plugin modules)
│   ├── market/                 # Market-level factors (breadth, sentiment, trend, etc.)
│   ├── technical/              # Technical indicators (MACD, RSI, KDJ, Bollinger, etc.)
│   └── volume_price/           # Volume-price factors (OBV, MFI, volume ratio, etc.)
├── filters/                    # Filter implementations (plugin modules)
│   ├── base_filter.py          # BaseFilter, FilterRegistry
│   ├── filter_engine.py        # FilterEngine (orchestrates all filters)
│   ├── announcement_filter.py  # Corporate announcement filters
│   ├── financial_filter.py     # Financial statement filters
│   ├── fundamental_filter.py   # Fundamental data filters
│   ├── liquidity_filter.py     # Liquidity/turnover filters
│   ├── market_behavior_filter.py  # Market behavior filters (dragon tiger, fund flow)
│   ├── market_filter.py        # Market cap/suspension/price filters
│   ├── pattern_filter.py       # Price pattern filters
│   ├── stock_filter.py         # ST/new stock/delisting filters
│   ├── technical_filter.py     # Technical indicator filters
│   └── valuation_filter.py     # Valuation filters
├── gateway/                    # API Gateway service
│   ├── main.py                 # FastAPI gateway (port 8000)
│   └── routers/                # (Reserved for router modules)
├── great_expectations/         # Great Expectations data quality config
├── logs/                       # Runtime log files
│   ├── pipeline/               # Pipeline task logs
│   ├── scheduler/              # Scheduler logs
│   ├── signals/                # Signal notification logs
│   └── system/                 # System-level logs
├── models/                     # ML model artifacts
│   ├── xgboost_stock_picker.pkl  # Trained XGBoost model
│   ├── xgboost_stock_picker_v2/  # Model v2 directory
│   ├── xgboost_stock_picker_v4/  # Model v4 directory
│   ├── opening_predictor.py    # Opening price predictor
│   └── stock_pick_verification.py  # Pick verification model
├── nacos-data/                 # Nacos configuration snapshots
├── optimization/               # Strategy optimization tools
├── openspec/                   # OpenSpec specification documents
├── patterns/                   # Price pattern detection
│   ├── base_pattern.py         # Base pattern class
│   ├── candlestick.py          # Candlestick pattern detection
│   ├── continuation.py         # Continuation patterns
│   ├── reversal.py             # Reversal patterns
│   ├── special.py              # Special patterns
│   └── pattern_engine.py       # Pattern detection engine
├── reports/                    # Generated report data (JSON)
├── scripts/                    # Executable scripts (267 files)
│   └── pipeline/               # Scheduled pipeline tasks (33 files)
│       ├── data_collect.py             # K-line data collection
│       ├── data_collect_with_validation.py  # Validated data collection
│       ├── data_audit_unified.py       # Unified data audit
│       ├── stock_pick.py               # Stock selection
│       ├── morning_push.py             # Morning report push
│       ├── night_picks.py              # Night picks generation
│       ├── send_morning_shao.py        # Morning briefing dispatch
│       ├── send_review_report.py       # Evening review dispatch
│       ├── precompute.py               # Precompute technical scores
│       ├── calculate_cvd.py            # CVD indicator calculation
│       ├── limitup_data_collect.py     # Limit-up data collection
│       ├── dragon_tiger_collect.py     # Dragon-tiger board collection
│       ├── fund_flow_collect.py        # Fund flow data collection
│       └── ...                         # Many more utility/analysis/debug scripts
├── services/                   # Microservice implementations
│   ├── analysis_service/       # Analysis service (fundamental, macro, sentiment)
│   ├── attribution/            # Performance attribution
│   ├── backtest_service/       # Backtesting service
│   ├── data/                   # Shared data utilities
│   ├── data_service/           # Data service (main FastAPI app)
│   │   ├── fetchers/           # Data fetcher modules (30+ fetchers)
│   │   ├── collectors/         # Data collectors (historical, intraday, realtime)
│   │   ├── processors/         # Data processors
│   │   ├── quality/            # Data quality validators
│   │   ├── scheduler/          # Internal data scheduler
│   │   ├── storage/            # Data storage adapters
│   │   ├── tasks/              # Background task definitions
│   │   └── main.py             # FastAPI app entry point
│   ├── limit_service/          # Limit-up analysis service
│   │   ├── analyzers/          # Limit-up analyzers
│   │   ├── engine.py           # Limit-up analysis engine
│   │   └── main.py             # FastAPI app entry point
│   ├── metadata/               # Metadata management service
│   ├── ml_service/             # Machine learning service
│   ├── notify_service/         # Notification service
│   │   ├── channels/           # Notification channels (wechat, dingtalk, email, kafka)
│   │   ├── templates/          # Message templates
│   │   ├── signal_hub.py       # Signal dispatch hub
│   │   └── main.py             # FastAPI app entry point
│   ├── risk_service/           # Risk management service (circuit breaker, position, stoploss)
│   ├── scheduler/              # External scheduler integration (DolphinScheduler)
│   ├── stock_service/          # Stock selection service
│   │   ├── engine.py           # Stock selection engine
│   │   ├── filters/            # Service-level filter adapters
│   │   ├── scorer.py           # Stock scoring module
│   │   └── main.py             # FastAPI app entry point
│   ├── strategy_service/       # Strategy implementations
│   │   ├── dragon_head/        # Dragon head strategy
│   │   ├── endstock_pick/      # End-of-day stock pick strategy
│   │   ├── limitup_callback/   # Limit-up callback strategy
│   │   ├── momentum/           # Momentum strategy
│   │   ├── multi_factor/       # Multi-factor strategy
│   │   └── sector_rotation/    # Sector rotation strategy
│   └── ...                     # More service modules (db services, key levels, etc.)
├── sql/                        # SQL schema files
├── templates/                  # Report templates
├── tests/                      # Test directory
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── performance/            # Performance tests
├── web/                        # Web frontend (placeholder)
├── web_report/                 # Web report (placeholder)
├── workflows/                  # Workflow definitions
│   ├── workflow_runner.py      # CLI workflow runner
│   ├── data_collection_workflow.py     # Data collection workflow
│   ├── stock_selection_workflow.py     # Stock selection workflow
│   ├── backtest_workflow.py            # Backtesting workflow
│   ├── daily_operation_workflow.py     # Daily operations workflow
│   ├── enhanced_*.py           # Enhanced versions of workflows
│   └── unified_trading_system.py       # Unified trading system orchestration
├── .env                        # Environment variables (secrets)
├── .env.example                # Example environment file
├── CLAUDE.md                   # AI coding assistant guidelines
├── docker-compose.yml          # Docker Compose (main services)
├── docker-compose.scheduler.yml # Docker Compose (scheduler only)
├── Dockerfile                  # Main application Dockerfile
├── Dockerfile.scheduler        # Scheduler Dockerfile
├── pyproject.toml              # Python project configuration
├── requirements.txt            # Python dependencies
├── requirements_scheduler.txt  # Scheduler-only dependencies
└── 说明文档.md                  # Project documentation (Chinese)
```

## Directory Purposes

**`core/`:**
- Purpose: Shared domain logic and infrastructure utilities
- Contains: 60+ Python modules covering data loading, factor/filter engines, caching, workflow framework, monitoring, configuration
- Key files: `config.py`, `paths.py`, `data_loader.py`, `factor_engine.py`, `strategy_engine.py`, `distributed_lock.py`, `unified_config.py`

**`factors/`:**
- Purpose: Factor plugin implementations organized by category
- Contains: Self-registering factor classes (technical, market, volume_price)
- Key files: `__init__.py` (imports trigger registration), `factor_library.py` (base class + registry)

**`filters/`:**
- Purpose: Filter plugin implementations for stock screening
- Contains: Self-registering filter classes (10+ filter modules covering financial, technical, market, pattern filters)
- Key files: `base_filter.py` (base class + registry), `filter_engine.py` (orchestration)

**`patterns/`:**
- Purpose: Price pattern detection (candlestick, continuation, reversal, special)
- Contains: Pattern detection algorithms
- Key files: `pattern_engine.py`, `base_pattern.py`

**`services/`:**
- Purpose: Microservice implementations with FastAPI endpoints
- Contains: 4 primary services (data, stock, limit, notify) plus supporting services
- Key files: Each service has `main.py` (FastAPI app) and `engine.py` (business logic)

**`scripts/`:**
- Purpose: Executable scripts for data operations, analysis, debugging
- Contains: 267 Python scripts including 33 scheduled pipeline tasks
- Key files: `pipeline/` subdirectory for cron tasks, `scheduler.py` for APScheduler runner

**`config/`:**
- Purpose: YAML configuration driving factor, filter, strategy, and scheduling behavior
- Contains: Task schedules, factor definitions, filter thresholds, strategy parameters
- Key files: `scheduler.yaml` (primary schedule), `factors_config.yaml`, `filters_config.yaml`

**`data/`:**
- Purpose: Runtime data storage
- Contains: Parquet files (kline, index), cache, checkpoints, task state, reports
- Key files: `data/kline/*.parquet` (per-stock K-line data), `data/enhanced_scores_full.parquet`

**`workflows/`:**
- Purpose: Higher-level workflow orchestration
- Contains: Workflow classes composing multiple steps
- Key files: `workflow_runner.py` (CLI entry), `*_workflow.py` files

**`models/`:**
- Purpose: ML model artifacts and prediction code
- Contains: Trained XGBoost models, opening predictor, pick verification
- Key files: `xgboost_stock_picker.pkl`, `xgboost_stock_picker_v4/`

## Key File Locations

**Entry Points:**
- `gateway/main.py`: API gateway (port 8000)
- `services/data_service/main.py`: Data service (port 8001)
- `services/stock_service/main.py`: Stock selection service (port 8002)
- `services/limit_service/main.py`: Limit-up analysis service (port 8003)
- `services/notify_service/main.py`: Notification service (port 8004)
- `scripts/scheduler.py`: Pipeline scheduler (APScheduler, reads `config/scheduler.yaml`)
- `workflows/workflow_runner.py`: CLI workflow runner

**Configuration:**
- `core/config.py`: Pydantic Settings (env vars, secrets)
- `core/unified_config.py`: Unified config manager (YAML + env + defaults)
- `core/paths.py`: Centralized path definitions
- `config/scheduler.yaml`: APScheduler task schedule (primary)
- `config/factors_config.yaml`: Factor configuration
- `config/filters_config.yaml`: Filter configuration
- `config/stock_selection_strategy.yaml`: Stock selection strategy

**Core Logic:**
- `core/factor_engine.py`: Factor computation engine
- `core/factor_library.py`: Factor base class + registry
- `core/strategy_engine.py`: Strategy execution engine
- `core/data_loader.py`: K-line data loading (Polars + Parquet)
- `core/data_adapter.py`: Data source adapter (Tushare + Baostock)
- `core/workflow_framework.py`: Workflow step framework (retry, checkpoint, dependency)
- `filters/filter_engine.py`: Filter orchestration engine
- `filters/base_filter.py`: Filter base class + registry

**Data Storage:**
- `data/kline/`: Per-stock K-line Parquet files (primary data store)
- `data/enhanced_scores_full.parquet`: Precomputed stock scores
- `data/scheduler_history.db`: Task execution history (SQLite)
- MySQL 8.0: Stock selections, report tracking (accessed via `services/db_pool.py`)

**Testing:**
- `tests/unit/`: Unit tests
- `tests/integration/`: Integration tests
- `tests/performance/`: Performance tests
- Root-level `test_*.py`: Ad-hoc test scripts (30+ files)

## Naming Conventions

**Files:**
- Python modules: `snake_case.py` (e.g., `data_loader.py`, `factor_engine.py`)
- Configuration: `snake_case.yaml` (e.g., `scheduler.yaml`, `factors_config.yaml`)
- Data files: `{type}_{date}.{ext}` (e.g., `daily_picks_20260512.json`)
- K-line data: `{stock_code}.parquet` (e.g., `000001.parquet`)
- Pipeline scripts: `snake_case.py` matching task name in scheduler config
- Service directories: `snake_case_service/` (e.g., `data_service/`, `limit_service/`)

**Directories:**
- Service modules: `snake_case` (e.g., `data_service`, `stock_service`)
- Factor categories: `snake_case` (e.g., `technical`, `volume_price`, `market`)
- Test types: `unit`, `integration`, `performance`

## Where to Add New Code

**New Factor:**
1. Create `factors/{category}/my_factor.py` implementing `BaseFactor`
2. Add `@register_factor("my_factor")` decorator
3. Import in `factors/{category}/__init__.py`
4. Create config YAML in `config/factors/`

**New Filter:**
1. Create `filters/my_filter.py` implementing `BaseFilter`
2. Add `@register_filter("my_filter")` decorator
3. Import in `filters/__init__.py`
4. Create config YAML in `config/filters/`

**New Pipeline Task:**
1. Create `scripts/pipeline/my_task.py` with a `run() -> bool` entry point
2. Add task definition to `config/scheduler.yaml` with schedule, timeout, lock config
3. Add `requires_lock: true` and `lock_key` if concurrent execution must be prevented

**New Strategy:**
1. Create strategy YAML in `config/strategies/`
2. Optionally create service logic in `services/strategy_service/{strategy_name}/`
3. Reference from `core/strategy_engine.py` or `workflows/stock_selection_workflow.py`

**New Service Endpoint:**
1. Add route handler in the relevant `services/{service}/main.py`
2. Add business logic in `services/{service}/engine.py`
3. Add proxy route in `gateway/main.py`

**New Notification Channel:**
1. Create `services/notify_service/channels/my_channel.py`
2. Implement channel interface following existing patterns (e.g., `wechat.py`)
3. Register in `services/notify_service/signal_hub.py`

**New Data Fetcher:**
1. Create `services/data_service/fetchers/my_fetcher.py`
2. Implement fetcher class following `QuoteFetcher` or `KlineHistoryFetcher` patterns
3. Register in `services/data_service/main.py`

**Shared Utilities:**
- Infrastructure helpers: `core/utils/`
- Storage helpers: `core/storage/`
- New Pydantic models: `core/models.py`

## Special Directories

**`data/`:**
- Purpose: All runtime data (Parquet files, caches, checkpoints, task state)
- Generated: Yes (by pipeline scripts and services)
- Committed: Partially (some CSV results are committed; Parquet and DB files are typically in `.gitignore`)

**`logs/`:**
- Purpose: Application logs organized by subsystem
- Generated: Yes (auto-created by logger and pipeline scripts)
- Committed: No (should be in `.gitignore`)

**`great_expectations/`:**
- Purpose: Great Expectations data quality validation config
- Generated: Partially (by setup scripts)
- Committed: Yes

**`config/backups/`:**
- Purpose: Configuration file backups
- Generated: Yes
- Committed: Yes

---

*Structure analysis: 2026-05-12*
