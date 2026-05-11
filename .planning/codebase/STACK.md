# Technology Stack

**Analysis Date:** 2026-05-12

## Languages

**Primary:**
- Python 3.11 - Core application language, all services, scripts, models, and pipelines
- Target version `>=3.10` declared in `pyproject.toml`; CI tests against 3.11 and 3.12 (`.github/workflows/integration-tests.yml`)

**Secondary:**
- YAML - Pipeline/task scheduling configs (`config/scheduler.yaml`, `config/datasource.yaml`, `config/alerts.yaml`, etc.)
- SQL - Schema definitions (`sql/create_verification_tables.sql`, `sql/stock_pick_verification.sql`)
- Jinja2 - Report templates (`templates/`)
- Dockerfile - Container images (4 Dockerfiles at project root)

## Runtime

**Environment:**
- Python 3.11-slim (Docker containers)
- Timezone: Asia/Shanghai (all Dockerfiles)
- PYTHONUNBUFFERED=1 (all containers)

**Package Manager:**
- pip with `requirements.txt` (main) and `requirements_scheduler.txt` (scheduler service)
- No lockfile present (no pip freeze output or requirements.lock)
- pyproject.toml declares project metadata but does not use Poetry/PDM for dependency management

## Frameworks

**Core Web/API:**
- FastAPI >=0.100.0 - Primary API framework for microservices (data service, stock service, limit service, notify service, API gateway)
  - `services/data_service/main.py`
  - `services/stock_service/main.py`
  - `services/limit_service/main.py`
  - `services/notify_service/main.py`
  - `gateway/main.py`
- Flask 3.0.0 - Web monitoring dashboard and cache management API
  - `web/app_v2.py` (dashboard with WebSocket via flask-socketio)
  - `api/cache_api.py`
  - `services/data_service/scheduler/api/health.py`
- Uvicorn >=0.23.0 - ASGI server for FastAPI services
- Gunicorn - Production WSGI server for Flask app (Dockerfile.prod: `gunicorn -w 4 -b 0.0.0.0:5000`)

**Scheduling:**
- APScheduler 3.10.4 - Task scheduling with cron triggers (`services/data_service/scheduler/main.py`)
- Kestra - Orchestrator platform (optional, `docker-compose.yml` provisions kestra + PostgreSQL)
- Dual scheduler mode supported (`config/dual_scheduler.yaml`)

**Data Processing:**
- Polars 1.19.0 - Primary DataFrame library for factor calculation, data loading, backtesting (used extensively across `core/`, `factors/`, `services/`)
- Pandas 2.2.0 - Secondary DataFrame library for data source providers and legacy code
- PyArrow 18.1.0 - Parquet file I/O, columnar data interchange
- DuckDB >=0.10.0 - Analytical queries (`scripts/calculate_key_levels.py`)

**ML/Modeling:**
- scikit-learn - GradientBoostingClassifier for limit-up prediction (`models/opening_predictor.py`)
- XGBoost - Stock picker model (`scripts/xgboost_stock_picker.py`, `services/ml_service/predictor.py`)
- Pickle - Model serialization (`models/opening_predictor.py`)

**Testing:**
- pytest >=7.0.0 - Test runner (`pyproject.toml` testpaths = ["tests"])
- pytest-asyncio >=0.21.0 - Async test support
- pytest-cov - Coverage reporting (CI pipeline)

**Build/Dev:**
- Ruff - Linter configured in `pyproject.toml` (line-length=100, target=py310)
- Docker Compose - Multi-service orchestration (6 compose files for different deployments)
- GitHub Actions - CI pipeline (`.github/workflows/integration-tests.yml`)

## Key Dependencies

**Data Sources:**
- AKShare >=1.12.0 - Chinese A-share market data (stock lists, financial statements, dragon-tiger lists, announcements)
- Tushare Pro - Alternative market data source (lazy-imported in `core/data_adapter.py`)
- Baostock - Primary historical K-line data source (configured as primary in `config/datasource.yaml`)
- Tencent Finance (QQ) - Real-time quotes backup data source

**Database Drivers:**
- PyMySQL 1.1.0 - MySQL client for direct queries
- SQLAlchemy >=2.0.0 - ORM and query builder (`models/stock_pick_verification.py`, `scripts/fetch_history_klines.py`)

**Caching:**
- Redis 5.0.1 - Distributed cache (L2) and distributed locks
  - Two-level cache: L1=MemoryCache, L2=RedisCache (`core/cache/multi_level_cache.py`)
  - Distributed lock manager (`core/storage/lock_manager.py`, `core/distributed_lock.py`)

**Messaging:**
- kafka-python >=2.0.2 - Kafka consumer for real-time price events (KAFKA_TOPIC_NAME configured in `core/config.py`)

**Observability:**
- Loguru 0.7.2 - Structured logging (used throughout all modules)
- prometheus-client >=0.19.0 - Metrics exposition (`core/pipeline_monitor.py`)
- Prometheus - Metrics scraping and storage (`config/prometheus.yml`, Docker service)

**Validation:**
- Pydantic 2.5.0 + pydantic-settings 2.1.0 - Configuration and model validation (`core/config.py`, `core/models.py`)
- Great Expectations - Data quality validation (`great_expectations/`, `scripts/setup_ge.py`)

**HTTP:**
- httpx >=0.24.0 - Async HTTP client for inter-service communication in gateway
- aiohttp >=3.9.0 - Async HTTP (`core/async_http.py`)
- requests >=2.31.0 - Synchronous HTTP for notification webhooks

**Other:**
- Jinja2 >=3.0.0 - Report template rendering
- cachetools >=5.3.0 - In-memory caching utilities
- psutil >=5.9.0 - System resource monitoring
- tqdm >=4.65.0 - Progress bars for batch operations

## Configuration

**Environment:**
- `.env` file (secrets, DB credentials, API tokens) - loaded by pydantic-settings (`core/config.py`)
- `.env.example` documents all required variables
- YAML configs in `config/` directory (scheduler, datasource, alerts, filters, strategies)
- Nacos service discovery (`core/config.py` NacosClientSingleton)

**Build:**
- `Dockerfile` - Data fetcher service (Python 3.11-slim, runs `scripts/scheduled_fetch.py`)
- `Dockerfile.prod` - Production web app (Flask + Gunicorn on port 5000)
- `Dockerfile.scheduler` - APScheduler service (runs `services.data_service.scheduler.main`)
- `Dockerfile.cron` - Cron-based task runner (multi-stage build)
- `pyproject.toml` - Project metadata, pytest config, ruff config

## Platform Requirements

**Development:**
- Python >=3.10 (3.11 recommended)
- Docker + Docker Compose for local services (MySQL 8.0, Redis 7, Prometheus, Kestra)
- pip for dependency installation
- Tushare Pro token for primary data source (optional, Baostock is primary)
- Network access to Chinese financial data APIs

**Production:**
- Docker Compose orchestration with 6 services:
  - `xcnstock-fetcher` (data pipeline)
  - `redis` (Redis 7-alpine)
  - `mysql` (MySQL 8.0)
  - `prometheus` (monitoring)
  - `kestra` (workflow orchestrator, optional)
  - `kestra-db` (PostgreSQL 15 for Kestra)
- Additional microservices deployed as separate containers:
  - API Gateway (FastAPI, port 8000)
  - Data Service (FastAPI, port 8001)
  - Stock Service (FastAPI, port 8002)
  - Limit Service (FastAPI, port 8003)
  - Notify Service (FastAPI, port 8004)
  - Scheduler Service (Flask + APScheduler)
- Data storage: Parquet files on disk (`data/`), MySQL for relational data, Redis for cache/locks
- Linux server (Docker-based deployment)

---

*Stack analysis: 2026-05-12*
