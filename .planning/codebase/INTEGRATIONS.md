# External Integrations

**Analysis Date:** 2026-05-12

## APIs & External Services

**Chinese A-Share Market Data:**
- Baostock - Primary data source for historical K-lines, fundamentals, and stock lists
  - SDK/Client: `baostock` (lazy-imported in `core/data_adapter.py`)
  - Config: `config/datasource.yaml` (priority 1, primary)
  - Auth: No token required, auto-login
  - Usage: Historical K-line data, fundamental data, index data
- AKShare - Stock lists, financial statements, dragon-tiger board, announcements, market behavior
  - SDK/Client: `akshare` (>=1.12.0)
  - Usage sites: `services/data_service/fetchers/financial/`, `services/data_service/fetchers/market_behavior/`, `services/data_service/fetchers/announcement/`
  - Config: `config/datasource.yaml` (currently disabled for K-line, enabled for stock lists)
  - Auth: No token required
- Tushare Pro - Alternative data source for market data
  - SDK/Client: `tushare` (lazy-imported in `core/data_adapter.py`)
  - Auth: `TUSHARE_TOKEN` env var
  - Config: `.env` (TUSHARE_TOKEN)
- Tencent Finance (QQ) - Backup real-time quote and K-line data
  - HTTP: `https://qt.gtimg.cn`, `https://web.ifzq.gtimg.cn`
  - Config: `config/datasource.yaml` (backup priority 1)

**AI Services:**
- Google Gemini AI - Market analysis and sentiment
  - Config: `core/config.py` (GEMINI_API_KEY, GEMINI_MODEL, GEMINI_MODEL_FALLBACK1, GEMINI_MODEL_FALLBACK2)
  - Models: gemini-3.1-pro-preview, gemini-3-flash, gemini-2.5-flash
  - Feature flags: GEMINI_ENABLED, GEMINI_ANALYSIS_ENABLED
  - Auth: `GEMINI_API_KEY` env var
- Internal AI API - Email sending and analysis
  - HTTP: `http://192.168.1.168:2000` (configured in `core/config.py`)
  - Endpoints: `/send_email`
  - Feature flags: AI_FALLBACK_ENABLED

**Service Discovery:**
- Nacos - Service discovery and dynamic configuration
  - SDK/Client: `nacos` (imported in `core/config.py` NacosClientSingleton)
  - Server: `NACOS_SERVER_ADDR` (default in config)
  - Auth: `NACOS_USERNAME`, `NACOS_PASSWORD`
  - Namespace: `NACOS_NAMESPACE`

## Data Storage

**Databases:**
- MySQL 8.0 - Primary relational database
  - Connection: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` env vars
  - Client: PyMySQL (direct), SQLAlchemy (ORM)
  - Schema files: `sql/create_verification_tables.sql`, `sql/stock_pick_verification.sql`
  - Config: `core/config.py` (DB_* settings), `docker-compose.yml` (MySQL 8.0 container)
  - Pool: Connection pool with configurable size (`DB_POOL_SIZE` default 10)
  - Tables: Stock pick verification, selection results, fund behavior data

**File Storage:**
- Local filesystem - Parquet files for analytical data
  - Path: `data/` directory (mounted as Docker volume)
  - Format: Parquet (primary), CSV (secondary), JSON (reports)
  - Key files: K-line data, factor data, CVD indicators, stock lists
  - Utilities: `core/storage/parquet_utils.py`

**Caching:**
- Redis 7 (Alpine) - Two-tier distributed cache
  - L1: In-memory (MemoryCache, TTL 3600s, max 1000 entries)
  - L2: Redis (RedisCache, TTL 86400s)
  - Client: `redis` library
  - Config: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB` env vars
  - Additional usage: Distributed locks (`core/distributed_lock.py`, `core/storage/lock_manager.py`)

**Analytical Engine:**
- DuckDB - Embedded analytical database for ad-hoc queries
  - Usage: `scripts/calculate_key_levels.py`

## Authentication & Identity

**Auth Provider:**
- Custom / API-key based
  - Implementation: `API_SECRET_KEY`, `API_ACCESS_TOKEN` in `.env`
  - No OAuth/JWT framework detected

**Notification Auth:**
- WeChat: `WECHAT_SEND_KEY` env var
- DingTalk: `DINGTALK_WEBHOOK`, `DINGTALK_SECRET` env vars
- Email: `EMAIL_SMTP_SERVER`, `EMAIL_SMTP_PORT`, `EMAIL_USERNAME`, `EMAIL_PASSWORD` env vars

## Monitoring & Observability

**Error Tracking:**
- None detected (no Sentry, Rollbar, or similar)

**Logs:**
- Loguru for structured logging across all modules
  - Log files: `logs/` directory (mounted as Docker volume)
  - Config: `core/logger.py`
  - Per-module log files (e.g., `system/gateway.log`, `system/data_service.log`)

**Metrics:**
- Prometheus + prometheus_client
  - Config: `config/prometheus.yml`
  - Instrumentation: `core/pipeline_monitor.py` (Counter, Histogram, Gauge, Info)
  - Scrape endpoint: Exposed via prometheus_client
  - Docker service: `xcnstock-prometheus` container

**Health Checks:**
- All FastAPI services expose `/health` endpoints
- Flask production app: Docker HEALTHCHECK on `http://localhost:5000/api/health`
- Redis: Docker healthcheck via `redis-cli ping`
- MySQL: Docker healthcheck via `mysqladmin ping`

## CI/CD & Deployment

**Hosting:**
- Docker Compose on Linux server
- Multiple compose files for different deployment scenarios:
  - `docker-compose.yml` - Development (fetcher, Redis, MySQL, Prometheus, Kestra)
  - `docker-compose.prod.yml` - Production (Flask app, web dashboard, Nginx, Redis, MySQL, scheduler, data collector)
  - `docker-compose.scheduler.yml` - APScheduler standalone
  - `docker-compose.cron.yml` - Cron-based scheduling
  - `docker-compose.dual-scheduler.yml` - APScheduler + Kestra dual mode

**CI Pipeline:**
- GitHub Actions (`.github/workflows/integration-tests.yml`)
  - Triggers: push to main/develop, PR to main, daily cron at 02:00
  - Matrix: Python 3.11 and 3.12
  - Stages: unit tests, integration tests, data freshness check, workflow tests, performance tests
  - Coverage: pytest-cov with Codecov upload

**CD Pipeline:**
- Not detected (manual Docker Compose deployment assumed)

## Environment Configuration

**Required env vars:**
- `TUSHARE_TOKEN` - Tushare Pro API token
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` - MySQL connection
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB` - Redis connection
- `API_SECRET_KEY`, `API_ACCESS_TOKEN` - API authentication
- `WECHAT_SEND_KEY` - WeChat notification key
- `DINGTALK_WEBHOOK`, `DINGTALK_SECRET` - DingTalk notification
- `EMAIL_SMTP_SERVER`, `EMAIL_USERNAME`, `EMAIL_PASSWORD` - Email SMTP
- `GEMINI_API_KEY` - Google Gemini AI
- `NACOS_SERVER_ADDR`, `NACOS_USERNAME`, `NACOS_PASSWORD` - Nacos service discovery

**Optional env vars:**
- `PARALLEL_MAX_CONCURRENT`, `PARALLEL_BATCH_SIZE`, `PARALLEL_CALLS_PER_MINUTE` - Parallel fetching config
- `CACHE_L1_MAXSIZE`, `CACHE_L1_TTL`, `CACHE_L2_TTL` - Cache tuning
- `POLARS_MAX_THREADS`, `POLARS_STREAMING` - Polars performance
- `PROMETHEUS_ENABLED`, `PROMETHEUS_PORT` - Monitoring toggle
- `ALERT_WEBHOOK_URL` - Alert webhook destination
- `SCHEDULER_PRIMARY`, `SCHEDULER_FAILOVER_ENABLED` - Dual scheduler config
- `KAFKA_BASE_URL`, `KAFKA_CLUSTER_NAME`, `KAFKA_TOPIC_NAME` - Kafka integration

**Secrets location:**
- `.env` file at project root (committed to `.gitignore`)
- Nacos for dynamic configuration (credentials in `.env`)

## Webhooks & Callbacks

**Incoming:**
- None detected

**Outgoing:**
- WeChat Work (Server酱) - `WECHAT_SEND_KEY` based push
- DingTalk - Webhook with signed secret (`DINGTALK_WEBHOOK` + `DINGTALK_SECRET`)
- Feishu - Supported in `core/notification/notifier.py` (webhook URL not in config)
- Email - SMTP or internal API (`EMAIL_API_URL`)
- Generic Webhook - `ALERT_WEBHOOK_URL` for alerts

## Data Format Standards

**Primary Data Format:**
- Parquet files for time-series and analytical data (Polars-native)
- MySQL for relational/transactional data
- Redis for cache and distributed locks

**Inter-Service Communication:**
- REST/HTTP (FastAPI services communicate via httpx async client in gateway)
- Kafka for real-time price event streaming (KAFKA_TOPIC_NAME configured)

---

*Integration audit: 2026-05-12*
