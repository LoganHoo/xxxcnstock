# Phase 1: Scheduler Process Stability - Context

**Gathered:** 2026-05-12
**Status:** Ready for planning

<domain>
## Phase Boundary

全新实现一个统一的 APScheduler 调度器进程，替代当前两套实现（scripts/scheduler.py 和 services/data_service/scheduler/），确保调度进程持续运行不崩溃。

This phase delivers a single, stable scheduler process that:
- Runs 24/7 without crashes or hangs
- Survives Redis outages, task crashes, and subprocess failures
- Provides HTTP health check API for monitoring
- Merges all existing scheduler functionality into one clean implementation

</domain>

<decisions>
## Implementation Decisions

### Architecture
- **D-01:** 全新实现 — 不基于现有任一套代码，吸取两者优点重写
- **D-02:** 双进程架构 — 调度引擎进程 + 独立 HTTP API 进程
- **D-03:** 统一配置到 `config/scheduler.yaml`，废弃 `config/cron_tasks.yaml`
- **D-04:** 调度引擎使用 APScheduler BlockingScheduler（已验证的模式）

### Current System Analysis
- **D-05:** 现有 scripts/scheduler.py 功能最全（重试/熔断/历史/锁/进度检查/信号处理），但代码混乱（1147行单文件）
- **D-06:** 现有 services/data_service/scheduler/ 有 Flask API 层和模块化结构，但功能不全
- **D-07:** 崩溃原因不明确 — 进程崩溃和脚本报错都有，需要直接修复而非先加可观测性

### Resilience Requirements
- **D-08:** Redis 不可用时必须降级运行（不崩溃），参考现有降级模式
- **D-09:** 单个任务崩溃不能影响调度器进程
- **D-10:** 子进程超时必须强制终止，不能留下僵尸进程

### Claude's Discretion
- 新调度器的模块结构和代码组织方式
- 具体的错误恢复策略
- HTTP API 的端点设计和返回格式
- 进程间通信方式（调度引擎 ↔ HTTP API）
- 旧调度器代码的迁移/清理计划

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Current Scheduler Implementations (must read to understand what to preserve)
- `scripts/scheduler.py` — Primary scheduler with full feature set (retry, circuit breaker, history DB, distributed locks, health check, signal handling, subprocess timeout)
- `services/data_service/scheduler/main.py` — Flask-based scheduler service entry point
- `services/data_service/scheduler/scheduler_service.py` — SchedulerService class abstraction
- `services/data_service/scheduler/tasks/executor.py` — TaskExecutor module
- `services/data_service/scheduler/locks/redis_lock.py` — RedisLockManager module
- `services/data_service/scheduler/api/health.py` — HealthCheckAPI Flask routes

### Configuration Files
- `config/scheduler.yaml` — Current Flask scheduler config (to be unified)
- `config/cron_tasks.yaml` — Current scripts/scheduler.py config (to be deprecated)

### Supporting Code
- `core/distributed_lock.py` — DistributedLock implementation used by current scheduler
- `core/logger.py` — Logger setup utility

### Codebase Architecture
- `.planning/codebase/ARCHITECTURE.md` — Full architecture analysis
- `.planning/codebase/CONCERNS.md` — Cross-cutting concerns (hardcoded creds, config issues)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **DistributedLock (core/distributed_lock.py):** Already implements Redis-based distributed locking with auto-renew. New scheduler should reuse this.
- **HistoryDB pattern (scripts/scheduler.py:122-228):** SQLite-based execution history with WAL mode, thread-safe access. Pattern is solid, should be extracted to a module.
- **TaskDistributedLock context manager (scripts/scheduler.py:323-369):** Clean lock management with Redis-downgrade fallback. Reuse pattern.
- **Circuit breaker state management (scripts/scheduler.py:528-619):** JSON-based circuit breaker with half-open recovery. Reuse concept.
- **Signal handling (scripts/scheduler.py:1021-1051):** Proper graceful shutdown with threading.Event. Reuse approach.
- **HealthHandler (scripts/scheduler.py:949-1004):** HTTP health check with /health, /tasks, /stats endpoints. Reuse pattern.

### Established Patterns
- **Subprocess execution:** All pipeline tasks run as subprocess (not in-process), which isolates crashes. Keep this pattern.
- **APScheduler BlockingScheduler:** Works well for cron-based scheduling with max_instances=1 per task. Keep this.
- **YAML config with global/task sections:** The config structure (global defaults + per-task overrides) is good. Keep this pattern.

### Integration Points
- **Pipeline scripts (scripts/pipeline/*.py):** New scheduler will invoke these as subprocesses — they are the "jobs"
- **Redis (host from env):** Required for distributed locks — must handle unavailability gracefully
- **SQLite (data/scheduler_history.db):** Execution history storage — new scheduler should continue using this path
- **Health check endpoint:** External monitoring (Prometheus, Docker healthcheck) hits this

</code_context>

<specifics>
## Specific Ideas

- User wants clean, modular code — not another 1147-line monolith
- Dual process: scheduler engine (runs jobs) and API process (serves HTTP)
- Must preserve all existing functionality from scripts/scheduler.py (retry, circuit breaker, history, locks, progress check, market calendar)
- Config should be a single unified file (scheduler.yaml format preferred)

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope
</deferred>

---

*Phase: 1-Scheduler Process Stability*
*Context gathered: 2026-05-12*
