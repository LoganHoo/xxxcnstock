# Phase 1: Scheduler Process Stability - Research

**Researched:** 2026-05-12
**Domain:** APScheduler 3.x process management, subprocess isolation, dual-process architecture
**Confidence:** HIGH

## Summary

This phase replaces two existing scheduler implementations with a single, stable scheduler process. The current system has two competing schedulers: `scripts/scheduler.py` (1147 lines, feature-complete but monolithic) and `services/data_service/scheduler/` (modular but incomplete). The new implementation must preserve all functionality from the scripts version (retry, circuit breaker, history, locks, progress check, market calendar, signal handling) while adopting a dual-process architecture: a BlockingScheduler engine process for job execution and a separate HTTP API process for monitoring.

The core technical challenge is process stability -- the scheduler must survive Redis outages, task crashes, subprocess timeouts, and memory leaks without the main process dying. APScheduler 3.11.2's BlockingScheduler is the right choice because it runs jobs in a ThreadPoolExecutor, meaning individual job failures (including subprocess crashes) are isolated from the scheduler's main loop. The dual-process split means the API server cannot crash the scheduler engine.

**Primary recommendation:** Use APScheduler 3.11.2 BlockingScheduler with subprocess-based task execution. Split into two processes communicating via a shared SQLite database and a simple HTTP status file or Unix socket. Reuse `core/distributed_lock.py` as-is for Redis-based locking.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Brand new implementation -- not based on either existing codebase, take best patterns from both
- **D-02:** Dual process architecture -- scheduler engine process + independent HTTP API process
- **D-03:** Unified config to `config/scheduler.yaml`, deprecate `config/cron_tasks.yaml`
- **D-04:** Scheduler engine uses APScheduler BlockingScheduler (verified pattern)

### Current System Analysis
- **D-05:** Existing `scripts/scheduler.py` has the most features (retry/circuit breaker/history/locks/progress check/signal handling), but messy code (1147 lines single file)
- **D-06:** Existing `services/data_service/scheduler/` has Flask API layer and modular structure, but incomplete features
- **D-07:** Crash causes unclear -- both process crashes and script errors occur, need direct fixing not observability first

### Resilience Requirements
- **D-08:** Redis unavailability must degrade gracefully (no crash), reference existing degradation pattern
- **D-09:** Single task crash must not affect scheduler process
- **D-10:** Subprocess timeout must force-kill, no zombie processes

### Claude's Discretion
- New scheduler module structure and code organization
- Specific error recovery strategies
- HTTP API endpoint design and return format
- Inter-process communication method (scheduler engine <-> HTTP API)
- Old scheduler code migration/cleanup plan

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCHED-01 | Scheduler process stability -- APScheduler service starts and stays running without crashes or hangs | APScheduler 3.11.2 BlockingScheduler isolation model, subprocess execution pattern, dual-process architecture, Redis degradation pattern, signal handling approach |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Cron scheduling & job dispatch | Scheduler Engine Process | -- | BlockingScheduler owns the timing loop |
| Task execution (subprocess) | Scheduler Engine Process | -- | Subprocess isolation keeps engine safe |
| Retry / circuit breaker / history | Scheduler Engine Process | -- | These run inside job callbacks |
| Distributed locking | Scheduler Engine Process | Redis (external) | Lock is per-task, engine manages lifecycle |
| Health check / status API | HTTP API Process | -- | Separate process so API bugs cannot crash engine |
| Configuration loading | Scheduler Engine Process | -- | Engine reads YAML at startup |
| Signal handling (SIGTERM/SIGINT) | Scheduler Engine Process | -- | Engine controls graceful shutdown |
| Inter-process state sharing | SQLite DB (shared) | -- | Both processes read/write history DB |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| APScheduler | 3.11.2 | Cron scheduling engine | Already installed, battle-tested, BlockingScheduler proven in current system [VERIFIED: pip show] |
| redis | 7.4.0 | Distributed locking backend | Already installed, used by core/distributed_lock.py [VERIFIED: pip show] |
| PyYAML | 6.0.3 | Configuration loading | Already installed, both existing configs use YAML [VERIFIED: pip show] |
| loguru | 0.7.3 | Logging | Already used throughout codebase via core/logger.py [VERIFIED: pip show] |
| Flask | 3.1.3 | HTTP API server | Already installed, used by existing services/data_service/scheduler/api [VERIFIED: pip show] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| subprocess (stdlib) | -- | Task script execution | Every task invocation -- isolates crashes |
| sqlite3 (stdlib) | -- | Execution history persistence | Recording task start/complete/fail |
| threading (stdlib) | -- | Concurrency primitives | Locks for shared state, shutdown events |
| signal (stdlib) | -- | Graceful shutdown | SIGTERM/SIGINT handling |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Flask for API | FastAPI (already in project) | Flask is simpler for this use case (health/status only, no async needed), and existing health.py uses Flask already |
| sqlite3 for history | SQLAlchemy | Overkill for this single-table use case; raw sqlite3 with WAL mode is proven in existing code |
| BlockingScheduler | BackgroundScheduler | BlockingScheduler is the correct choice when scheduler IS the process (per APScheduler docs); BackgroundScheduler is for embedding in other apps |
| Subprocess execution | In-process execution | Subprocess is non-negotiable for crash isolation (D-10) |

**Installation:**
No new packages needed -- all dependencies are already installed.

**Version verification:**
```
APScheduler==3.11.2  (installed, verified)
redis==7.4.0         (installed, verified)
PyYAML==6.0.3        (installed, verified)
Flask==3.1.3         (installed, verified)
loguru==0.7.3        (installed, verified)
```

## Architecture Patterns

### System Architecture Diagram

```
                    config/scheduler.yaml
                           |
                           v
                  +------------------+
                  | Scheduler Engine |
                  | (Process 1)      |
                  |                  |
                  | BlockingScheduler|
                  |   |              |
                  |   v              |
                  | ThreadPoolExec   |
                  |   |  |  |       |
                  |   v  v  v       |
                  | Job1 Job2 Job3  |-----> subprocess.Popen()
                  |   |              |          |
                  |   v              |          v
                  | retry/circuit   |    scripts/pipeline/*.py
                  | breaker/history |          |
                  |   |              |          v
                  |   v              |    (return code)
                  | SQLite HistoryDB|
                  |   |              |
                  |   v              |
                  | DistributedLock |
                  +-------+----------+
                          |
                    (shared SQLite DB)
                          |
                  +-------+----------+
                  | HTTP API         |
                  | (Process 2)      |
                  |                  |
                  | Flask /health    |
                  | Flask /tasks     |
                  | Flask /stats     |
                  +------------------+
                          |
                          v
                  External monitoring
                  (Docker healthcheck,
                   Prometheus, curl)
```

### Recommended Project Structure
```
services/data_service/scheduler/
    __init__.py
    engine.py              # BlockingScheduler main loop, signal handling
    config.py              # YAML config loader, task model
    executor.py            # Subprocess execution with timeout/progress
    history.py             # SQLite history database (extracted from scripts/scheduler.py)
    lock_manager.py        # Wraps core/distributed_lock.py for task-level locking
    circuit_breaker.py     # Circuit breaker state management
    calendar.py            # Market calendar / day_type logic
    api/
        __init__.py
        app.py             # Flask app factory
        routes.py          # Health/tasks/stats endpoints
    run_engine.py          # Entry point for scheduler engine process
    run_api.py             # Entry point for API process
```

### Pattern 1: BlockingScheduler with Subprocess Isolation
**What:** Each task runs as `subprocess.Popen()` inside a ThreadPoolExecutor job. The scheduler never executes pipeline scripts in-process.
**When to use:** Always -- this is the core execution model.
**Example:**
```python
# Source: APScheduler 3.x docs + existing scripts/scheduler.py pattern
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess

scheduler = BlockingScheduler(timezone='Asia/Shanghai')

def run_task(task_config):
    """Job callback -- runs in ThreadPoolExecutor, isolated from scheduler loop."""
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            timeout=task_config['timeout'],
            cwd=str(project_root),
            env=build_env(task_config),
            text=True
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        # Kill subprocess, scheduler keeps running
        process.kill()
        process.wait()
        return False
    except Exception:
        # Any error in job callback is swallowed by APScheduler
        # EVENT_JOB_ERROR listener gets notified
        return False

scheduler.add_job(
    run_task, trigger=CronTrigger.from_crontab(schedule),
    args=[task_config], id=task_name,
    max_instances=1, replace_existing=True
)
scheduler.start()  # Blocks forever
```

### Pattern 2: Redis Degradation (Lock Graceful Fallback)
**What:** When Redis is unavailable, skip locking and execute without lock (degraded mode).
**When to use:** Every lock acquisition attempt.
**Example:**
```python
# Source: existing scripts/scheduler.py TaskDistributedLock pattern (lines 323-369)
def acquire_lock_or_degrade(lock_key, timeout):
    try:
        redis_client = get_redis_client()
        lock = DistributedLock(redis_client, lock_key, ttl_seconds=timeout, auto_renew=True)
        acquired = lock.acquire(blocking=False)
        if acquired:
            return lock  # Normal mode
        else:
            return None  # Locked by another instance, skip
    except redis.ConnectionError:
        logger.warning(f"Redis unavailable, running in degraded mode [{lock_key}]")
        return True  # Degraded: allow execution without lock
    except Exception as e:
        logger.warning(f"Lock error: {e}, allowing execution [{lock_key}]")
        return True  # Fail open
```

### Pattern 3: Dual Process with Shared State via SQLite
**What:** Engine process writes execution history to SQLite WAL-mode database. API process reads from same database.
**When to use:** All inter-process state sharing.
**Example:**
```python
# Source: existing scripts/scheduler.py HistoryDB pattern (lines 122-228)
# Engine process writes:
with history_db_lock:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('INSERT INTO task_executions ...', (task_name, start_time, 'running'))

# API process reads (no lock contention in WAL mode):
conn = sqlite3.connect(db_path, timeout=30)
conn.execute('PRAGMA journal_mode=WAL')  # Readers don't block writers
cursor = conn.execute('SELECT * FROM task_executions WHERE ...')
```

### Pattern 4: Graceful Shutdown via Signal + Threading.Event
**What:** SIGTERM/SIGINT sets a threading.Event, scheduler checks it before each task.
**When to use:** Always -- required for Docker/container orchestration.
**Example:**
```python
# Source: existing scripts/scheduler.py signal_handler (lines 1021-1051)
import signal
import threading

shutdown_event = threading.Event()

def signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}, initiating graceful shutdown...")
    shutdown_event.set()
    # Shutdown scheduler in separate thread to avoid deadlock in signal handler
    threading.Thread(target=scheduler.shutdown, kwargs={'wait': True}, daemon=True).start()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
```

### Anti-Patterns to Avoid
- **Calling sys.exit() in signal handler:** Causes deadlocks with APScheduler's shutdown. Use threading.Event instead. [VERIFIED: existing scripts/scheduler.py fixed this in lines 1021-1051]
- **Running pipeline scripts in-process:** Any import of pipeline code into the scheduler process creates crash coupling. Always use subprocess. [VERIFIED: D-10 decision]
- **Blocking Redis calls without timeout:** Default redis-py socket_timeout is None (infinite). Must set socket_connect_timeout and socket_timeout. [VERIFIED: existing code uses 3s connect / 5s read]
- **BackgroundScheduler for standalone scheduler:** BackgroundScheduler is for embedding in other apps. For a dedicated scheduler process, use BlockingScheduler. [CITED: APScheduler 3.x docs]
- **Sharing Lock objects between processes:** Redis locks are process-safe by design. Python threading locks are NOT cross-process. [ASSUMED]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Distributed locking | Custom Redis SET/DEL | `core/distributed_lock.py` (DistributedLock class) | Already has auto-renewal, Lua-based atomic release, UUID-based ownership |
| Cron trigger parsing | Custom cron parser | `APScheduler CronTrigger.from_crontab()` | Handles all cron syntax, timezone-aware |
| Thread-safe state | Custom lock management | `threading.RLock()` + existing pattern | Reentrant locks prevent deadlock in nested calls |
| SQLite concurrent access | Custom file locking | SQLite WAL mode + threading.RLock | WAL allows concurrent reads, RLock serializes writes |
| Subprocess timeout | Manual timing loop | `subprocess.run(timeout=N)` or `Popen` + poll | Already proven in existing code |

**Key insight:** The existing codebase has solid implementations of distributed locking (`core/distributed_lock.py`) and history management (`scripts/scheduler.py HistoryDB`). The new scheduler should extract and reuse these patterns, not rewrite them.

## Common Pitfalls

### Pitfall 1: APScheduler ThreadPoolExecutor Exhaustion
**What goes wrong:** Default ThreadPoolExecutor has 10 threads. If 10 long-running tasks overlap, new tasks are queued and may misfire.
**Why it happens:** APScheduler's default max_workers=10. Tasks like `data_collect_with_validation.py` (timeout=3600s) can hold threads for an hour.
**How to avoid:** Set explicit executor config with appropriate max_workers. Use `max_instances=1` per job. Monitor `EVENT_JOB_MAX_INSTANCES` events.
**Warning signs:** Tasks not firing at scheduled time, misfire events in logs.

### Pitfall 2: SQLite "database is locked" Under Concurrent Writes
**What goes wrong:** Multiple APScheduler threads try to write history simultaneously, causing OperationalError.
**Why it happens:** SQLite default journal mode uses exclusive locks on write.
**How to avoid:** Use WAL mode (`PRAGMA journal_mode=WAL`), `PRAGMA synchronous=NORMAL`, and a threading.RLock around all write operations. Existing code already does this correctly (lines 131-133, 78-79 of scripts/scheduler.py).
**Warning signs:** `OperationalError: database is locked` in logs.

### Pitfall 3: Zombie Subprocess After Timeout
**What goes wrong:** `subprocess.run(timeout=N)` raises TimeoutExpired but the child process keeps running.
**Why it happens:** `subprocess.run()` sends SIGTERM on timeout but doesn't guarantee process death. Child may spawn its own children that survive.
**How to avoid:** Use `Popen` with explicit terminate -> wait(5) -> kill() sequence. Use process groups for child cleanup. Existing code does this correctly (lines 657-721 of scripts/scheduler.py).
**Warning signs:** `ps aux | grep python` shows stale pipeline processes.

### Pitfall 4: Redis Connection Not Recovering After Outage
**What goes wrong:** Redis goes down, scheduler catches the error once, but subsequent lock attempts still fail because the connection pool is poisoned.
**Why it happens:** redis-py ConnectionPool caches broken connections.
**How to avoid:** Create a new Redis client on each lock attempt (or use connection pool with health checks). Existing code uses a global pool -- new implementation should create fresh connections or call `pool.reset()` on ConnectionError.
**Warning signs:** "Connection refused" errors persisting after Redis is back up.

### Pitfall 5: Signal Handler Deadlock with BlockingScheduler
**What goes wrong:** Calling `scheduler.shutdown(wait=True)` directly in a signal handler deadlocks because the scheduler's internal lock is held.
**Why it happens:** Signal handlers run in the main thread. BlockingScheduler.start() also runs in the main thread. Calling shutdown from signal context interrupts the scheduler's internal state machine.
**How to avoid:** Use `threading.Thread(target=scheduler.shutdown, daemon=True).start()` in signal handler, or use `scheduler.shutdown(wait=False)` followed by setting a shutdown event. Existing code handles this correctly (lines 1035-1045).
**Warning signs:** Process hangs on SIGTERM, requires SIGKILL.

### Pitfall 6: Config Divergence Between Two Config Files
**What goes wrong:** `config/scheduler.yaml` (34 tasks, simpler structure) and `config/cron_tasks.yaml` (40+ tasks, richer features like `depends_on`, `day_type`, `circuit_breaker`, `progress_check`) drift out of sync.
**Why it happens:** Two humans maintaining two files with overlapping but different schemas.
**How to avoid:** Merge into single `config/scheduler.yaml` that supports ALL fields from `cron_tasks.yaml`. The unified config must include: `depends_on`, `day_type`, `circuit_breaker`, `progress_check`, `run_once`, `alert_on_failure`, `priority`, `optional`, `env`, `args`, `market_calendar` in global section.
**Warning signs:** Tasks missing from one config, different schedules for same task.

## Code Examples

### Unified Config Schema (merged from both files)
```yaml
# config/scheduler.yaml - unified schema
scheduler:
  timezone: "Asia/Shanghai"
  max_workers: 4
  use_redis_lock: true
  redis_lock_timeout: 1800
  max_retries: 3
  retry_delay: 60
  retry_backoff: 2
  max_backoff: 600
  health_check_port: 8080
  api_port: 5001
  market_calendar:
    enabled: true
    special_holidays:
      - "2026-01-01"
      # ... full list from cron_tasks.yaml

tasks:
  - name: "data_fetch"
    description: "data collection"
    script: "scripts/pipeline/data_collect_with_validation.py"
    schedule: "0 16 * * 1-5"
    enabled: true
    timeout: 3600
    day_type: "weekday"          # daily | weekday | weekend
    depends_on: "system_health_check"
    run_once: true
    alert_on_failure: true
    priority: "critical"
    optional: false
    args: []
    env: {}
    progress_check:
      enabled: true
      interval: 600
      min_progress: 10
    circuit_breaker:
      enabled: true
      failure_threshold: 3
      recovery_timeout: 3600
      fallback_script: "scripts/pipeline/data_audit_fallback.py"

lock:
  redis:
    host: "${REDIS_HOST:-localhost}"
    port: ${REDIS_PORT:-6379}
    password: "${REDIS_PASSWORD:-}"
```

### Subprocess Execution with Timeout and Progress Check
```python
# Source: extracted from scripts/scheduler.py run_subprocess_with_timeout (lines 641-721)
def run_subprocess_with_timeout(cmd, timeout, cwd, env=None, task_name=None,
                                progress_config=None, max_output_size=10*1024*1024):
    """Execute subprocess with timeout, progress monitoring, and output limits."""
    process = None
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=cwd, env=env, text=True, bufsize=1)
        start_time = time.time()
        while True:
            if process.poll() is not None:
                break
            if time.time() - start_time > timeout:
                raise subprocess.TimeoutExpired(cmd, timeout)
            # Optional: check progress file for stall detection
            if progress_config and progress_config.get('enabled') and task_name:
                check_progress_stall(task_name, progress_config)
            time.sleep(1)

        stdout, stderr = process.communicate()  # already done, just collect
        return process.returncode, stdout[:max_output_size], stderr[:max_output_size]

    except subprocess.TimeoutExpired:
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise
    except Exception:
        if process:
            try:
                process.kill()
                process.wait()
            except Exception:
                pass
        raise
```

### APScheduler Event Listener for Crash Detection
```python
# Source: APScheduler 3.x events docs + existing pattern
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

def job_error_listener(event):
    """Called when any job raises an exception."""
    if event.exception:
        logger.error(f"Job {event.job_id} failed: {event.exception}")
        logger.debug(f"Traceback: {event.traceback}")
    # Scheduler keeps running -- this is the isolation guarantee

def job_missed_listener(event):
    """Called when a job is missed (misfire)."""
    logger.warning(f"Job {event.job_id} missed scheduled run at {event.scheduled_run_time}")

scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
scheduler.add_listener(job_missed_listener, EVENT_JOB_MISSED)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| APScheduler 3.x | Still current (3.11.2) | APScheduler 4.x is in beta but not stable | Stay on 3.x -- it's battle-tested and what the project already uses |
| BackgroundScheduler + Flask in same process | Separate processes | Design decision D-02 | Eliminates risk of Flask crash killing scheduler |
| Raw HTTP server (http.server) | Flask for API | Already in services/ | Flask is more robust, handles edge cases better |

**Deprecated/outdated:**
- `http.server.HTTPServer` for health checks (used in scripts/scheduler.py): Too basic, no error handling. Use Flask instead.
- `redis-py` lock() built-in (used in services/data_service/scheduler/locks/redis_lock.py): Use `core/distributed_lock.py` which has auto-renewal and proper Lua-based release.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | APScheduler 4.x is not production-ready | Standard Stack | Would mean we should consider migrating to 4.x |
| A2 | sqlite3 WAL mode works across processes on the same machine | Architecture Patterns | Would need different IPC mechanism |
| A3 | subprocess.Popen with terminate/kill is sufficient for zombie cleanup | Common Pitfalls | May need process groups or PID tracking |
| A4 | The scheduler runs on a single machine (no multi-node distribution) | Architecture | Would change locking requirements |

**If this table is empty:** All claims in this research were verified or cited -- no user confirmation needed.

## Open Questions

1. **APScheduler 4.x readiness**
   - What we know: APScheduler 4.x exists but has breaking API changes. Current project uses 3.11.2.
   - What's unclear: Whether 4.x is stable enough for production use.
   - Recommendation: Stay on 3.11.2 -- it works and is already installed. Migration to 4.x can be a future phase.

2. **Process manager for dual-process architecture**
   - What we know: Need two processes (engine + API). Docker Compose or systemd could manage them.
   - What's unclear: How the deployment starts/stops both processes together.
   - Recommendation: Use a simple process manager script or Docker Compose with two services. The engine process is the primary; the API process is secondary.

3. **cron_tasks.yaml features not in scheduler.yaml**
   - What we know: cron_tasks.yaml has `depends_on`, `day_type`, `circuit_breaker`, `progress_check`, `run_once`, `priority`, `optional`, `env`, `market_calendar`, `groups`, `planned_tasks`. scheduler.yaml has simpler task definitions.
   - What's unclear: Which of these features are actually used in production vs. declared but unused.
   - Recommendation: Implement all of them in the unified config since they're all referenced by scripts/scheduler.py.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.13 | Scheduler engine | Yes | 3.13.13 | -- |
| APScheduler | Cron scheduling | Yes | 3.11.2 | -- |
| redis-py | Distributed locking | Yes | 7.4.0 | Degraded mode (no lock) |
| Flask | HTTP API | Yes | 3.1.3 | -- |
| PyYAML | Config loading | Yes | 6.0.3 | -- |
| loguru | Logging | Yes | 0.7.3 | -- |
| Redis server | Lock backend | Yes (remote) | -- | Degraded mode |
| sqlite3 | History DB | Yes (stdlib) | -- | -- |
| pytest | Testing | Yes | 9.0.3 | -- |
| Docker | Deployment | Yes | 29.4.3 | -- |

**Missing dependencies with no fallback:**
None -- all required dependencies are installed.

**Missing dependencies with fallback:**
- Redis server: Falls back to degraded mode (no distributed locking). This is by design (D-08).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.3 |
| Config file | pyproject.toml [tool.pytest.ini_options] |
| Quick run command | `pytest tests/ -x -q --tb=short -k "scheduler"` |
| Full suite command | `pytest tests/ -v --tb=long` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCHED-01 | Scheduler starts and stays running | unit | `pytest tests/test_scheduler_stability.py -x` | No -- Wave 0 |
| SCHED-01 | Single task crash does not kill scheduler | unit | `pytest tests/test_scheduler_stability.py::test_task_crash_isolation -x` | No -- Wave 0 |
| SCHED-01 | Redis unavailability does not crash scheduler | unit | `pytest tests/test_scheduler_stability.py::test_redis_degradation -x` | No -- Wave 0 |
| SCHED-01 | Subprocess timeout kills child process | unit | `pytest tests/test_scheduler_stability.py::test_subprocess_timeout -x` | No -- Wave 0 |
| SCHED-01 | SIGTERM causes graceful shutdown | unit | `pytest tests/test_scheduler_stability.py::test_graceful_shutdown -x` | No -- Wave 0 |
| SCHED-01 | Config loads all task definitions | unit | `pytest tests/test_scheduler_config.py -x` | No -- Wave 0 |
| SCHED-01 | History DB records task execution | unit | `pytest tests/test_scheduler_history.py -x` | No -- Wave 0 |
| SCHED-01 | Circuit breaker opens/closes correctly | unit | `pytest tests/test_scheduler_circuit_breaker.py -x` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/ -x -q --tb=short -k "scheduler"`
- **Per wave merge:** `pytest tests/ -v --tb=long -k "scheduler"`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_scheduler_stability.py` -- covers SCHED-01 crash isolation, Redis degradation, subprocess timeout, graceful shutdown
- [ ] `tests/test_scheduler_config.py` -- covers config loading, task validation
- [ ] `tests/test_scheduler_history.py` -- covers SQLite history DB operations
- [ ] `tests/test_scheduler_circuit_breaker.py` -- covers circuit breaker state transitions
- [ ] Shared fixtures for mock Redis, mock subprocess, temp DB paths

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Internal service, no user auth |
| V3 Session Management | No | No user sessions |
| V4 Access Control | Partial | API endpoints should not expose sensitive data, but internal network only |
| V5 Input Validation | Yes | YAML config validation, subprocess argument sanitization |
| V6 Cryptography | No | No encryption needed for this phase |

### Known Threat Patterns for Scheduler + Subprocess Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Command injection via config | Tampering | Validate script paths exist and are within project root before subprocess call |
| YAML deserialization attack | Tampering | Use yaml.SafeLoader (already in UniqueKeySafeLoader) |
| Path traversal in script config | Tampering | Resolve script paths against project root, reject `..` components |
| Redis credential exposure | Information disclosure | Load from env vars, not hardcoded in config |

## Sources

### Primary (HIGH confidence)
- APScheduler 3.x official documentation (apscheduler.readthedocs.io) -- userguide, events module, API reference
- Existing code: scripts/scheduler.py (1147 lines) -- full feature reference
- Existing code: core/distributed_lock.py -- DistributedLock implementation
- pip show output -- verified all package versions

### Secondary (MEDIUM confidence)
- Existing code: services/data_service/scheduler/ -- modular structure reference
- Config files: config/scheduler.yaml, config/cron_tasks.yaml -- schema comparison
- tests/conftest.py -- existing test infrastructure

### Tertiary (LOW confidence)
- APScheduler 4.x status -- assumed not production-ready based on training knowledge (flagged as A1)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all packages verified installed, versions confirmed via pip
- Architecture: HIGH -- based on APScheduler official docs and proven patterns from existing code
- Pitfalls: HIGH -- all pitfalls identified from existing code comments and known APScheduler behavior

**Research date:** 2026-05-12
**Valid until:** 2026-06-12 (30 days -- stable stack, no fast-moving dependencies)
