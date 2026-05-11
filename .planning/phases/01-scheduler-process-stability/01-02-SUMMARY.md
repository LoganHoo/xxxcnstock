---
phase: 01-scheduler-process-stability
plan: 02
subsystem: scheduler
tags: [redis-degradation, circuit-breaker, sqlite-history, market-calendar, flask-api]

requires:
  - phase: 01-scheduler-process-stability/01
    provides: "Walking skeleton: engine, subprocess executor, config loader"
provides:
  - TaskLockManager with Redis graceful degradation (fail-open on connection error)
  - HistoryDB with SQLite WAL-mode, thread-safe concurrent writes
  - CircuitBreaker state machine (closed/open/half_open) with JSON file persistence
  - MarketCalendar with holiday awareness and day_type filtering
  - Flask API process serving /health, /tasks, /stats endpoints from shared SQLite
  - Engine integration: calendar check -> circuit breaker -> run_once -> lock -> execute -> record -> release
affects: [scheduler-reliability, monitoring]

tech-stack:
  added: []
  patterns: [redis-degradation-fail-open, sqlite-wal-shared-db, circuit-breaker-json-state, dual-process-architecture]

key-files:
  created:
    - services/data_service/scheduler/lock_manager.py
    - services/data_service/scheduler/history.py
    - services/data_service/scheduler/circuit_breaker.py
    - services/data_service/scheduler/calendar.py
    - services/data_service/scheduler/api/app.py
    - services/data_service/scheduler/api/routes.py
    - services/data_service/scheduler/run_api.py
    - tests/test_scheduler_history.py
    - tests/test_scheduler_circuit_breaker.py
  modified:
    - services/data_service/scheduler/engine.py
    - services/data_service/scheduler/api/__init__.py

key-decisions:
  - "CircuitBreaker uses per-task JSON files (not a single shared file) for atomicity and simpler locking"
  - "HistoryDB uses RLock for write serialization; reads need no lock in WAL mode"
  - "TaskLockManager creates fresh Redis client on each lock attempt to avoid poisoned connection pools (per RESEARCH.md Pitfall 4)"
  - "Engine callback wraps all resilience code in outer try/except per D-09 (task crash isolation)"

patterns-established:
  - "Redis degradation: fail-open with warning log, tasks continue without distributed locking"
  - "SQLite WAL shared DB: engine writes, API reads, both processes access same file"
  - "Circuit breaker: per-task JSON state files with atomic write (tmp + rename)"
  - "Dual process: engine and API fully independent, communicate via shared SQLite only"

requirements-completed: [SCHED-01]

duration: 6min
completed: 2026-05-12
---

# Phase 1 Plan 02: Resilience Layer Summary

**Redis degradation, SQLite execution history, circuit breaker state machine, market calendar, and independent Flask API process wired into the scheduler engine**

## Performance

- **Duration:** 6 min
- **Started:** 2026-05-11T20:09:18Z
- **Completed:** 2026-05-11T20:15:38Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- TaskLockManager wraps DistributedLock with graceful Redis degradation: fresh client per attempt, fail-open on connection error
- HistoryDB provides thread-safe SQLite execution history with WAL mode, supporting concurrent reads from the API process
- CircuitBreaker implements closed/open/half_open state machine with per-task JSON persistence and atomic writes
- MarketCalendar handles trading day identification with holiday awareness and day_type filtering (daily/weekday/weekend)
- Engine job callback integrates all four modules: calendar -> circuit breaker -> run_once -> lock -> execute -> record -> release
- Flask API process serves /health, /tasks, /stats, /stats/<task_name> from shared SQLite, fully independent of engine
- All 21 tests pass (6 config + 6 stability + 4 history + 5 circuit breaker)

## Task Commits

1. **Task 1: Create resilience modules (lock manager, history, circuit breaker, calendar)** - `6a74e44` (feat)
2. **Task 2: Wire resilience modules into engine and create Flask API process** - `cad5b49` (feat)

## Files Created/Modified
- `services/data_service/scheduler/lock_manager.py` - Redis lock with graceful degradation fallback, fresh client per attempt
- `services/data_service/scheduler/history.py` - SQLite WAL-mode execution history with thread-safe RLock writes
- `services/data_service/scheduler/circuit_breaker.py` - Circuit breaker state machine with per-task JSON persistence
- `services/data_service/scheduler/calendar.py` - Market calendar with holiday list and day_type filtering
- `services/data_service/scheduler/engine.py` - Integrated all resilience modules into job callback with outer try/except
- `services/data_service/scheduler/api/app.py` - Flask app factory with HistoryDB injection
- `services/data_service/scheduler/api/routes.py` - /health, /tasks, /stats, /stats/<task_name> endpoints
- `services/data_service/scheduler/api/__init__.py` - Package exports create_app
- `services/data_service/scheduler/run_api.py` - Entry point for independent API process
- `tests/test_scheduler_history.py` - 4 tests: start/complete, failure recording, concurrent writes, stats calculation
- `tests/test_scheduler_circuit_breaker.py` - 5 tests: starts closed, opens after threshold, recovers after timeout, success resets, half-open reopens

## Decisions Made
- Per-task JSON files for circuit breaker state instead of a single shared file, enabling atomic writes without cross-task locking concerns
- RLock for HistoryDB write serialization; reads operate lock-free in WAL mode since SQLite WAL allows concurrent readers
- Fresh Redis client per lock attempt to avoid poisoned connection pools (per RESEARCH.md Pitfall 4 analysis)
- Engine callback uses nested try/except: inner for subprocess execution errors, outer to guarantee resilience code never crashes the scheduler (D-09)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed circuit breaker test with recovery_timeout=0**
- **Found during:** Task 1 (circuit breaker tests)
- **Issue:** Test `test_recovers_after_timeout` used `recovery_timeout=0` which caused immediate half-open transition, making the first `should_skip` assertion fail (circuit opened but immediately recovered)
- **Fix:** Changed to `recovery_timeout=1` with `sleep(1.1)` to properly test the time-based transition
- **Files modified:** tests/test_scheduler_circuit_breaker.py
- **Verification:** All 9 resilience tests pass

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Test timing fix only, no production code change.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete production resilience layer: scheduler survives Redis outages, records all executions, prevents thrashing via circuit breaker, respects market calendar
- Engine and API are fully independent processes sharing state via SQLite
- Ready for deployment configuration (Docker Compose dual-service, systemd units, etc.)
- Future plans can add: retry logic with backoff, progress stall detection, notification/alerting on failures

## Self-Check: PASSED

All 11 files verified present. Both commits (6a74e44, cad5b49) verified in git log. All 21 tests pass.

---
*Phase: 01-scheduler-process-stability*
*Completed: 2026-05-12*
