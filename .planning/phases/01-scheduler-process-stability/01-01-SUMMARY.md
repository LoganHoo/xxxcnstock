---
phase: 01-scheduler-process-stability
plan: 01
subsystem: scheduler
tags: [apscheduler, subprocess, yaml, cron, signal-handling, health-check]

requires: []
provides:
  - Unified config/scheduler.yaml merging both existing config files (48 tasks)
  - task_config.py with typed dataclasses and validation
  - SchedulerEngine with BlockingScheduler, subprocess execution, crash isolation
  - run_subprocess with timeout/terminate/kill and zombie prevention
  - Health endpoint at /health returning alive status with uptime
  - Signal handling (SIGTERM/SIGINT) with graceful shutdown via threading.Event
  - Entry point run_engine.py for direct execution
affects: [02-scheduler-process-stability, scheduler-reliability]

tech-stack:
  added: []
  patterns: [subprocess-isolation, typed-config-dataclass, signal-handler-daemon-thread]

key-files:
  created:
    - services/data_service/scheduler/task_config.py
    - services/data_service/scheduler/engine.py
    - services/data_service/scheduler/executor.py
    - services/data_service/scheduler/run_engine.py
    - tests/test_scheduler_config.py
    - tests/test_scheduler_stability.py
  modified:
    - config/scheduler.yaml
    - services/data_service/scheduler/__init__.py

key-decisions:
  - "Named module task_config.py instead of config.py to avoid conflict with existing config/ package directory"
  - "Used BackgroundScheduler in tests to control lifecycle, BlockingScheduler in production"
  - "Prefer cron_tasks.yaml values for overlapping tasks (richer schema) in unified config"

patterns-established:
  - "Subprocess isolation: all tasks run as subprocess.Popen with start_new_session=True"
  - "Typed config: YAML loaded into dataclasses with validation, disabled tasks filtered"
  - "Signal handling: threading.Event + daemon thread for scheduler.shutdown() to avoid deadlock"
  - "Health endpoint: lightweight http.server.HTTPServer in daemon thread"

requirements-completed: [SCHED-01]

duration: 8min
completed: 2026-05-12
---

# Phase 1 Plan 01: Walking Skeleton Summary

**Unified scheduler engine with APScheduler BlockingScheduler, subprocess executor with crash isolation, typed config loading 48 tasks from merged YAML**

## Performance

- **Duration:** 8 min
- **Started:** 2026-05-11T19:59:57Z
- **Completed:** 2026-05-11T20:07:28Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Merged scheduler.yaml (35 tasks) + cron_tasks.yaml (44 tasks) into unified config with 48 enabled tasks
- Built SchedulerEngine with crash isolation: individual task failures do not kill the scheduler process
- Implemented subprocess executor with terminate -> wait(5) -> kill() timeout chain preventing zombie processes
- Health endpoint at /health returns JSON with status, uptime, and registered task count
- Graceful shutdown via SIGTERM/SIGINT using threading.Event (avoids APScheduler deadlock)
- All 12 tests pass (6 config + 6 stability)

## Task Commits

1. **Task 1: Create unified config and test scaffolding** - `d40d03a` (feat)
2. **Task 2: Build scheduler engine with subprocess executor, crash isolation, and signal handling** - `4ce8133` (feat)

## Files Created/Modified
- `config/scheduler.yaml` - Unified config merging both existing files with 48 tasks, global scheduler settings, lock and monitoring config
- `services/data_service/scheduler/__init__.py` - Package exports for SchedulerConfig, TaskConfig, load_config
- `services/data_service/scheduler/task_config.py` - Typed dataclasses (TaskConfig, SchedulerConfig, etc.) and load_config() with validation
- `services/data_service/scheduler/engine.py` - SchedulerEngine class with BlockingScheduler, job callbacks, event listeners, signal handlers, health server
- `services/data_service/scheduler/executor.py` - run_subprocess() with Popen + poll + terminate/kill timeout chain
- `services/data_service/scheduler/run_engine.py` - Entry point for direct execution
- `tests/test_scheduler_config.py` - 6 tests for config loading, validation, filtering, optional fields
- `tests/test_scheduler_stability.py` - 6 tests for crash isolation, subprocess timeout, graceful shutdown, health endpoint, task registration

## Decisions Made
- Named the config module `task_config.py` instead of `config.py` because an existing `config/` package directory at the same path causes Python import resolution to prefer the directory over the file
- Used BackgroundScheduler in test fixtures to allow test-controlled lifecycle; BlockingScheduler for production
- In the unified config, preferred cron_tasks.yaml values for overlapping tasks since it has the richer schema (depends_on, day_type, circuit_breaker, etc.)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Renamed config.py to task_config.py due to existing config/ package conflict**
- **Found during:** Task 1 (config module creation)
- **Issue:** `services/data_service/scheduler/config/` already exists as a package directory with `config_loader.py`, causing Python to resolve `from .config import ...` to the package, not a new module file
- **Fix:** Named the new module `task_config.py` instead of `config.py`
- **Files modified:** task_config.py (new name), __init__.py, test imports
- **Verification:** All tests pass with correct imports

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minimal -- module renamed to avoid conflict with existing code. All functionality preserved.

## Issues Encountered
- test_loads_all_tasks called shutdown(wait=False) on a BackgroundScheduler that was never started, raising SchedulerNotRunningError -- fixed by guarding with `if engine.scheduler.running`

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Walking skeleton complete: scheduler engine starts, loads config, registers all 48 tasks
- Next plans can build on this foundation: add distributed locking, retry logic, circuit breaker, history DB, market calendar checks
- The dual-process architecture (engine + API) is not yet split -- this plan delivers the engine process only

## Self-Check: PASSED

All 8 files verified present. Both commits (d40d03a, 4ce8133) verified in git log.

---
*Phase: 01-scheduler-process-stability*
*Completed: 2026-05-12*
