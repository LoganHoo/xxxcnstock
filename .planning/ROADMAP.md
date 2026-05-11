# Roadmap: XCNStock Stabilization & Refactoring

## Overview

This roadmap stabilizes the XCNStock scheduling and pipeline system so it runs without human intervention, then progressively improves code quality and business agility. The journey: first make the scheduler survive (Phases 1-4), then clean up configuration (Phases 5-6), then fix code structure (Phases 7-10), and finally enable business users to change strategies without developer involvement (Phases 11-12).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Scheduler Process Stability** - APScheduler keeps running without crashes or hangs
- [ ] **Phase 2: Task Failure Recovery** - Failed tasks auto-retry with exponential backoff and alert on exhaustion
- [ ] **Phase 3: Execution History** - Task execution records persisted and queryable
- [ ] **Phase 4: Pipeline Dependency Chains** - Pipeline tasks execute in correct dependency order
- [ ] **Phase 5: Configuration Unification** - Single config entry point, eliminate dual config systems
- [ ] **Phase 6: Credential Cleanup** - All hardcoded credentials removed, env-driven config
- [ ] **Phase 7: Import Path Fix** - Eliminate sys.path hacks, proper package structure
- [ ] **Phase 8: Error Handling Standardization** - Structured error handling in pipeline scripts
- [ ] **Phase 9: Duplicate Code Elimination** - Extract shared logic into core modules
- [ ] **Phase 10: Module Boundary Enforcement** - Clear layer responsibilities, no cross-layer calls
- [ ] **Phase 11: Strategy Configuration** - Strategy changes via YAML only, no code changes
- [ ] **Phase 12: Feature Toggles & Independent Steps** - Config-driven toggles and runnable individual pipeline steps

## Phase Details

### Phase 1: Scheduler Process Stability
**Goal**: APScheduler service starts and stays running indefinitely without crashes, hangs, or silent failures
**Mode**: mvp
**Depends on**: Nothing (first phase)
**Requirements**: SCHED-01
**Success Criteria** (what must be TRUE):
  1. Scheduler process starts and remains running for 24+ hours without manual intervention
  2. When an individual task crashes, the scheduler process itself stays alive and continues scheduling other tasks
  3. Scheduler health check endpoint returns alive status and uptime
  4. System resource usage (memory, threads) remains stable over 24 hours with no leaks
**Plans**: 2 plans

Plans:
- [ ] 01-01-PLAN.md — Walking skeleton: unified config, scheduler engine, subprocess executor, crash isolation, signal handling
- [ ] 01-02-PLAN.md — Production resilience: Redis degradation, history DB, circuit breaker, market calendar, Flask API process

### Phase 2: Task Failure Recovery
**Goal**: Every scheduled task has automatic retry with exponential backoff, and persistent failures trigger alert notifications
**Mode**: mvp
**Depends on**: Phase 1
**Requirements**: SCHED-02
**Success Criteria** (what must be TRUE):
  1. When a task fails, it automatically retries up to 3 times with increasing delay between attempts
  2. After all retries exhausted, an alert notification is sent via configured channel (DingTalk/WeChat)
  3. Alert message includes task name, failure reason, and retry count
  4. Successful retry resets the failure counter without sending false alarms
**Plans**: TBD

### Phase 3: Execution History
**Goal**: Every task execution is recorded with status, timing, and error details, queryable for debugging
**Mode**: mvp
**Depends on**: Phase 1
**Requirements**: SCHED-03
**Success Criteria** (what must be TRUE):
  1. Every task execution creates a database record with start time, end time, and status (success/failed)
  2. Failed task records include the error message and stack trace
  3. Execution history is queryable by date range and task name
  4. History records are retained for at least 30 days automatically
**Plans**: TBD

### Phase 4: Pipeline Dependency Chains
**Goal**: Pipeline tasks execute in correct dependency order -- downstream tasks only run after upstream tasks succeed
**Mode**: mvp
**Depends on**: Phase 2, Phase 3
**Requirements**: SCHED-04
**Success Criteria** (what must be TRUE):
  1. Daily pipeline tasks execute in defined sequence: collect -> audit -> compute -> score -> report
  2. If an upstream task fails, downstream dependent tasks are skipped (not silently run on stale data)
  3. Independent tasks within the same pipeline run in parallel for efficiency
  4. Pipeline completion status is logged with overall success/failure and individual task outcomes
**Plans**: TBD

### Phase 5: Configuration Unification
**Goal**: One configuration entry point that merges YAML, env vars, and defaults -- no more dual config systems
**Mode**: mvp
**Depends on**: Nothing (independent of scheduler, runs in parallel with Phases 2-4 if needed)
**Requirements**: CONF-01
**Success Criteria** (what must be TRUE):
  1. All modules read configuration through a single import (get_settings or equivalent unified entry point)
  2. core/config.py and core/unified_config.py are merged or one is deprecated with a clear migration
  3. Existing YAML configs (scheduler.yaml, factors/*.yaml, etc.) continue to load without breaking changes
  4. Configuration precedence is documented: env vars override YAML, YAML overrides defaults
**Plans**: TBD

### Phase 6: Credential Cleanup
**Goal**: Zero hardcoded IPs, passwords, or file paths in source code -- all sensitive values come from environment variables or Nacos
**Mode**: mvp
**Depends on**: Phase 5
**Requirements**: CONF-02, CONF-03
**Success Criteria** (what must be TRUE):
  1. Grep for '49.233.10.199', '100200', '192.168.1.168' returns zero results in Python files
  2. Every os.getenv() call with hardcoded fallback credentials is replaced with config-driven reads
  3. .env.example documents all required environment variables with descriptions
  4. Application fails fast with a clear error message when required env vars are missing
**Plans**: TBD

### Phase 7: Import Path Fix
**Goal**: All imports resolve through proper package structure with zero sys.path.insert() calls
**Mode**: mvp
**Depends on**: Nothing (independent, but logically follows config cleanup)
**Requirements**: CODE-04
**Success Criteria** (what must be TRUE):
  1. Grep for 'sys.path.insert' returns zero results in Python source files
  2. All hardcoded absolute paths (/Volumes/Xdata/...) are removed
  3. pip install -e . works and enables correct imports across all modules
  4. All existing scripts run successfully with the new import structure
**Plans**: TBD

### Phase 8: Error Handling Standardization
**Goal**: All critical pipeline scripts have structured try/catch blocks with proper logging -- no silent exception swallowing
**Mode**: mvp
**Depends on**: Phase 7
**Requirements**: CODE-03
**Success Criteria** (what must be TRUE):
  1. Every pipeline script in scripts/pipeline/ has a top-level try/catch that logs the full error and exits with non-zero code
  2. No bare `except:` or `except Exception: pass` patterns remain in pipeline scripts
  3. Error logs include structured context: script name, input parameters, and actionable error message
  4. Logging output goes to both console and log files with consistent formatting
**Plans**: TBD

### Phase 9: Duplicate Code Elimination
**Goal**: Common patterns (data fetching, DB connections, logging setup) extracted into core/ modules with zero copy-paste across scripts
**Mode**: mvp
**Depends on**: Phase 7, Phase 8
**Requirements**: CODE-01
**Success Criteria** (what must be TRUE):
  1. DB connection code uses a shared utility (no more raw pymysql.connect() scattered across scripts)
  2. Data fetching patterns use shared adapters (no duplicate Baostock/Tushare connection logic)
  3. Logging setup uses a single consistent pattern across all pipeline scripts
  4. Grep for duplicate function names across scripts/ returns zero meaningful duplicates
**Plans**: TBD

### Phase 10: Module Boundary Enforcement
**Goal**: core/scripts/services/workflows layers have clear responsibilities with no cross-layer direct calls bypassing the layer abstraction
**Mode**: mvp
**Depends on**: Phase 9
**Requirements**: CODE-02
**Success Criteria** (what must be TRUE):
  1. scripts/ only calls core/ public APIs, never directly accesses services/ internals
  2. services/ only uses core/ abstractions, never imports from scripts/ or other services/ directly
  3. core/ has no dependencies on scripts/ or services/ (verified by import analysis)
  4. Each module's public interface is documented or implied by __all__ exports
**Plans**: TBD

### Phase 11: Strategy Configuration
**Goal**: Quant analysts change stock selection strategies by editing YAML files only -- no code changes, no redeployment needed
**Mode**: mvp
**Depends on**: Phase 10
**Requirements**: BIZ-01
**Success Criteria** (what must be TRUE):
  1. Changing factor weights in a YAML config file changes stock selection results on next run
  2. Adding a new strategy is done by creating a new YAML file with no Python code changes
  3. Existing strategies continue to work identically after this change
  4. Strategy YAML schema is documented with examples for common modifications
**Plans**: TBD

### Phase 12: Feature Toggles & Independent Steps
**Goal**: Factors/filters/data sources toggle on/off via config, and every pipeline step runs independently for debugging
**Mode**: mvp
**Depends on**: Phase 11
**Requirements**: BIZ-02, BIZ-03
**Success Criteria** (what must be TRUE):
  1. Setting enabled: false on a factor in config skips that factor in computation without errors
  2. Setting enabled: false on a data source falls back to remaining sources gracefully
  3. Any single pipeline step (e.g., stock_pick, data_collect) runs independently via command line with correct results
  4. Running a step independently produces the same output as running it within the full pipeline
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10 -> 11 -> 12

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Scheduler Process Stability | 0/2 | Planning complete | - |
| 2. Task Failure Recovery | 0/? | Not started | - |
| 3. Execution History | 0/? | Not started | - |
| 4. Pipeline Dependency Chains | 0/? | Not started | - |
| 5. Configuration Unification | 0/? | Not started | - |
| 6. Credential Cleanup | 0/? | Not started | - |
| 7. Import Path Fix | 0/? | Not started | - |
| 8. Error Handling Standardization | 0/? | Not started | - |
| 9. Duplicate Code Elimination | 0/? | Not started | - |
| 10. Module Boundary Enforcement | 0/? | Not started | - |
| 11. Strategy Configuration | 0/? | Not started | - |
| 12. Feature Toggles & Independent Steps | 0/? | Not started | - |
