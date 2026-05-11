# Requirements: XCNStock Stabilization & Refactoring

**Defined:** 2026-05-12
**Core Value:** Scheduler and pipeline scripts must run stably and automatically without human monitoring

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Scheduling Stability

- [ ] **SCHED-01**: Scheduler process stability -- APScheduler service starts and stays running without crashes or hangs
- [ ] **SCHED-02**: Task failure retry + alert -- Tasks auto-retry with exponential backoff, trigger alert notification after exhausting retries
- [ ] **SCHED-03**: Execution history -- Task execution records persisted to database, queryable for status and failure reasons
- [ ] **SCHED-04**: Task dependency orchestration -- Pipeline task dependency order guaranteed, downstream tasks only run after upstream completion

### Configuration Management

- [ ] **CONF-01**: Unified config entry point -- Consolidate to unified_config.py, eliminate core/config.py and scattered config loading
- [ ] **CONF-02**: Remove hardcoded credentials -- Eliminate 62 hardcoded IPs/passwords/paths in code, all through config
- [ ] **CONF-03**: Environment variable driven config -- All sensitive config loaded via .env or Nacos, never hardcoded in source

### Code Quality

- [ ] **CODE-01**: Eliminate duplicate code -- Extract common logic to core/, eliminate duplicated data fetching/error handling/logging across scripts
- [ ] **CODE-02**: Clear module boundaries -- core/scripts/services/workflows each have clear responsibilities, no cross-layer direct calls
- [ ] **CODE-03**: Standardized error handling -- Critical pipeline scripts have try/catch and structured logging, exceptions not silently swallowed
- [ ] **CODE-04**: Fix import paths -- Eliminate sys.path manipulation in 30+ files, use proper package structure and relative imports

### Business Agility

- [ ] **BIZ-01**: Strategy configuration -- Strategy changes only require editing YAML config, no code modifications
- [ ] **BIZ-02**: Feature toggle mechanism -- Factors/filters/data sources can be enabled/disabled via config without redeployment
- [ ] **BIZ-03**: Independent pipeline step execution -- Each pipeline step can run and debug independently without running full pipeline

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Monitoring & Alerting

- **MONIT-01**: Grafana dashboard for scheduler status visualization
- **MONIT-02**: Automatic exception notification to DingTalk/WeChat
- **MONIT-03**: Data quality trend reporting

### Infrastructure

- **INFRA-01**: Docker health check and auto-restart
- **INFRA-02**: Database connection pool unified management
- **INFRA-03**: API gateway authentication

### Testing

- **TEST-01**: Pipeline script integration tests
- **TEST-02**: Factor calculation unit test coverage > 80%
- **TEST-03**: Regression test suite

## Out of Scope

| Feature | Reason |
|---------|--------|
| Architecture rewrite | Production service cannot stop, existing architecture is sound |
| Frontend UI refactoring | Focus on backend stability |
| Real-time WebSocket push | Not urgent |
| User authentication system | Internal service, not needed yet |
| CI/CD improvements | Stabilize first |
| Kestra migration | Keep APScheduler as primary, Kestra as optional |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| SCHED-01 | Phase 1 | Pending |
| SCHED-02 | Phase 2 | Pending |
| SCHED-03 | Phase 3 | Pending |
| SCHED-04 | Phase 4 | Pending |
| CONF-01 | Phase 5 | Pending |
| CONF-02 | Phase 6 | Pending |
| CONF-03 | Phase 6 | Pending |
| CODE-04 | Phase 7 | Pending |
| CODE-03 | Phase 8 | Pending |
| CODE-01 | Phase 9 | Pending |
| CODE-02 | Phase 10 | Pending |
| BIZ-01 | Phase 11 | Pending |
| BIZ-02 | Phase 12 | Pending |
| BIZ-03 | Phase 12 | Pending |

**Coverage:**
- v1 requirements: 14 total
- Mapped to phases: 14
- Unmapped: 0

---
*Requirements defined: 2026-05-12*
*Last updated: 2026-05-12 after roadmap creation*
