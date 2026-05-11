# Phase 2: Task Failure Recovery - Context

**Gathered:** 2026-05-12
**Status:** Ready for planning

<domain>
## Phase Boundary

为调度引擎添加任务级自动重试（指数退避）和持久失败告警通知。当任务失败时自动重试最多3次，重试耗尽后通过钉钉/微信发送告警。

This phase delivers:
- Per-task retry with configurable exponential backoff
- Alert notification dispatch on persistent failure
- Retry history tracking in execution records

</domain>

<decisions>
## Implementation Decisions

### From Roadmap
- **D-01:** 重试最多3次，指数退避（base_delay * 2^attempt）
- **D-02:** 重试耗尽后触发告警通知
- **D-03:** 告警包含任务名、失败原因、重试次数
- **D-04:** 成功重试不触发误报

### From Phase 1 Context (carried forward)
- **D-05:** 基于新调度引擎实现，不修改旧 scripts/scheduler.py
- **D-06:** 配置已统一到 config/scheduler.yaml
- **D-07:** 引擎在 services/data_service/scheduler/engine.py
- **D-08:** 已有 HistoryDB (SQLite) 记录执行历史
- **D-09:** 已有 CircuitBreaker 熔断器
- **D-10:** 已有 TaskLockManager 分布式锁

### Claude's Discretion
- 重试逻辑的具体实现方式（在 job callback 内循环 vs 注册多次 APScheduler job）
- 告警通知的具体实现（复用现有 notify_service 还是简化实现）
- 重试记录的 HistoryDB schema 扩展

</decisions>

<canonical_refs>
## Canonical References

### Phase 1 Outputs (MUST read)
- `services/data_service/scheduler/engine.py` — Current engine with job callback
- `services/data_service/scheduler/task_config.py` — TaskConfig with retry config fields
- `services/data_service/scheduler/history.py` — HistoryDB for execution tracking
- `services/data_service/scheduler/circuit_breaker.py` — CircuitBreaker for persistent failures
- `services/data_service/scheduler/executor.py` — run_subprocess with timeout
- `config/scheduler.yaml` — Unified config with retry section per task

### Existing Notification System
- `services/notify_service/main.py` — Notify service
- `services/notify_service/signal_hub.py` — Signal dispatch hub
- `core/models.py` — StockSelectionSignal, NotificationMessage models

### Legacy Reference
- `scripts/scheduler.py` lines 725-931 — Existing retry loop with exponential backoff

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **TaskConfig.retry**: Already defined in task_config.py as `Optional[dict]` with fields `{enabled, max_retries, delay, backoff}` — config schema is ready
- **HistoryDB**: Already has record_start/record_complete with retry_count field
- **CircuitBreaker**: Already tracks failures, but retry happens before circuit breaker check
- **run_subprocess**: Already handles subprocess execution with timeout

### Established Patterns
- Job callback wraps all logic in try/except for crash isolation
- Config-driven behavior per task (enabled/disabled fields)
- Subprocess execution with timeout and force-kill

### Integration Points
- Engine job callback needs retry loop around run_subprocess
- HistoryDB record_complete already accepts retry_count parameter
- Notify service exists but may need a lightweight direct-call path for scheduler alerts

</code_context>

<specifics>
## Specific Ideas

- Retry delay formula: min(retry_delay * backoff^(attempt-1), max_backoff) — same as scripts/scheduler.py
- Alert should use DingTalk webhook (simplest path) since notify_service is a separate HTTP service
- Consider: retry happens WITHIN a single job callback (loop), not as separate APScheduler jobs

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope
</deferred>

---

*Phase: 2-Task Failure Recovery*
*Context gathered: 2026-05-12*
