# Phase 1: Scheduler Process Stability - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-12
**Phase:** 1-Scheduler Process Stability
**Areas discussed:** Scheduler implementation choice, Failure mode analysis, Merge strategy, Config unification, Architecture

---

## Scheduler Implementation Choice

| Option | Description | Selected |
|--------|-------------|----------|
| scripts/scheduler.py | 功能最全，已有重试/熔断/历史/健康检查 | |
| services/scheduler/ | Flask服务架构，更简洁 | |
| 合并两套实现 | 合并两者优点到统一的实现 | ✓ |

**User's choice:** 全新实现 — 不基于任一现有实现，吸取两者优点重写
**Notes:** User explicitly wants a clean rewrite, not patching existing code

---

## Failure Mode Analysis

| Option | Description | Selected |
|--------|-------------|----------|
| 进程崩溃/卡死 | 调度进程本身崩掉或卡死，不再执行后续任务 | |
| 单个脚本报错 | 调度器还活着，但某些脚本报错或超时 | |
| 都有，说不清 | 两者都有，说不清具体原因 | ✓ |

**User's choice:** Both process crashes and script errors, unclear specific causes
**Notes:** No clear crash logs — need to stabilize first, debug later

---

## Merge Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| 基于 scripts/ 增强 | 以 scripts/scheduler.py 为基础，补上 Flask API 层 | |
| 基于 services/ 补全 | 以 services/scheduler/ 为基础，补全重试/熔断/历史功能 | |
| 全新实现 | 全新实现，吸取两者优点 | ✓ |

**User's choice:** Brand new implementation
**Notes:** scripts/scheduler.py has the most features but is a 1147-line monolith. services/ has better structure but is incomplete.

---

## Debug Approach

| Option | Description | Selected |
|--------|-------------|----------|
| 先加可观测性 | 添加详细日志和状态监控，定位具体问题 | |
| 直接开始修复 | 直接开始稳定化，边做边发现问题 | ✓ |

**User's choice:** Start fixing directly
**Notes:** Urgent timeline (this week) — no time for observability first

---

## Config Unification

| Option | Description | Selected |
|--------|-------------|----------|
| 统一到 scheduler.yaml | 合并为统一配置文件 | ✓ |
| 保留两个配置 | 保留两个配置文件但确保不冲突 | |

**User's choice:** Unify to config/scheduler.yaml
**Notes:** Currently has scheduler.yaml (Flask version) and cron_tasks.yaml (scripts version)

---

## Architecture

| Option | Description | Selected |
|--------|-------------|----------|
| 单进程 | 所有功能在一个进程里，HTTP API + 调度器 | |
| 双进程 | 调度器进程 + 独立 API 进程 | ✓ |

**User's choice:** Dual process architecture
**Notes:** Scheduler engine process and HTTP API process running separately

---

## Claude's Discretion

- Module structure and code organization of new scheduler
- Specific error recovery strategies
- HTTP API endpoint design and response format
- Inter-process communication method (scheduler ↔ API)
- Old scheduler code migration/cleanup plan

## Deferred Ideas

None — discussion stayed within phase scope
