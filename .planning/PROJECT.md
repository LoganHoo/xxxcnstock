# XCNStock 稳定化与重构

## What This Is

A股量化选股分析平台，提供实时/历史行情采集、多因子选股、涨停分析、多渠道推送等功能。当前作为**生产服务**运行，但系统不稳定、代码混乱、响应需求慢。

核心问题：调度器和业务脚本频繁报错，每天需要人工介入。代码重复多、配置散落、模块边界不清，导致改一处影响其他地方。

## Core Value

**调度器和流水线脚本必须稳定自动运行，不需要人盯。** 其他一切以此为前提。

## Requirements

### Validated

<!-- 现有代码已实现的能力 -->

- ✓ A股日线行情数据采集（Baostock/Tushare/Tencent多数据源）— existing
- ✓ K线数据Parquet存储和加载 — existing
- ✓ 多因子计算引擎（技术面、量价、市场因子）— existing
- ✓ 过滤器注册表模式（保守/标准/激进预设）— existing
- ✓ 策略引擎（因子加权+过滤排序）— existing
- ✓ 涨停板分析和次日预测 — existing
- ✓ 多渠道通知（微信Server酱/钉钉/邮件/Kafka）— existing
- ✓ FastAPI微服务架构（4服务+网关）— existing
- ✓ 多级缓存（内存+Redis）— existing
- ✓ APScheduler定时调度 — existing
- ✓ Prometheus监控指标 — existing
- ✓ 每日流水线：收盘数据采集→审计→因子计算→选股→报告推送 — existing
- ✓ 晨间流水线：外盘更新→宏观分析→晨报推送→盘前准备 — existing

### Active

<!-- 当前需要解决的问题 -->

- [ ] 调度器稳定运行，不崩溃、不卡死
- [ ] 所有定时任务按配置时间正确执行
- [ ] 任务失败自动重试，重试失败有告警
- [ ] 配置统一管理，消除硬编码的host/密码/路径
- [ ] 核心模块职责清晰，减少跨层调用
- [ ] 消除重复代码，公共逻辑提取到core/
- [ ] 关键流水线脚本有日志和错误处理
- [ ] 新增/修改选股策略不影响现有逻辑

### Out of Scope

- 重写为全新架构 — 当前系统有生产用户，不能推倒重来
- 移动端/前端UI重构 — 当前聚焦后端稳定性
- 实时行情WebSocket推送 — 非紧急需求
- 用户认证系统 — 内部服务，暂不需要
- CI/CD流水线完善 — 先稳定运行再说

## Context

- **技术栈**: Python 3.11 + Polars + FastAPI + MySQL 8.0 + Redis 7 + APScheduler
- **代码质量问题**: 62处硬编码凭据/IP、双配置系统(core/config.py vs unified_config.py)、API网关无认证、数据验证器缺陷、29处直连DB绕过连接池、30+文件sys.path操控
- **调度架构**: 双调度器（APScheduler + 可选Kestra），通过config/scheduler.yaml配置cron任务
- **业务特点**: A股市场规则变化（交易时间、板块分类）和需求方频繁调整选股策略
- **部署方式**: Docker Compose多服务编排，生产环境Linux服务器

## Constraints

- **时间**: 本周内（2026-05-12起）完成系统稳定化
- **业务连续性**: 不能中断现有生产服务，改动需要向后兼容
- **技术栈**: 必须基于现有Python + FastAPI + MySQL + Redis，不引入新框架
- **数据源**: 依赖外部API（Baostock/Tushare/AKShare），需处理限流和异常
- **单机部署**: 当前单服务器运行所有服务，无分布式需求

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| 先稳定后重构 | 生产服务不能停，先解决稳定性问题再优化代码 | — Pending |
| 保留现有架构 | 4微服务+网关+调度器架构合理，问题在实现而非设计 | — Pending |
| 配置统一到unified_config | 消除双配置系统，所有配置通过一个入口读取 | — Pending |
| 插件式策略管理 | 因子/过滤器已用注册表模式，策略变动应通过配置而非改代码 | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-12 after initialization*
