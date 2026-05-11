# Requirements: XCNStock 稳定化与重构

**Defined:** 2026-05-12
**Core Value:** 调度器和流水线脚本必须稳定自动运行，不需要人盯

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Scheduling Stability

- [ ] **SCHED-01**: 调度器进程保活 — APScheduler服务启动后稳定运行，不崩溃、不卡死
- [ ] **SCHED-02**: 任务失败重试+告警 — 任务失败后自动重试（指数退避），超过次数触发告警通知
- [ ] **SCHED-03**: 执行历史记录 — 任务执行记录持久化到数据库，可查看历史状态和失败原因
- [ ] **SCHED-04**: 任务依赖编排 — 流水线任务依赖顺序保证，前置任务完成才执行后续任务

### Configuration Management

- [ ] **CONF-01**: 统一配置入口 — 统一到unified_config.py，消除core/config.py和散落的配置加载
- [ ] **CONF-02**: 清除硬编码凭据 — 消除代码中62处硬编码IP/密码/路径，全部走配置
- [ ] **CONF-03**: 环境变量驱动配置 — 所有敏感配置通过.env或Nacos加载，不写死在代码中

### Code Quality

- [ ] **CODE-01**: 消除重复代码 — 提取公共逻辑到core/，消除脚本间重复的数据获取/错误处理/日志逻辑
- [ ] **CODE-02**: 模块边界清晰化 — core/scripts/services/workflows各层职责明确，禁止跨层直接调用
- [ ] **CODE-03**: 错误处理标准化 — 关键流水线脚本有try/catch和结构化日志，异常不静默吞掉
- [ ] **CODE-04**: 修复import路径问题 — 消除30+文件sys.path操控，使用正确的包结构和相对导入

### Business Agility

- [ ] **BIZ-01**: 策略配置化 — 选股策略变动只改YAML配置文件，不需要修改代码
- [ ] **BIZ-02**: 功能开关机制 — 因子/过滤器/数据源可通过配置开关启用/禁用，无需重新部署
- [ ] **BIZ-03**: 流水线步骤可独立执行 — 每个流水线步骤可单独运行和调试，不必跑全流程

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Monitoring & Alerting

- **MONIT-01**: Grafana仪表盘可视化调度状态
- **MONIT-02**: 异常自动通知到钉钉/微信
- **MONIT-03**: 数据质量趋势报告

### Infrastructure

- **INFRA-01**: Docker健康检查和自动重启
- **INFRA-02**: 数据库连接池统一管理
- **INFRA-03**: API网关认证

### Testing

- **TEST-01**: 流水线脚本集成测试
- **TEST-02**: 因子计算单元测试覆盖率>80%
- **TEST-03**: 回归测试套件

## Out of Scope

| Feature | Reason |
|---------|--------|
| 架构重写 | 生产服务不能停，现有架构合理 |
| 前端UI重构 | 聚焦后端稳定性 |
| 实时WebSocket推送 | 非紧急需求 |
| 用户认证系统 | 内部服务，暂不需要 |
| CI/CD完善 | 先稳定再说 |
| Kestra迁移 | 保留APScheduler为主，Kestra作为可选 |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| SCHED-01 | — | Pending |
| SCHED-02 | — | Pending |
| SCHED-03 | — | Pending |
| SCHED-04 | — | Pending |
| CONF-01 | — | Pending |
| CONF-02 | — | Pending |
| CONF-03 | — | Pending |
| CODE-01 | — | Pending |
| CODE-02 | — | Pending |
| CODE-03 | — | Pending |
| CODE-04 | — | Pending |
| BIZ-01 | — | Pending |
| BIZ-02 | — | Pending |
| BIZ-03 | — | Pending |

**Coverage:**
- v1 requirements: 14 total
- Mapped to phases: 0
- Unmapped: 14 ⚠️

---
*Requirements defined: 2026-05-12*
*Last updated: 2026-05-12 after initial definition*
