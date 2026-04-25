# XCNStock 路线图

## 里程碑概览

| 里程碑 | 状态 | 完成日期 | 主要交付物 |
|-------|------|---------|-----------|
| M1: 数据流水线优化 | ✅ | 2026-04-25 | 并行采集、增量处理、Polars指标 |
| M2: 工作流合并 | ✅ | 2026-04-25 | 统一采集/监控工作流 |
| M3: 工作流清理 | ⏳ | 2025-06-25 | 删除废弃文件、归档文档 |
| M4: 性能提升V2 | 📋 | 待定 | 待规划 |

---

## Phase 1: 数据流水线性能优化 ✅

**状态**: 已完成  
**时间**: 2026-04-25  
**变更单**: `optimize-data-pipeline-performance`

### 交付物
- [x] 并行数据获取引擎
- [x] 增量数据处理系统
- [x] Polars技术指标实现
- [x] 多级缓存系统
- [x] 分布式锁协调
- [x] 监控告警系统
- [x] 部署配置和文档

### 关键指标
- 数据采集: 5000+股票 < 5分钟
- 指标计算: 10,000条 < 2ms
- 测试通过率: 100%

---

## Phase 2: Kestra工作流合并 ✅

**状态**: 已完成 96.7%  
**时间**: 2026-04-25  
**变更单**: `kestra-workflow-consolidation`

### 交付物
- [x] 统一数据采集工作流 (5合1)
- [x] 统一监控工作流 (3合1)
- [x] 晨会报告调试模式
- [x] 9个废弃工作流标记
- [x] 迁移脚本 (verify/rollback/health)
- [x] 健康检查通过

### 待完成
- [ ] 删除废弃工作流文件 (计划: 2025-06-25)
- [ ] 归档变更文档 (计划: 2025-06-25)

---

## Phase 3: 工作流清理与归档 📋

**状态**: 已规划，待执行  
**计划时间**: 2026-04-25  
**负责人**: AI Assistant

### 执行计划

#### Plan 03-01: 删除废弃工作流文件
**预计耗时**: 10分钟  
**状态**: 📝 已规划

**任务清单**:
- [ ] 验证删除前状态（健康检查）
- [ ] 删除 8 个废弃工作流文件
- [ ] 更新废弃工作流文档
- [ ] 提交变更

**废弃文件清单**:
| 文件名 | 替代方案 |
|--------|----------|
| `xcnstock_daily_update.yml` | `xcnstock_data_collection_unified` (mode=daily) |
| `xcnstock_data_collection_with_ge.yml` | `xcnstock_data_collection_unified` (mode=ge) |
| `xcnstock_smart_pipeline.yml` | `xcnstock_data_pipeline` |
| `xcnstock_data_collection.yml` | `xcnstock_data_collection_unified` (mode=standard) |
| `xcnstock_data_inspection.yml` | `xcnstock_monitoring_unified` (check_type=data) |
| `xcnstock_system_monitor.yml` | `xcnstock_monitoring_unified` (check_type=system) |
| `xcnstock_data_pipeline_simple.yml` | `xcnstock_data_collection_unified` |
| `xcnstock_morning_report_simple.yml` | `xcnstock_monitoring_unified` |

#### Plan 03-02: 归档变更文档
**预计耗时**: 10分钟  
**状态**: 📝 已规划

**任务清单**:
- [ ] 创建归档目录结构
- [ ] 移动变更文档到 archive/
- [ ] 更新归档索引
- [ ] 清理空目录
- [ ] 提交变更

**归档内容**:
- `openspec/openspec/changes/kestra-workflow-consolidation/` → `openspec/openspec/changes/archive/kestra-workflow-consolidation/`

---

## Phase 4: 性能提升V2 📋

**状态**: 待规划  
**计划时间**: 待定

### 潜在优化方向

#### 4.1 技术指标扩展
- 添加更多技术指标 (ADX, OBV, etc.)
- 优化Polars计算性能
- 添加指标缓存

#### 4.2 数据存储优化
- 分区Parquet文件
- 压缩优化
- 查询性能提升

#### 4.3 工作流优化
- 动态调度
- 资源自动伸缩
- 故障自动恢复

---

## 依赖关系图

```
M1: 数据流水线优化 ✅
    │
    ▼
M2: 工作流合并 ✅
    │
    ▼
M3: 工作流清理 ⏳ (依赖M2完成并稳定运行2个月)
    │
    ▼
M4: 性能提升V2 📋 (依赖M3完成)
```

---

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|-----|------|---------|
| 废弃工作流删除后发现问题 | 高 | 保留备份2周，支持快速回滚 |
| 新工作流性能不达标 | 中 | 持续监控，设置告警阈值 |
| 数据质量问题 | 高 | 多层级验证，异常自动重试 |

---

## 更新记录

| 日期 | 更新内容 | 更新人 |
|-----|---------|-------|
| 2026-04-25 | 创建路线图，标记M1/M2完成 | AI Assistant |
| 2026-04-25 | 添加M3计划 (2025-06-25) | AI Assistant |
