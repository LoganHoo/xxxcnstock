# Phase 3: 工作流清理与归档 - Context

**Gathered:** 2026-04-25
**Status:** Ready for planning
**Source:** ROADMAP.md Phase 3 definition

---

<domain>
## Phase Boundary

本阶段是 Kestra 工作流合并项目 (Phase 2) 的收尾工作。Phase 2 已将 18 个工作流合并为 11 个，减少了 38.9% 的工作流数量。本阶段负责：

1. **删除废弃工作流文件** - 清理 9 个已被标记为废弃的 Kestra 工作流 YAML 文件
2. **归档变更文档** - 将 OpenSpec 变更文档移动到 archive 目录

**范围边界：**
- ✅ 删除废弃的 Kestra 工作流 YAML 文件
- ✅ 归档 kestra-workflow-consolidation 变更文档
- ✅ 更新归档索引
- ❌ 不修改任何活跃工作流
- ❌ 不修改业务逻辑代码
- ❌ 不涉及数据库迁移

</domain>

<decisions>
## Implementation Decisions

### 废弃工作流列表 (已确认)
根据 Phase 2 的合并结果，以下 9 个工作流已被标记为废弃：

| 文件名 | 状态 | 替代方案 |
|--------|------|----------|
| xcnstock_daily_update.yml | 废弃 | xcnstock_data_collection_unified.yml |
| xcnstock_data_collection_with_ge.yml | 废弃 | xcnstock_data_collection_unified.yml |
| xcnstock_smart_pipeline.yml | 废弃 | xcnstock_data_collection_unified.yml |
| xcnstock_data_pipeline_simple.yml | 废弃 | xcnstock_data_collection_unified.yml |
| xcnstock_morning_report_simple.yml | 废弃 | xcnstock_monitoring_unified.yml |
| xcnstock_data_inspection.yml | 废弃 | xcnstock_monitoring_unified.yml |
| xcnstock_system_monitor.yml | 废弃 | xcnstock_monitoring_unified.yml |
| xcnstock_debug.yml | 废弃 | xcnstock_monitoring_unified.yml |
| (第9个待确认) | 废弃 | - |

### 归档策略
- 源位置: `openspec/openspec/changes/kestra-workflow-consolidation/`
- 目标位置: `openspec/openspec/changes/archive/kestra-workflow-consolidation/`
- 保留完整目录结构
- 更新 archive 索引文件

### 安全策略
- 删除前进行健康检查验证
- 保留备份 2 周 (通过 Git 历史)
- 支持快速回滚

### Claude's Discretion
- 具体的文件删除顺序
- 归档索引的格式设计
- 完成报告的详细程度

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### 项目文档
- `openspec/openspec/changes/kestra-workflow-consolidation/proposal.md` - Phase 2 提案
- `openspec/openspec/changes/kestra-workflow-consolidation/design.md` - Phase 2 设计文档
- `openspec/openspec/changes/kestra-workflow-consolidation/tasks.md` - Phase 2 任务清单

### 工作流目录
- `kestra/flows/` - Kestra 工作流文件目录

### 活跃工作流 (保留)
- `xcnstock_data_collection_unified.yml` - 统一数据采集
- `xcnstock_monitoring_unified.yml` - 统一监控
- `xcnstock_morning_report.yml` - 晨会报告
- `xcnstock_data_pipeline.yml` - 数据流水线
- `xcnstock_data_collection.yml` - 数据采集
- `xcnstock_post_market_report.yml` - 盘后报告
- `xcnstock_post_market_analysis.yml` - 盘后分析
- `xcnstock_post_market_data.yml` - 盘后数据
- `xcnstock_notification.yml` - 通知
- `xcnstock_morning_pipeline.yml` - 晨会流水线
- `xcnstock_post_market_pipeline.yml` - 盘后流水线

### 已归档变更
- `openspec/openspec/changes/archive/2026-04-25-optimize-data-pipeline-performance/` - Phase 1 归档示例

</canonical_refs>

<specifics>
## Specific Ideas

### 废弃工作流识别方法
1. 检查每个工作流文件头部的 `# Status: deprecated` 标记
2. 对比 Phase 2 设计文档中的废弃清单
3. 确认没有活跃引用 (通过 grep 检查其他文件)

### 健康检查验证
删除前必须运行健康检查脚本：
```bash
./scripts/pipeline/verify_migration.sh
```

### 归档索引格式
参考现有 archive 目录结构：
```
archive/
├── 2026-04-25-optimize-data-pipeline-performance/
└── kestra-workflow-consolidation/  (新归档)
```

</specifics>

<deferred>
## Deferred Ideas

- 自动化废弃工作流检测脚本 (未来优化)
- 工作流依赖关系可视化 (未来优化)
- 归档文档的自动索引生成 (未来优化)

</deferred>

---

*Phase: 03-workflow-cleanup*
*Context gathered: 2026-04-25*
