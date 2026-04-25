---
phase: 3
name: 工作流清理与归档
status: planned
wave: 1
autonomous: true
created: 2026-04-25
---

# Plan 03-01: 删除废弃 Kestra 工作流文件

## Objective

删除 8 个已被标记为废弃的 Kestra 工作流 YAML 文件，清理代码库。这些工作流已被 Phase 2 的统一工作流替代，删除日期已到期 (2025-06-25)。

## Background

根据 Phase 2 (Kestra 工作流合并) 的规划，以下工作流已被标记为废弃：
- 数据采集类：被 `xcnstock_data_collection_unified` 替代
- 监控维护类：被 `xcnstock_monitoring_unified` 替代

废弃工作流清单已在 `kestra/flows/deprecated/DEPRECATED_WORKFLOWS.md` 中记录。

## Tasks

### Task 1: 验证删除前状态
**文件**: `kestra/flows/*.yml`

执行删除前的验证检查：

1. 确认以下 8 个废弃工作流文件存在：
   - `xcnstock_daily_update.yml`
   - `xcnstock_data_collection_with_ge.yml`
   - `xcnstock_smart_pipeline.yml`
   - `xcnstock_data_pipeline_simple.yml`
   - `xcnstock_morning_report_simple.yml`
   - `xcnstock_data_inspection.yml`
   - `xcnstock_system_monitor.yml`
   - `xcnstock_data_collection.yml`

2. 确认替代的统一工作流正常运行：
   - `xcnstock_data_collection_unified.yml` 存在且有效
   - `xcnstock_monitoring_unified.yml` 存在且有效

3. 运行健康检查脚本验证系统状态：
   ```bash
   ./scripts/pipeline/verify_migration.sh
   ```

**Success Criteria**:
- 所有 8 个废弃文件存在
- 统一工作流文件存在
- 健康检查通过

---

### Task 2: 删除废弃工作流文件
**文件**: `kestra/flows/*.yml`

删除以下 8 个废弃工作流文件：

```bash
# 数据采集类 (4个)
kestra/flows/xcnstock_daily_update.yml
kestra/flows/xcnstock_data_collection_with_ge.yml
kestra/flows/xcnstock_smart_pipeline.yml
kestra/flows/xcnstock_data_collection.yml

# 监控维护类 (2个)
kestra/flows/xcnstock_data_inspection.yml
kestra/flows/xcnstock_system_monitor.yml

# 其他废弃 (2个)
kestra/flows/xcnstock_data_pipeline_simple.yml
kestra/flows/xcnstock_morning_report_simple.yml
```

**注意**: `xcnstock_debug.yml` 虽然也被 grep 匹配到，但需要确认是否真的废弃。检查文件内容后再决定是否删除。

**Success Criteria**:
- 8 个废弃文件已被删除
- `git status` 显示正确的删除变更

---

### Task 3: 更新废弃工作流文档
**文件**: `kestra/flows/deprecated/DEPRECATED_WORKFLOWS.md`

更新文档状态为"已删除"：

1. 在文档顶部添加删除完成标记
2. 更新删除计划表格，标记实际删除日期
3. 添加回滚说明（如需从历史恢复）

**Success Criteria**:
- 文档反映已删除状态
- 包含回滚指导

---

### Task 4: 提交变更
**提交信息**: `cleanup(kestra): 删除8个废弃工作流文件`

提交删除操作：

```bash
git add -A
git commit -m "cleanup(kestra): 删除8个废弃工作流文件

删除已被统一工作流替代的废弃工作流：
- xcnstock_daily_update.yml → xcnstock_data_collection_unified
- xcnstock_data_collection_with_ge.yml → xcnstock_data_collection_unified
- xcnstock_smart_pipeline.yml → xcnstock_data_pipeline
- xcnstock_data_collection.yml → xcnstock_data_collection_unified
- xcnstock_data_inspection.yml → xcnstock_monitoring_unified
- xcnstock_system_monitor.yml → xcnstock_monitoring_unified
- xcnstock_data_pipeline_simple.yml → xcnstock_data_collection_unified
- xcnstock_morning_report_simple.yml → xcnstock_monitoring_unified

废弃日期: 2025-05-25
计划删除日期: 2025-06-25
实际删除日期: 2026-04-25

如需回滚: git revert HEAD"
```

**Success Criteria**:
- 提交成功
- 提交信息包含完整变更说明

---

## Files Modified

- `kestra/flows/xcnstock_daily_update.yml` (删除)
- `kestra/flows/xcnstock_data_collection_with_ge.yml` (删除)
- `kestra/flows/xcnstock_smart_pipeline.yml` (删除)
- `kestra/flows/xcnstock_data_collection.yml` (删除)
- `kestra/flows/xcnstock_data_inspection.yml` (删除)
- `kestra/flows/xcnstock_system_monitor.yml` (删除)
- `kestra/flows/xcnstock_data_pipeline_simple.yml` (删除)
- `kestra/flows/xcnstock_morning_report_simple.yml` (删除)
- `kestra/flows/deprecated/DEPRECATED_WORKFLOWS.md` (更新)

## Verification

### 删除后验证
1. 运行健康检查脚本：
   ```bash
   ./scripts/pipeline/verify_migration.sh
   ```

2. 确认活跃工作流数量：
   ```bash
   ls kestra/flows/*.yml | wc -l
   # 预期: 约 15 个（原 23 个 - 8 个废弃）
   ```

3. 确认统一工作流仍能正常加载：
   ```bash
   # 检查 YAML 语法
   python -c "import yaml; yaml.safe_load(open('kestra/flows/xcnstock_data_collection_unified.yml'))"
   python -c "import yaml; yaml.safe_load(open('kestra/flows/xcnstock_monitoring_unified.yml'))"
   ```

---

# Plan 03-02: 归档 OpenSpec 变更文档

## Objective

将 Phase 2 的 OpenSpec 变更文档从 `changes/` 目录移动到 `changes/archive/` 目录，完成项目归档。

## Background

Phase 2 (kestra-workflow-consolidation) 已完成并稳定运行，其变更文档需要归档保存。

## Tasks

### Task 1: 创建归档目录结构
**目录**: `openspec/openspec/changes/archive/kestra-workflow-consolidation/`

创建归档目录：

```bash
mkdir -p openspec/openspec/changes/archive/kestra-workflow-consolidation/specs/data-pipeline
mkdir -p openspec/openspec/changes/archive/kestra-workflow-consolidation/specs/morning-report
mkdir -p openspec/openspec/changes/archive/kestra-workflow-consolidation/specs/unified-data-collection
mkdir -p openspec/openspec/changes/archive/kestra-workflow-consolidation/specs/unified-monitoring
```

**Success Criteria**:
- 目录结构创建成功

---

### Task 2: 移动变更文档
**源**: `openspec/openspec/changes/kestra-workflow-consolidation/`  
**目标**: `openspec/openspec/changes/archive/kestra-workflow-consolidation/`

移动以下文件：

```bash
# 核心文档
openspec/openspec/changes/kestra-workflow-consolidation/proposal.md
openspec/openspec/changes/kestra-workflow-consolidation/design.md
openspec/openspec/changes/kestra-workflow-consolidation/tasks.md
openspec/openspec/changes/kestra-workflow-consolidation/.openspec.yaml

# 规格文档
openspec/openspec/changes/kestra-workflow-consolidation/specs/data-pipeline/spec.md
openspec/openspec/changes/kestra-workflow-consolidation/specs/morning-report/spec.md
openspec/openspec/changes/kestra-workflow-consolidation/specs/unified-data-collection/spec.md
openspec/openspec/changes/kestra-workflow-consolidation/specs/unified-monitoring/spec.md
```

**Success Criteria**:
- 所有文件成功移动到归档目录
- 源目录为空（可删除）

---

### Task 3: 更新归档索引
**文件**: `openspec/openspec/changes/archive/ARCHIVE_INDEX.md`

创建或更新归档索引文件：

```markdown
# 变更文档归档索引

## 归档记录

### 2026-04-25 - kestra-workflow-consolidation
**变更单**: kestra-workflow-consolidation  
**描述**: Kestra 工作流合并 - 将 18 个工作流合并为 11 个  
**状态**: ✅ 已完成  
**归档日期**: 2026-04-25  
**路径**: `archive/kestra-workflow-consolidation/`

**主要交付物**:
- 统一数据采集工作流 (5合1)
- 统一监控工作流 (3合1)
- 迁移脚本和验证工具

**相关阶段**: Phase 2
```

**Success Criteria**:
- 索引文件创建/更新成功
- 包含完整的归档信息

---

### Task 4: 清理空目录
**目录**: `openspec/openspec/changes/kestra-workflow-consolidation/`

删除已空的源目录：

```bash
rm -rf openspec/openspec/changes/kestra-workflow-consolidation/
```

**Success Criteria**:
- 源目录已删除
- 无残留文件

---

### Task 5: 提交变更
**提交信息**: `docs(openspec): 归档 kestra-workflow-consolidation 变更文档`

提交归档操作：

```bash
git add -A
git commit -m "docs(openspec): 归档 kestra-workflow-consolidation 变更文档

将 Phase 2 变更文档归档：
- 移动: changes/kestra-workflow-consolidation/ → changes/archive/
- 更新: archive/ARCHIVE_INDEX.md

变更单: kestra-workflow-consolidation
完成日期: 2026-04-25
归档日期: 2026-04-25"
```

**Success Criteria**:
- 提交成功
- 包含完整的归档说明

---

## Files Modified

- `openspec/openspec/changes/kestra-workflow-consolidation/` (删除，内容移动到 archive)
- `openspec/openspec/changes/archive/kestra-workflow-consolidation/` (新增)
- `openspec/openspec/changes/archive/ARCHIVE_INDEX.md` (新增/更新)

## Verification

### 归档后验证
1. 确认归档目录存在且内容完整：
   ```bash
   ls -la openspec/openspec/changes/archive/kestra-workflow-consolidation/
   ```

2. 确认源目录已删除：
   ```bash
   test ! -d openspec/openspec/changes/kestra-workflow-consolidation/ && echo "已删除"
   ```

3. 确认索引文件存在：
   ```bash
   cat openspec/openspec/changes/archive/ARCHIVE_INDEX.md
   ```

---

# Summary

## Phase 3 执行计划总结

| Plan | 任务 | 预计耗时 | 关键产出 |
|------|------|----------|----------|
| 03-01 | 删除废弃工作流 | 10分钟 | 删除 8 个 YAML 文件 |
| 03-02 | 归档变更文档 | 10分钟 | 归档到 archive/ 目录 |

## 依赖关系

```
Plan 03-01 (删除废弃工作流)
    │
    ├── 依赖: Phase 2 完成并稳定运行
    └── 产出: 清理后的 kestra/flows/ 目录

Plan 03-02 (归档变更文档)
    │
    ├── 依赖: Plan 03-01 完成
    └── 产出: 归档的变更文档
```

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 误删活跃工作流 | 高 | 删除前验证文件包含 deprecated 标记 |
| 归档后文档丢失 | 低 | 使用 git mv 保留历史记录 |
| 统一工作流故障 | 高 | 删除前运行健康检查 |

## 回滚方案

如需回滚：

```bash
# 回滚删除操作
git revert <cleanup-commit-hash>

# 回滚归档操作
git revert <archive-commit-hash>
```

---

*Phase: 03-workflow-cleanup*  
*Created: 2026-04-25*  
*Status: Ready for execution*
