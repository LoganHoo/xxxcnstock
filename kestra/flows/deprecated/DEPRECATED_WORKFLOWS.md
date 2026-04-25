# 废弃工作流清单

> ✅ **已删除** - 这些工作流已于 2026-04-25 删除
> 如需恢复，请使用 `git revert` 回滚

---

## 删除完成标记

**删除日期**: 2026-04-25  
**删除操作**: cleanup(kestra) 提交  
**删除文件数**: 8 个  
**状态**: ✅ 已完成

---

## 已删除工作流列表

### 数据采集类 (被 xcnstock_data_collection_unified 替代)

| 原工作流 | 替代方案 | 状态 |
|---------|---------|------|
| `xcnstock_data_collection.yml` | `xcnstock_data_collection_unified` (mode=standard) | ✅ 已删除 |
| `xcnstock_daily_update.yml` | `xcnstock_data_collection_unified` (mode=daily) | ✅ 已删除 |
| `xcnstock_data_collection_with_ge.yml` | `xcnstock_data_collection_unified` (mode=ge) | ✅ 已删除 |
| `xcnstock_smart_pipeline.yml` | `xcnstock_data_pipeline` (skip_if_fresh=true) | ✅ 已删除 |
| `xcnstock_data_pipeline_simple.yml` | `xcnstock_data_collection_unified` | ✅ 已删除 |

### 监控维护类 (被 xcnstock_monitoring_unified 替代)

| 原工作流 | 替代方案 | 状态 |
|---------|---------|------|
| `xcnstock_system_monitor.yml` | `xcnstock_monitoring_unified` (check_type=system) | ✅ 已删除 |
| `xcnstock_data_inspection.yml` | `xcnstock_monitoring_unified` (check_type=data) | ✅ 已删除 |
| `xcnstock_morning_report_simple.yml` | `xcnstock_monitoring_unified` | ✅ 已删除 |

---

## 回滚方案

如需恢复已删除的工作流:

```bash
# 方法1: 使用 git revert 回滚删除提交
git log --oneline --all | grep "cleanup(kestra)"
# 找到删除提交的 hash，然后:
git revert <commit-hash>

# 方法2: 从历史分支恢复
git show <commit-hash>:kestra/flows/xcnstock_daily_update.yml > kestra/flows/xcnstock_daily_update.yml
```

---

## 历史记录

### 删除计划 (原始)

| 日期 | 操作 | 状态 |
|-----|------|------|
| 2025-05-25 | 标记废弃，发送通知 | ✅ 完成 |
| 2025-06-01 | 禁用旧工作流触发器 | ✅ 完成 |
| 2025-06-25 | 删除旧工作流文件 | ✅ 提前完成 (2026-04-25) |

### 迁移指南 (历史参考)

**数据采集工作流迁移示例:**
```yaml
# 原工作流 (已删除)
# xcnstock_data_collection.yml
triggers:
  - id: daily_schedule
    cron: "0 16 * * *"

# 新工作流 (当前使用)
# xcnstock_data_collection_unified.yml
triggers:
  - id: daily_standard_schedule
    cron: "0 16 * * *"
    inputs:
      mode: "standard"
```

**监控工作流迁移示例:**
```yaml
# 原工作流 (已删除)
# xcnstock_system_monitor.yml
triggers:
  - id: daily_cleanup_schedule
    cron: "0 3 * * *"

# 新工作流 (当前使用)
# xcnstock_monitoring_unified.yml
triggers:
  - id: daily_maintenance_schedule
    cron: "0 3 * * *"
    inputs:
      check_type: "full"
```

---

*文档更新日期: 2026-04-25*  
*关联阶段: Phase 3 - 工作流清理与归档*
