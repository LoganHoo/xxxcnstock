# XCNStock Kestra 工作流迁移指南

## 概述

本文档指导如何将旧的分散工作流迁移到新的统一工作流架构。

## 迁移前后对比

### 数据采集工作流

| 旧工作流 | 新工作流 | 参数 |
|---------|---------|------|
| `xcnstock_data_collection` | `xcnstock_data_collection_unified` | `mode=standard` |
| `xcnstock_daily_update` | `xcnstock_data_collection_unified` | `mode=daily` |
| `xcnstock_data_collection_with_ge` | `xcnstock_data_collection_unified` | `mode=ge` |

### 监控维护工作流

| 旧工作流 | 新工作流 | 参数 |
|---------|---------|------|
| `xcnstock_system_monitor` | `xcnstock_monitoring_unified` | `check_type=system` |
| `xcnstock_data_inspection` | `xcnstock_monitoring_unified` | `check_type=data` |

## 迁移步骤

### 步骤 1: 部署新工作流

```bash
# 部署统一数据采集工作流
kestra flows update xcnstock_data_collection_unified \
  -f kestra/flows/xcnstock_data_collection_unified.yml

# 部署统一监控工作流
kestra flows update xcnstock_monitoring_unified \
  -f kestra/flows/xcnstock_monitoring_unified.yml
```

### 步骤 2: 验证新工作流

```bash
# 手动触发测试
kestra executions create xcnstock.xcnstock_data_collection_unified \
  --data '{"mode": "standard", "target_date": "2025-04-25"}'

# 查看执行状态
kestra executions list xcnstock.xcnstock_data_collection_unified
```

### 步骤 3: 禁用旧工作流触发器

```bash
# 禁用旧数据采集工作流
kestra triggers disable xcnstock.xcnstock_data_collection daily_collection
kestra triggers disable xcnstock.xcnstock_daily_update daily_schedule
kestra triggers disable xcnstock.xcnstock_data_collection_with_ge daily_schedule

# 禁用旧监控工作流
kestra triggers disable xcnstock.xcnstock_system_monitor daily_cleanup_schedule
kestra triggers disable xcnstock.xcnstock_system_monitor dashboard_schedule
kestra triggers disable xcnstock.xcnstock_data_inspection daily_inspection_trigger
```

### 步骤 4: 运行迁移验证脚本

```bash
# 验证迁移状态
python scripts/migrate_workflows.py verify

# 查看当前状态
python scripts/migrate_workflows.py status
```

## 新工作流使用指南

### 统一数据采集工作流

#### 标准采集模式
```yaml
# 采集当日数据（不更新股票列表）
inputs:
  mode: "standard"
  target_date: "2025-04-25"
```

#### 每日更新模式
```yaml
# 更新股票列表后采集
inputs:
  mode: "daily"
  target_date: "2025-04-25"
```

#### GE验证模式
```yaml
# 采集并执行GE数据验证
inputs:
  mode: "ge"
  target_date: "2025-04-25"
```

#### 补采历史数据
```yaml
# 补采指定区间
inputs:
  mode: "standard"
  start_date: "2025-04-01"
  end_date: "2025-04-10"
```

### 统一监控工作流

#### 完整检查
```yaml
# 执行所有检查项目
inputs:
  check_type: "full"
```

#### 仅数据检查
```yaml
# 只检查数据新鲜度和完整性
inputs:
  check_type: "data"
```

#### 仅系统维护
```yaml
# 只执行缓存清理
inputs:
  check_type: "system"
```

#### 仅生成监控面板
```yaml
# 只生成监控面板HTML
inputs:
  check_type: "dashboard"
```

## 调度配置

### 新工作流调度

```yaml
# 统一数据采集工作流
# - 每日 16:00: mode=daily (更新股票列表)
# - 每日 16:30: mode=ge (GE验证采集)

# 统一监控工作流
# - 每日 03:00: check_type=full (完整维护)
# - 每 10 分钟: check_type=dashboard (监控面板)
# - 每日 08:00: check_type=data (数据巡检)
```

## 回滚方案

如果迁移后发现问题，可以回滚到旧工作流：

```bash
# 方法 1: 使用迁移脚本
python scripts/migrate_workflows.py rollback

# 方法 2: 手动回滚
# 1. 禁用新工作流触发器
kestra triggers disable xcnstock.xcnstock_data_collection_unified daily_standard_schedule

# 2. 启用旧工作流触发器（编辑文件后重新部署）
# 将 xcnstock_data_collection.yml 中的 disabled: true 改为 disabled: false
kestra flows update xcnstock.xcnstock_data_collection \
  -f kestra/flows/xcnstock_data_collection.yml
```

## 故障排查

### 问题 1: 新工作流执行失败

**症状**: 工作流执行报错或超时

**排查步骤**:
1. 检查执行日志: `kestra executions logs <execution-id>`
2. 验证环境变量配置
3. 检查 Python 依赖是否安装

**解决方案**:
```bash
# 查看详细日志
kestra executions logs <execution-id> --tail 100

# 手动测试脚本
python scripts/pipeline/data_collect.py --date 2025-04-25
```

### 问题 2: 触发器未正确禁用

**症状**: 新旧工作流同时执行

**排查步骤**:
1. 检查触发器状态: `kestra triggers list xcnstock`
2. 验证工作流文件中的 disabled 设置

**解决方案**:
```bash
# 列出所有触发器
kestra triggers list xcnstock

# 禁用指定触发器
kestra triggers disable xcnstock.<workflow_id> <trigger_id>
```

### 问题 3: 数据不一致

**症状**: 新工作流采集的数据与旧工作流不同

**排查步骤**:
1. 对比执行参数
2. 检查数据文件时间戳
3. 验证采集范围

**解决方案**:
```bash
# 检查数据文件
ls -la data/kline/*.parquet | head -20

# 验证数据日期
python -c "import pyarrow.parquet as pq; \
  t = pq.read_table('data/kline/000001.parquet'); \
  print(max(t.column('trade_date').to_pylist()))"
```

## 验证清单

迁移完成后，请确认以下项目：

- [ ] 新工作流文件已部署到 Kestra
- [ ] 新工作流可以手动触发并成功执行
- [ ] 旧工作流触发器已禁用
- [ ] 调度时间符合预期
- [ ] 数据采集结果正确
- [ ] 监控报告正常生成
- [ ] 通知功能正常
- [ ] 迁移验证脚本通过

## 联系支持

如有问题，请：
1. 查看迁移报告: `data/reports/workflow_migration_report_*.md`
2. 检查执行日志: Kestra UI 或 CLI
3. 联系开发团队

## 附录

### 工作流文件清单

```
kestra/flows/
├── xcnstock_data_collection_unified.yml      # 新: 统一数据采集
├── xcnstock_monitoring_unified.yml           # 新: 统一监控
├── xcnstock_data_collection.yml              # 废弃: 原数据采集
├── xcnstock_daily_update.yml                 # 废弃: 原每日更新
├── xcnstock_data_collection_with_ge.yml      # 废弃: 原GE验证采集
├── xcnstock_system_monitor.yml               # 废弃: 原系统监控
└── xcnstock_data_inspection.yml              # 废弃: 原数据巡检
```

### 迁移脚本使用

```bash
# 验证迁移状态
python scripts/migrate_workflows.py verify

# 查看当前状态
python scripts/migrate_workflows.py status

# 回滚（谨慎使用）
python scripts/migrate_workflows.py rollback
```
