# Kestra 工作流优化指南

## 优化总结

### 1. 已修复问题

| 问题 | 修复方案 | 状态 |
|------|----------|------|
| **ID冲突** | `morning_report_simple` ID改为 `xcnstock_morning_report_simple` | ✅ |
| **ID冲突** | `data_pipeline_simple` ID改为 `xcnstock_data_pipeline_simple` | ✅ |
| **缺少统一通知** | 新增 `xcnstock_notification` 工作流 | ✅ |
| **post_market_pipeline过大** | 拆分为3个子工作流 | ✅ |

### 2. 新增工作流

#### 2.1 统一通知工作流
- **文件**: `xcnstock_notification.yml`
- **用途**: 被其他工作流调用，统一发送执行结果通知
- **输入参数**:
  - `workflow_name`: 调用工作流名称
  - `status`: 执行状态 (success/failed/warning)
  - `message`: 通知消息
  - `duration`: 执行时长
  - `error_details`: 错误详情

#### 2.2 盘后数据层工作流
- **文件**: `xcnstock_post_market_data.yml`
- **职责**: 数据采集、验证、补采
- **包含任务**:
  1. 基础数据采集 (data_fetch)
  2. 更新大盘指数数据 (update_index_data)
  3. 龙虎榜数据采集 (dragon_tiger_fetch)
  4. 智能数据审计 (data_audit)
  5. 通知数据层完成

#### 2.3 盘后分析层工作流
- **文件**: `xcnstock_post_market_analysis.yml`
- **职责**: 指标计算、复盘分析、选股验证
- **包含任务**:
  1. CVD指标计算 (calculate_cvd)
  2. 当日复盘分析 (market_review)
  3. 选股复盘 (picks_review)
  4. 回撤分析 (drawdown_analysis)
  5. 每日选股复盘 (daily_selection_review)
  6. 通知分析层完成

#### 2.4 盘后报告层工作流
- **文件**: `xcnstock_post_market_report.yml`
- **职责**: 报告生成、推送、跟踪更新
- **包含任务**:
  1. 推送完整复盘报告 (review_report)
  2. 推送精简复盘快报 (review_brief)
  3. 更新推荐股票跟踪 (update_tracking)
  4. 盘后监控摘要 (evening_monitoring_summary)
  5. 通知报告层完成

### 3. 优化后的架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    优化后的盘后流水线架构                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  xcnstock_post_market_pipeline (主控工作流)                      │
│         │                                                        │
│         ├──► xcnstock_post_market_data (数据层)                  │
│         │              ├─ 数据采集                               │
│         │              ├─ 指数更新                               │
│         │              ├─ 龙虎榜采集                             │
│         │              └─ 数据审计                               │
│         │                                                        │
│         ├──► xcnstock_post_market_analysis (分析层)              │
│         │              ├─ CVD计算                                │
│         │              ├─ 复盘分析                               │
│         │              ├─ 选股复盘                               │
│         │              └─ 回撤分析                               │
│         │                                                        │
│         └──► xcnstock_post_market_report (报告层)                │
│                        ├─ 复盘报告                               │
│                        ├─ 复盘快报                               │
│                        ├─ 跟踪更新                               │
│                        └─ 监控摘要                               │
│                                                                  │
│  统一通知: xcnstock_notification (被各层调用)                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4. 统一配置标准

#### 4.1 超时配置标准

| 任务类型 | 超时时间 | 说明 |
|----------|----------|------|
| 数据采集 | PT60M | 大量数据下载 |
| 数据审计 | PT30M | 质量检查 |
| 指标计算 | PT10M | CPU密集型 |
| 复盘分析 | PT15M | 综合分析 |
| 报告生成 | PT10M | 文档生成 |
| 通知发送 | PT1M | 快速操作 |

#### 4.2 重试配置标准

```yaml
# 关键任务重试
retry:
  type: exponential
  interval: PT2M
  maxInterval: PT10M
  maxAttempt: 5

# 一般任务重试
retry:
  type: constant
  interval: PT1M
  maxAttempt: 3
```

#### 4.3 资源限制建议

```yaml
# 在 pluginDefaults 中添加
pluginDefaults:
  - type: io.kestra.plugin.scripts.python.Script
    values:
      runner: PROCESS
      # 建议添加资源限制
      # memory: 4Gi
      # cpu: 2
```

### 5. 进一步优化建议

#### 5.1 短期优化
1. **为所有工作流添加通知调用**
   - 在关键工作流末尾调用 `xcnstock_notification`
   - 在失败时发送告警通知

2. **统一 runner 配置**
   - 所有工作流使用 `runner: PROCESS` 保持一致

3. **添加任务标签**
   - 为每个任务添加 `labels` 便于监控

#### 5.2 中期优化
1. **重构 post_market_pipeline**
   - 改为调用3个子工作流
   - 使用 `Subflow` 任务串联

2. **添加并行执行**
   - 独立任务使用 `Parallel` 并行执行

3. **添加条件分支**
   - 使用 `If` 任务实现条件执行

#### 5.3 长期优化
1. **添加工作流模板**
   - 创建可复用的任务模板

2. **优化依赖管理**
   - 使用 `beforeCommands` 统一管理依赖安装

3. **添加监控集成**
   - 集成 Prometheus 指标收集

### 6. 工作流清单 (优化后)

| # | 工作流ID | 类型 | 说明 |
|---|----------|------|------|
| 1 | xcnstock_data_pipeline | 核心 | 主数据流水线 |
| 2 | xcnstock_data_collection | 核心 | 统一数据采集 |
| 3 | xcnstock_data_collection_with_ge | 核心 | GE验证采集 |
| 4 | xcnstock_daily_update | 核心 | 每日数据更新 |
| 5 | xcnstock_smart_pipeline | 核心 | 智能数据流水线 |
| 6 | xcnstock_morning_pipeline | 时段 | 盘前流水线 |
| 7 | xcnstock_morning_report | 时段 | 盘前涨停板报告 |
| 8 | xcnstock_morning_report_simple | 时段 | 盘前报告简化版 |
| 9 | xcnstock_post_market_pipeline | 时段 | 盘后流水线（主控）|
| 10 | xcnstock_post_market_data | 时段 | 盘后数据层 |
| 11 | xcnstock_post_market_analysis | 时段 | 盘后分析层 |
| 12 | xcnstock_post_market_report | 时段 | 盘后报告层 |
| 13 | xcnstock_evening_pipeline | 时段 | 晚间流水线 |
| 14 | xcnstock_daily_report | 报告 | 每日选股报告 |
| 15 | xcnstock_weekly_review | 报告 | 周度复盘 |
| 16 | xcnstock_verify_picks | 报告 | 选股验证 |
| 17 | xcnstock_system_monitor | 监控 | 系统监控 |
| 18 | xcnstock_daily_maintenance | 监控 | 日常维护 |
| 19 | xcnstock_data_inspection | 监控 | 数据巡检 |
| 20 | xcnstock_data_pipeline_simple | 工具 | 简化版流水线 |
| 21 | xcnstock_debug | 工具 | 调试工具 |
| 22 | xcnstock_notification | 工具 | 统一通知 |

**总计: 22个工作流** (原18个 + 新增4个)
