# XCNStock Kestra 工作流文档

## 概述

本文档详细描述了 XCNStock 量化交易系统的 Kestra 工作流架构，包括功能说明、业务流、执行时序和与原有 cron 任务的对比。

---

## 工作流清单

### 当前工作流 (11个)

| 工作流文件 | ID | 命名空间 | 任务数 | 类型 | 状态 |
|-----------|-----|---------|--------|------|------|
| xcnstock_daily_report.yml | xcnstock_daily_report | xcnstock | 7 | 选股报告 | ✅ 新增 |
| xcnstock_daily_update.yml | xcnstock_daily_update | xcnstock | 2 | 数据采集 | ⚠️ 待合并 |
| xcnstock_data_collection.yml | xcnstock_data_collection | xcnstock | 5 | 数据采集 | ✅ 新增(合并版) |
| xcnstock_data_collection_with_ge.yml | xcnstock_data_collection_with_ge | xcnstock | 2 | 数据采集 | ⚠️ 待合并 |
| xcnstock_data_inspection.yml | xcnstock_data_inspection | xcnstock | 4 | 数据质检 | ⚠️ 待合并 |
| xcnstock_data_pipeline.yml | xcnstock_data_pipeline | xcnstock | 6 | 综合流水线 | ⚠️ 待合并 |
| xcnstock_debug.yml | xcnstock_debug | xcnstock | 1 | 调试工具 | ✅ 保留 |
| xcnstock_morning_report.yml | xcnstock_morning_report | xcnstock | 1 | 盘前分析 | ✅ 保留 |
| xcnstock_smart_pipeline.yml | xcnstock_smart_pipeline | xcnstock | 7 | 智能流水线 | ⚠️ 待合并 |
| xcnstock_verify_picks.yml | xcnstock_verify_picks | xcnstock | 6 | 结果验证 | ✅ 新增 |
| xcnstock_weekly_review.yml | xcnstock_weekly_review | xcnstock | 4 | 周度报告 | ✅ 保留 |

---

## 工作流详细说明

### 1. xcnstock_data_collection (统一数据采集)

**功能描述**:
统一的数据采集工作流，整合了原有的多个分散采集任务。

**任务流程**:
```
阶段1: 更新股票列表
    └─ 获取最新上市/退市/停牌信息
    
阶段2: 采集K线数据
    ├─ 支持当日采集模式
    └─ 支持历史补采模式(start_date/end_date)
    
阶段3: GE数据质量验证
    ├─ Great Expectations验证
    ├─ 成功率检查(阈值80%)
    └─ 生成验证报告
    
阶段4: 智能补采缺失数据
    ├─ 识别缺失日期
    ├─ 自动补采(最多7天)
    └─ 生成补采报告
    
阶段5: 生成采集报告
    └─ 汇总各阶段结果
```

**输入参数**:
- `target_date`: 目标日期 (默认: 触发日期)
- `start_date`: 补采开始日期 (可选)
- `end_date`: 补采结束日期 (可选)
- `force_full`: 强制全量采集 (默认: false)

**触发方式**:
- 定时触发: 每日 16:30 (cron: "30 16 * * *")
- 手动触发: 支持补采参数

**对应 cron 任务**:
- data_fetch (16:00)
- data_fetch_retry (16:30)
- data_fetch_retry2 (17:35)
- data_audit_unified (16:50)

---

### 2. xcnstock_daily_report (每日选股报告)

**功能描述**:
生成每日选股报告，包含指标计算、选股策略执行、结果保存和复盘数据生成。

**任务流程**:
```
阶段1: 检查数据可用性
    └─ 检查当日数据是否已采集
    
阶段2: 指标计算
    ├─ CVD指标计算
    └─ 技术因子预计算
    
阶段3: 选股策略执行
    ├─ 主力痕迹选股
    └─ 评分排序
    
阶段4: 保存选股结果
    └─ 保存到数据库
    
阶段5: 生成报告
    └─ 生成选股报告文件
    
阶段6: 生成复盘数据
    └─ 创建review_pending文件(用于次日验证)
    
阶段7: 发送通知
    └─ 发送选股结果通知
```

**输入参数**:
- `report_date`: 报告日期 (默认: 触发日期)
- `skip_if_no_data`: 无数据时跳过 (默认: true)

**触发方式**:
- 定时触发: 每日 19:00 (cron: "0 19 * * *")
- 手动触发: 支持指定日期

**对应 cron 任务**:
- calculate_cvd (17:15)
- precompute (20:00)
- night_analysis (20:35)
- picks_review (17:45)

---

### 3. xcnstock_verify_picks (选股结果验证)

**功能描述**:
验证前一日选股结果的表现，计算收益率和胜率，更新策略绩效追踪。

**任务流程**:
```
阶段1: 识别待验证的选股
    └─ 查找review_pending文件
    
阶段2: 获取当日行情
    └─ 获取收盘价用于计算收益
    
阶段3: 验证选股表现
    ├─ 计算每只股票的收益率
    ├─ 统计平均收益和胜率
    └─ 生成验证报告
    
阶段4: 更新策略绩效
    ├─ 更新绩效追踪文件
    ├─ 计算累计指标
    └─ 保留最近90天记录
    
阶段5: 标记已验证
    ├─ 更新状态为verified
    └─ 移动文件到review_verified
    
阶段6: 发送验证报告
    └─ 发送验证结果通知
```

**输入参数**:
- `verify_date`: 验证日期 (默认: 触发日期，验证前一日的选股)

**触发方式**:
- 定时触发: 每日 15:30 (cron: "30 15 * * *")
- 手动触发: 支持指定日期

**对应 cron 任务**:
- daily_selection_review (18:10)
- drawdown_analysis (17:50)

---

### 4. xcnstock_morning_report (盘前涨停板分析)

**功能描述**:
交易日早上9:26执行，分析涨停板开板预测。

**任务流程**:
```
阶段1: 盘前涨停板分析
    └─ 执行morning_limit_up分析
```

**触发方式**:
- 定时触发: 每日 9:26 (cron: "26 9 * * 1-5")

**对应 cron 任务**:
- fund_behavior_report (09:26)

---

### 5. xcnstock_weekly_review (周度复盘报告)

**功能描述**:
每周日晚上执行，生成周度市场复盘和选股回顾。

**任务流程**:
```
阶段1: 周度市场数据汇总
    └─ 汇总周度市场数据
    
阶段2: 选股策略回顾
    └─ 回顾本周选股策略表现
    
阶段3: 生成周度报告
    └─ 生成周度复盘报告
    
阶段4: 发送通知
    └─ 发送周度报告
```

**输入参数**:
- `review_week`: 复盘周日期 (默认: 触发日期)
- `include_backtest`: 包含回测分析 (默认: true)

**触发方式**:
- 定时触发: 每周日 (cron: "0 20 * * 0")

**对应 cron 任务**:
- weekly_multi_period_update (周六 10:00)

---

### 6. xcnstock_debug (调试工作流)

**功能描述**:
调试工作流，检查容器环境和数据状态。

**任务流程**:
```
阶段1: 调试检查
    └─ 运行debug_check
```

**触发方式**:
- 手动触发

---

## 业务流架构

### 完整业务闭环

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         XCNStock 量化交易系统业务流                          │
└─────────────────────────────────────────────────────────────────────────────┘

交易日时间线:
═════════════

09:26  ┌─────────────────────────────────────────────────────────────────────┐
       │  xcnstock_morning_report (盘前涨停板分析)                          │
       │  ├─ 分析涨停板开板预测                                             │
       │  └─ 发送盘前报告                                                   │
       └─────────────────────────────────────────────────────────────────────┘

16:30  ┌─────────────────────────────────────────────────────────────────────┐
       │  xcnstock_data_collection (统一数据采集)                           │
       │  ├─ 更新股票列表                                                   │
       │  ├─ 采集K线数据                                                    │
       │  ├─ GE数据质量验证                                                 │
       │  ├─ 智能补采缺失数据                                               │
       │  └─ 生成采集报告                                                   │
       └─────────────────────────────────────────────────────────────────────┘

19:00  ┌─────────────────────────────────────────────────────────────────────┐
       │  xcnstock_daily_report (每日选股报告)                              │
       │  ├─ 检查数据可用性                                                 │
       │  ├─ 指标计算(CVD+技术因子)                                         │
       │  ├─ 选股策略执行                                                   │
       │  ├─ 保存选股结果                                                   │
       │  ├─ 生成选股报告                                                   │
       │  ├─ 生成复盘数据(review_pending)                                   │
       │  └─ 发送报告通知                                                   │
       └─────────────────────────────────────────────────────────────────────┘

次日15:30 ┌──────────────────────────────────────────────────────────────────┐
          │  xcnstock_verify_picks (选股结果验证)                           │
          │  ├─ 识别待验证的选股                                             │
          │  ├─ 获取当日行情                                                 │
          │  ├─ 验证选股表现(收益/胜率)                                      │
          │  ├─ 更新策略绩效                                                 │
          │  ├─ 标记已验证                                                   │
          │  └─ 发送验证报告                                                 │
          └──────────────────────────────────────────────────────────────────┘

周日20:00 ┌──────────────────────────────────────────────────────────────────┐
          │  xcnstock_weekly_review (周度复盘)                              │
          │  ├─ 周度市场数据汇总                                             │
          │  ├─ 选股策略回顾                                                 │
          │  ├─ 生成周度报告                                                 │
          │  └─ 发送周度报告                                                 │
          └──────────────────────────────────────────────────────────────────┘
```

### 数据流

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  股票列表   │────►│   K线数据   │────►│   指标数据  │────►│   选股结果  │
│  (parquet)  │     │  (parquet)  │     │  (parquet)  │     │   (json)    │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                                                                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  绩效追踪   │◄────│  验证报告   │◄────│  复盘数据   │◄────│  选股报告   │
│  (json)     │     │   (json)    │     │   (json)    │     │   (json)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

---

## 与 cron_tasks.yaml 的对比

### 任务映射关系

| Kestra 工作流 | 对应的 Cron 任务 | 说明 |
|--------------|-----------------|------|
| xcnstock_data_collection | data_fetch, data_fetch_retry, data_fetch_retry2, data_audit_unified | 合并多个采集和质检任务 |
| xcnstock_daily_report | calculate_cvd, precompute, night_analysis, picks_review | 合并指标计算和选股 |
| xcnstock_verify_picks | daily_selection_review, drawdown_analysis | 合并验证和回撤分析 |
| xcnstock_morning_report | fund_behavior_report | 对应核心盘前任务 |
| xcnstock_weekly_review | weekly_multi_period_update | 对应周度任务 |

### 优化对比

| 维度 | Cron 任务 (原) | Kestra 工作流 (新) | 优化效果 |
|------|---------------|-------------------|---------|
| 任务数量 | 40+ 个 | 5个核心工作流 | 精简 87.5% |
| 依赖管理 | 复杂的时间调度 | 工作流内嵌依赖 | 更清晰 |
| 失败重试 | 需外部脚本支持 | 内置 retry 机制 | 更可靠 |
| 可视化 | 无 | Kestra UI 完整展示 | 更直观 |
| 复盘验证 | 分散在多个任务 | xcnstock_verify_picks 统一处理 | 更完整 |

### 尚未迁移的 Cron 任务

以下任务尚未迁移到 Kestra，仍由 APScheduler 管理:

#### 盘前任务 (08:30-09:30)
- morning_data (08:30)
- collect_macro (08:32)
- collect_oil_dollar (08:34)
- collect_commodities (08:36)
- collect_sentiment (08:38)
- collect_news (08:40)
- market_analysis (08:42)
- morning_report (08:45)
- fund_behavior_resource_prepare (09:15)
- fund_behavior_resource_validate (09:22)
- fund_behavior_guardian_check (09:24)

#### 盘后任务 (16:00-19:00)
- dragon_tiger_fetch (16:32)
- market_review (17:30)
- review_report (18:00)
- review_brief (19:00)
- update_tracking (19:30)
- update_index_data (16:05)

#### 晚间任务 (20:00-21:00)
- manual_verification (20:30)

#### 系统维护任务
- scheduler_watchdog (每5分钟)
- cache_cleanup (03:00)
- data_integrity_check (09:05)
- generate_dashboard (每10分钟)
- morning_monitoring_summary (08:55)
- evening_monitoring_summary (18:05)

---

## 工作流依赖关系

```
xcnstock_data_collection
    │
    ▼
xcnstock_daily_report (依赖数据采集完成)
    │
    ▼
xcnstock_verify_picks (次日验证前一日选股)

xcnstock_morning_report (独立执行)

xcnstock_weekly_review (独立执行，每周日)
```

---

## 执行时序图

```
时间 ───────────────────────────────────────────────────────────────────────►

交易日:
  9:26  ┌─────────────────────────────────────────────────────────────────┐
        │                    xcnstock_morning_report                       │
        │                      (盘前涨停分析)                              │
        └─────────────────────────────────────────────────────────────────┘

  16:30 ┌─────────────────────────────────────────────────────────────────┐
        │                   xcnstock_data_collection                       │
        │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
        │  │股票列表 │ │K线采集  │ │GE验证   │ │智能补采 │ │生成报告 │   │
        │  │更新     │ │         │ │         │ │         │ │         │   │
        │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
        └─────────────────────────────────────────────────────────────────┘

  19:00 ┌─────────────────────────────────────────────────────────────────┐
        │                    xcnstock_daily_report                         │
        │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
        │  │检查数据 │ │指标计算 │ │选股策略 │ │保存结果 │ │生成报告 │   │
        │  │         │ │         │ │         │ │         │ │         │   │
        │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
        │  ┌─────────┐ ┌─────────┐                                        │
        │  │生成复盘 │ │发送通知 │                                        │
        │  │数据     │ │         │                                        │
        │  └─────────┘ └─────────┘                                        │
        └─────────────────────────────────────────────────────────────────┘

次日15:30 ┌───────────────────────────────────────────────────────────────┐
          │                   xcnstock_verify_picks                        │
          │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ │
          │  │识别待验 │ │获取行情 │ │验证表现 │ │更新绩效 │ │标记已验 │ │
          │  │证记录   │ │         │ │         │ │         │ │证       │ │
          │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ │
          │  ┌─────────┐                                                │
          │  │发送验证 │                                                │
          │  │报告     │                                                │
          │  └─────────┘                                                │
          └───────────────────────────────────────────────────────────────┘

周日20:00 ┌───────────────────────────────────────────────────────────────┐
          │                  xcnstock_weekly_review                        │
          │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
          │  │周度数据 │ │策略回顾 │ │生成报告 │ │发送报告 │            │
          │  │汇总     │ │         │ │         │ │         │            │
          │  └─────────┘ └─────────┘ └─────────┘ └─────────┘            │
          └───────────────────────────────────────────────────────────────┘
```

---

## 配置说明

### 环境变量

所有工作流使用以下环境变量:
- `PYTHONPATH`: 项目根目录 (/projects/xxxcnstock)
- `LOG_LEVEL`: 日志级别 (INFO)

### 通用变量

```yaml
variables:
  project_root: /projects/xxxcnstock
  python_path: "{{ vars.project_root }}"
  log_level: INFO
```

### 输入参数

各工作流支持以下输入参数:

**xcnstock_data_collection**:
- target_date: 目标日期
- start_date: 补采开始日期
- end_date: 补采结束日期
- force_full: 强制全量采集

**xcnstock_daily_report**:
- report_date: 报告日期
- skip_if_no_data: 无数据时跳过

**xcnstock_verify_picks**:
- verify_date: 验证日期

**xcnstock_weekly_review**:
- review_week: 复盘周日期
- include_backtest: 包含回测分析

---

## 监控和告警

### 内置通知

所有工作流在执行完成后会发送通知:
- 成功: 发送执行摘要
- 失败: 发送错误信息

### 报告文件

工作流生成以下报告文件:

| 报告文件 | 生成工作流 | 说明 |
|---------|-----------|------|
| collection_report_YYYYMMDD.json | xcnstock_data_collection | 采集报告 |
| validation_report_YYYYMMDD.json | xcnstock_data_collection | GE验证报告 |
| backfill_report.json | xcnstock_data_collection | 补采报告 |
| daily_report_YYYYMMDD.json | xcnstock_daily_report | 选股报告 |
| review_pending_YYYYMMDD.json | xcnstock_daily_report | 待验证数据 |
| review_verified_YYYYMMDD.json | xcnstock_verify_picks | 已验证数据 |
| verification_result_YYYYMMDD.json | xcnstock_verify_picks | 验证结果 |
| strategy_performance.json | xcnstock_verify_picks | 绩效追踪 |

---

## 最佳实践

### 1. 补采操作

当需要补采历史数据时:
```bash
# 在 Kestra UI 中手动触发 xcnstock_data_collection
# 设置参数:
# - start_date: 2024-01-01
# - end_date: 2024-01-31
```

### 2. 故障排查

当工作流失败时:
1. 检查 Kestra UI 中的执行日志
2. 查看对应的报告文件
3. 使用 xcnstock_debug 工作流检查环境

### 3. 性能优化

- 大数据量补采建议分批进行
- 避免在交易时段执行重任务
- 利用 Kestra 的并行执行能力

---

## 附录

### A. 工作流文件位置

```
kestra/flows/
├── xcnstock_daily_report.yml          # 每日选股报告
├── xcnstock_daily_update.yml          # 每日数据更新(待合并)
├── xcnstock_data_collection.yml       # 统一数据采集
├── xcnstock_data_collection_with_ge.yml # GE验证采集(待合并)
├── xcnstock_data_inspection.yml       # 数据质检(待合并)
├── xcnstock_data_pipeline.yml         # 数据流水线(待合并)
├── xcnstock_debug.yml                 # 调试工作流
├── xcnstock_morning_report.yml        # 盘前涨停分析
├── xcnstock_smart_pipeline.yml        # 智能流水线(待合并)
├── xcnstock_verify_picks.yml          # 选股结果验证
└── xcnstock_weekly_review.yml         # 周度复盘
```

### B. 相关脚本位置

```
scripts/pipeline/
├── daily_data_update.py              # 每日数据更新
├── data_collect.py                   # 数据采集
├── data_collect_with_validation.py   # GE验证采集
├── smart_data_audit.py               # 智能数据审计
├── smart_data_backfill.py            # 智能补采
├── cvd_calculator.py                 # CVD指标计算
├── precompute.py                     # 指标预计算
├── stock_pick.py                     # 选股执行
├── stock_selection.py                # 选股评分
├── night_picks.py                    # 晚间选股报告
├── morning_limit_up.py               # 盘前涨停分析
├── daily_review.py                   # 每日复盘
├── market_review.py                  # 市场复盘
├── strategy_review.py                # 策略回顾
├── generate_weekly_report.py         # 周度报告
└── send_workflow_notification.py     # 通知发送
```

---

## 版本历史

| 版本 | 日期 | 变更说明 |
|------|------|---------|
| 1.0 | 2026-04-25 | 初始版本，创建优化后的工作流架构 |

---

## 维护者

- 创建: XCNStock Team
- 最后更新: 2026-04-25
