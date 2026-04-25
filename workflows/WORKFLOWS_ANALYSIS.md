# Workflows 目录分析

## 📁 文件清单

| 文件 | 类型 | 状态 | 说明 |
|:---|:---:|:---:|:---|
| `__init__.py` | 初始化 | ✅ | 包初始化文件 |
| `data_collection_workflow.py` | 基础工作流 | ✅ | 数据采集工作流 |
| `stock_selection_workflow.py` | 基础工作流 | ✅ | 选股工作流 |
| `backtest_workflow.py` | 基础工作流 | ✅ | 回测工作流 |
| `daily_operation_workflow.py` | 基础工作流 | ✅ | 日常运营工作流 |
| `enhanced_data_collection_workflow.py` | 增强工作流 | ✅ | 增强版数据采集（含GE检查点） |
| `enhanced_scoring_workflow.py` | 增强工作流 | ✅ | 增强版评分计算（含GE检查点） |
| `enhanced_selection_workflow.py` | 增强工作流 | ✅ | 增强版选股（含SQLite存储+邮件） |
| `enhanced_workflow_scheduler.py` | 调度器 | ✅ | 统一流水线调度器 |
| `workflow_runner.py` | 运行器 | ✅ | 统一工作流运行入口 |
| `real_stock_selection_workflow.py` | 实盘工作流 | ⚠️ | 实盘选股工作流 |
| `datahub_lineage_workflow.py` | DataHub | ⚠️ | DataHub血缘追踪工作流 |

---

## 🏗️ 架构层次

```
┌─────────────────────────────────────────────────────────────┐
│                    统一入口层                                │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │ workflow_runner │  │ enhanced_sched  │                   │
│  │   (命令行入口)   │  │  (流水线调度)    │                   │
│  └─────────────────┘  └─────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    增强工作流层 (推荐)                        │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ enhanced_data │ │ enhanced_    │ │ enhanced_    │        │
│  │ _collection  │ │ scoring      │ │ selection    │        │
│  │  数据采集     │ │  评分计算     │ │  选股         │        │
│  └──────────────┘ └──────────────┘ └──────────────┘        │
│         │                │                │                 │
│         └────────────────┼────────────────┘                 │
│                          │                                  │
│                   ┌──────┴──────┐                          │
│                   │ 依赖检查     │                          │
│                   │ 自动重试     │                          │
│                   │ 断点续传     │                          │
│                   │ GE数据质量   │                          │
│                   └─────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    基础工作流层 (旧版)                        │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ data_coll    │ │ stock_sel    │ │ backtest     │        │
│  │ ection       │ │ ection       │ │              │        │
│  └──────────────┘ └──────────────┘ └──────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔧 工作流对比

### 基础工作流 vs 增强工作流

| 特性 | 基础工作流 | 增强工作流 |
|:---|:---:|:---:|
| 依赖检查 | ❌ | ✅ |
| 自动重试 | ❌ | ✅ |
| 断点续传 | ❌ | ✅ |
| GE数据质量 | ❌ | ✅ |
| 自动修复 | ❌ | ✅ |
| 统一报告 | ❌ | ✅ |
| SQLite存储 | ❌ | ✅ |
| 邮件通知 | ❌ | ✅ |

### 推荐使用

```python
# ✅ 推荐：增强版选股工作流
from workflows.enhanced_selection_workflow import run_selection
result = run_selection(date="2026-04-23", top_n=20)

# ❌ 不推荐：基础选股工作流
from workflows.stock_selection_workflow import StockSelectionWorkflow
workflow = StockSelectionWorkflow()
result = workflow.run()
```

---

## 📋 各工作流详情

### 1. enhanced_selection_workflow.py ⭐推荐

**功能**:
- 基于WorkflowExecutor框架
- 6个GE数据质量检查点
- SQLite本地存储
- 邮件报告发送
- 依赖检查和自动重试

**使用**:
```bash
python workflows/enhanced_selection_workflow.py --top-n 20
```

**输出**:
- `data/selection_results/selection_*.parquet`
- `data/selection_results/selection_*.csv`
- `data/selection_results/report_*.md`
- `data/selection_report.db` (SQLite)
- 邮件发送到 287363@qq.com

---

### 2. enhanced_data_collection_workflow.py ⭐推荐

**功能**:
- K线数据采集
- GE数据质量验证
- 断点续传
- 自动重试

**使用**:
```bash
python workflows/enhanced_data_collection_workflow.py --date 2026-04-23
```

---

### 3. enhanced_scoring_workflow.py ⭐推荐

**功能**:
- 股票评分计算
- GE数据质量验证
- 多维度评分

**使用**:
```bash
python workflows/enhanced_scoring_workflow.py --date 2026-04-23
```

---

### 4. enhanced_workflow_scheduler.py ⭐推荐

**功能**:
- 统一调度三个增强工作流
- 流水线执行
- 统一报告

**使用**:
```bash
python workflows/enhanced_workflow_scheduler.py --pipeline full
```

---

### 5. workflow_runner.py

**功能**:
- 命令行统一入口
- 支持所有基础工作流

**使用**:
```bash
python workflows/workflow_runner.py --workflow data_collection
```

---

### 6. daily_operation_workflow.py

**功能**:
- 日常运营任务
- 数据质量检查
- 系统健康检查

---

### 7. real_stock_selection_workflow.py ⚠️

**状态**: 实盘工作流，需要额外配置

---

### 8. datahub_lineage_workflow.py ⚠️

**状态**: DataHub集成，需要额外配置

---

## 🗑️ 可清理文件

以下文件可能已过时或重复:

| 文件 | 建议 | 原因 |
|:---|:---:|:---|
| `data_collection_workflow.py` | 保留 | 基础版本，可能被引用 |
| `stock_selection_workflow.py` | 保留 | 基础版本，可能被引用 |
| `backtest_workflow.py` | 保留 | 回测功能 |
| `real_stock_selection_workflow.py` | 检查 | 是否与enhanced_selection重复 |
| `datahub_lineage_workflow.py` | 检查 | DataHub是否还在使用 |

---

## 📊 依赖关系

```
enhanced_workflow_scheduler
    ├── enhanced_data_collection_workflow
    │       └── GECheckpointValidators
    ├── enhanced_scoring_workflow
    │       └── GECheckpointValidators
    └── enhanced_selection_workflow
            ├── GECheckpointValidators
            └── SelectionReportService (SQLite)

workflow_runner
    ├── data_collection_workflow
    ├── stock_selection_workflow
    ├── backtest_workflow
    └── daily_operation_workflow
```

---

## 🎯 使用建议

### 日常使用
```bash
# 完整流水线（推荐）
python workflows/enhanced_workflow_scheduler.py --pipeline full

# 单独运行选股
python workflows/enhanced_selection_workflow.py --top-n 20
```

### 开发测试
```bash
# 测试数据质量检查点
python scripts/test_ge_checkpoints.py

# 检查数据新鲜度
python scripts/check_data_freshness.py
```

---

## 📈 状态总结

| 类别 | 数量 | 状态 |
|:---|:---:|:---|
| 增强工作流 | 4 | ✅ 推荐使用 |
| 基础工作流 | 4 | ⚠️ 维护模式 |
| 调度/运行器 | 2 | ✅ 活跃 |
| 特殊工作流 | 2 | ⚠️ 需检查 |
| **总计** | **12** | **8活跃 + 4需检查** |
