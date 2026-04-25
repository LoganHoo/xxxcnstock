# Kestra 工作流与脚本映射关系

本文档详细说明每个 Kestra 工作流对应的 Python 脚本及其功能。

## 📋 工作流列表

### 1. xcnstock_daily_update - 每日数据更新

**调度**: 每日 16:00

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/daily_data_update.py` | 每日数据更新主逻辑 | 主任务 |
| `scripts/pipeline/send_workflow_notification.py` | 发送工作流通知 | 成功/失败通知 |

**工作流功能**:
1. 设置目标日期（智能判断交易日）
2. 执行每日数据更新（股票列表+数据采集）
3. 发送执行结果通知

---

### 2. xcnstock_data_collection_with_ge - 带GE验证的数据采集

**调度**: 每日 16:30

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/data_collect_with_validation.py` | 带GE验证的数据采集 | 主任务 |
| `scripts/pipeline/send_workflow_notification.py` | 发送工作流通知 | 成功/失败通知 |

**工作流功能**:
1. 设置目标日期
2. 执行带GE验证的数据采集
3. 自动重试失败的采集（最多2次）
4. 发送采集结果通知

---

### 3. xcnstock_data_pipeline - 完整数据流水线

**调度**: 手动触发

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/data_collect.py` | 数据采集 | 数据采集任务 |
| `scripts/pipeline/smart_data_audit.py` | 智能数据审计 | 数据审计任务 |
| `scripts/pipeline/cvd_calculator.py` | CVD指标计算 | 指标计算任务 |
| `scripts/pipeline/market_review.py` | 市场回顾分析 | 市场回顾任务 |
| `scripts/pipeline/stock_selection.py` | 选股策略执行 | 选股任务 |
| `scripts/pipeline/send_workflow_notification.py` | 发送工作流通知 | 成功/失败通知 |

**工作流功能**:
1. 数据采集
2. 数据审计
3. CVD指标计算
4. 市场回顾
5. 选股策略
6. 发送通知

---

### 4. xcnstock_data_pipeline_simple - 简化数据流水线

**调度**: 手动触发

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/data_collect.py` | 数据采集 | 数据采集任务 |
| `scripts/pipeline/smart_data_audit.py` | 智能数据审计 | 数据审计任务 |
| `scripts/calculate_cvd.py` | CVD指标计算 | 指标计算任务 |
| `scripts/pipeline/market_review.py` | 市场回顾分析 | 市场回顾任务 |
| `scripts/pipeline/stock_screening.py` | 股票筛选 | 选股任务 |
| `scripts/pipeline/generate_report.py` | 生成报告 | 报告生成任务 |

**工作流功能**:
简化版完整流水线，适用于快速执行

---

### 5. xcnstock_smart_pipeline - 智能数据流水线

**调度**: 手动触发

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/data_collect.py` | 数据采集 | 数据采集任务 |

**工作流功能**:
1. 智能判断需要采集的日期
2. 并行执行数据采集
3. 支持增量和全量采集

---

### 6. xcnstock_data_inspection - 数据检查

**调度**: 手动触发

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/check_data_freshness.py` | 检查数据新鲜度 | 新鲜度检查任务 |
| `scripts/pipeline/check_data_completeness.py` | 检查数据完整性 | 完整性检查任务 |
| `scripts/pipeline/generate_inspection_report.py` | 生成检查报告 | 报告生成任务 |

**工作流功能**:
1. 检查数据新鲜度
2. 检查数据完整性
3. 生成检查报告

---

### 7. xcnstock_morning_report - 晨报生成

**调度**: 每日 08:30

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/morning_limit_up.py` | 涨停分析 | 涨停分析任务 |

**工作流功能**:
1. 生成每日晨报
2. 涨停股票分析

---

### 8. xcnstock_morning_report_simple - 简化晨报

**调度**: 每日 08:30

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/morning_limit_up.py` | 涨停分析 | 涨停分析任务 |

**工作流功能**:
简化版晨报生成

---

### 9. xcnstock_weekly_review - 周度回顾

**调度**: 每周一 09:00

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/daily_review.py` | 每日回顾 | 日回顾任务 |
| `scripts/pipeline/strategy_review.py` | 策略回顾 | 策略回顾任务 |
| `scripts/pipeline/generate_weekly_report.py` | 生成周报 | 周报生成任务 |

**工作流功能**:
1. 执行每日回顾
2. 执行策略回顾
3. 生成周度报告

---

### 10. xcnstock_debug - 调试工作流

**调度**: 手动触发

**对应脚本**:
| 脚本路径 | 功能 | 调用位置 |
|---------|------|---------|
| `scripts/pipeline/debug_check.py` | 调试检查 | 调试任务 |

**工作流功能**:
用于调试和测试的工作流

---

## 📊 脚本分类统计

### 数据采集类
- `scripts/pipeline/data_collect.py` - 基础数据采集
- `scripts/pipeline/data_collect_with_validation.py` - 带验证的数据采集
- `scripts/pipeline/daily_data_update.py` - 每日数据更新

### 数据审计类
- `scripts/pipeline/smart_data_audit.py` - 智能数据审计
- `scripts/pipeline/check_data_freshness.py` - 数据新鲜度检查
- `scripts/pipeline/check_data_completeness.py` - 数据完整性检查

### 指标计算类
- `scripts/pipeline/cvd_calculator.py` - CVD指标计算
- `scripts/calculate_cvd.py` - CVD计算（旧版）

### 分析类
- `scripts/pipeline/market_review.py` - 市场回顾
- `scripts/pipeline/stock_selection.py` - 选股策略
- `scripts/pipeline/stock_screening.py` - 股票筛选
- `scripts/pipeline/morning_limit_up.py` - 涨停分析
- `scripts/pipeline/daily_review.py` - 每日回顾
- `scripts/pipeline/strategy_review.py` - 策略回顾

### 报告类
- `scripts/pipeline/generate_report.py` - 生成报告
- `scripts/pipeline/generate_weekly_report.py` - 生成周报
- `scripts/pipeline/generate_inspection_report.py` - 生成检查报告

### 工具类
- `scripts/pipeline/send_workflow_notification.py` - 发送工作流通知
- `scripts/pipeline/debug_check.py` - 调试检查

---

## 🔗 依赖关系图

```
xcnstock_daily_update
├── daily_data_update.py
└── send_workflow_notification.py

xcnstock_data_collection_with_ge
├── data_collect_with_validation.py
└── send_workflow_notification.py

xcnstock_data_pipeline
├── data_collect.py
├── smart_data_audit.py
├── cvd_calculator.py
├── market_review.py
├── stock_selection.py
└── send_workflow_notification.py

xcnstock_data_inspection
├── check_data_freshness.py
├── check_data_completeness.py
└── generate_inspection_report.py

xcnstock_weekly_review
├── daily_review.py
├── strategy_review.py
└── generate_weekly_report.py
```

---

## 📝 使用说明

### 查看工作流使用的脚本
```bash
# 查看特定工作流使用的脚本
grep -n "from scripts\|import scripts" kestra/flows/xcnstock_daily_update.yml
```

### 验证脚本存在性
```bash
# 验证所有工作流引用的脚本是否存在
python kestra/test_workflows.py --validate-flows
```

### 执行特定工作流
```bash
# 执行工作流
python kestra/execute_flow.py --flow xcnstock_daily_update
```
