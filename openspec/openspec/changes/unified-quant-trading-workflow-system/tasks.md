# 统一量化交易工作流管理系统 - 任务清单

## 概览

| 属性 | 值 |
|-----|-----|
| 变更ID | unified-quant-trading-workflow-system |
| 总任务数 | 28 |
| 预计总工期 | 6个工作日 |
| 优先级 | P0 - 核心业务功能 |

---

## 阶段1: 基础框架 (1天)

### 任务 1.1: 创建工作流执行器基类
**优先级**: 🔴 最高  
**预计耗时**: 2小时  
**依赖**: 无

**描述**: 实现 WorkflowExecutor 抽象基类，为所有工作流提供统一接口

**DoD**:
- [ ] 创建 `core/workflow_framework.py` 中的 `WorkflowExecutor` 基类
- [ ] 实现依赖检查接口 `check_dependencies()`
- [ ] 实现执行接口 `execute()`
- [ ] 实现模板方法 `run()`（依赖检查→执行→保存状态）
- [ ] 实现自动修复接口 `auto_fix_dependency()`
- [ ] 实现 GE 检查点集成
- [ ] 编写单元测试（测试覆盖率>80%）

**输出文件**:
- `core/workflow_framework.py` (更新)
- `tests/test_workflow_framework.py`

---

### 任务 1.2: 实现工作流状态管理
**优先级**: 🔴 最高  
**预计耗时**: 2小时  
**依赖**: 1.1

**描述**: 实现 SQLite 状态存储，记录工作流执行历史

**DoD**:
- [ ] 创建 `core/workflow_state_db.py`
- [ ] 实现 `WorkflowStateDB` 类
- [ ] 创建工作流执行记录表 `workflow_executions`
- [ ] 创建工作流检查点表 `workflow_checkpoints`
- [ ] 实现状态查询接口 `get_workflow_status()`
- [ ] 实现状态更新接口 `update_workflow_status()`
- [ ] 编写单元测试

**输出文件**:
- `core/workflow_state_db.py`
- `tests/test_workflow_state_db.py`

---

### 任务 1.3: 实现工作流调度器
**优先级**: 🔴 最高  
**预计耗时**: 2小时  
**依赖**: 1.2

**描述**: 实现工作流调度器，支持优先级和依赖管理

**DoD**:
- [x] 创建 `core/workflow_scheduler.py`
- [x] 实现 `WorkflowScheduler` 类
- [x] 实现工作流注册 `register_workflow()`
- [x] 实现依赖检查 `check_dependencies()`
- [x] 实现优先级调度 `schedule_by_priority()`
- [x] 实现重试机制 `retry_failed_workflow()`
- [x] 编写单元测试（16个测试用例全部通过）

**输出文件**:
- `core/workflow_scheduler.py`
- `tests/test_workflow_scheduler.py`

---

### 任务 1.4: 实现工作流监控面板
**优先级**: 🟡 中  
**预计耗时**: 2小时  
**依赖**: 1.2

**描述**: 实现 Web 监控面板，实时显示工作流状态

**DoD**:
- [ ] 创建 `scripts/monitoring/workflow_dashboard.py`
- [ ] 实现 `WorkflowDashboard` 类
- [ ] 实现状态聚合 `get_workflow_summary()`
- [ ] 实现 HTML 报告生成 `generate_html_report()`
- [ ] 实现定时刷新（每5分钟）
- [ ] 集成到现有监控面板

**输出文件**:
- `scripts/monitoring/workflow_dashboard.py`
- `templates/workflow_dashboard.html`

---

## 阶段2: 盘后流程 (2天)

### 任务 2.1: 实现统一数据采集工作流
**优先级**: 🔴 最高  
**预计耗时**: 4小时  
**依赖**: 1.1, 1.2, 1.3

**描述**: 实现7个数据源的统一采集工作流

**DoD**:
- [ ] 创建 `workflows/unified_data_collection_workflow.py`
- [ ] 实现 `UnifiedDataCollectionWorkflow` 类
- [ ] 实现股票列表采集 `collect_stock_list()`
- [ ] 实现个股K线采集 `collect_kline_data()`
- [ ] 实现基本面采集 `collect_fundamental_data()`
- [ ] 实现CCTV新闻采集 `collect_cctv_news()`
- [ ] 实现大盘指数采集 `collect_index_data()`
- [ ] 实现外盘指数采集 `collect_foreign_index()`
- [ ] 实现大宗商品采集 `collect_commodities()`
- [ ] 实现 GE 检查点（3个）
- [ ] 实现失败重试（每个采集3次）
- [ ] 编写单元测试（6个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置

**输出文件**:
- `workflows/unified_data_collection_workflow.py`
- `tests/test_unified_data_collection_workflow.py`

---

### 任务 2.2: 实现智能数据质检系统
**优先级**: 🔴 最高  
**预计耗时**: 4小时  
**依赖**: 2.1

**描述**: 实现数据质量检查和自动修复系统

**DoD**:
- [ ] 创建 `workflows/data_quality_inspector.py`
- [ ] 实现 `DataQualityInspector` 类
- [ ] 实现新鲜度检查 `check_freshness()`
- [ ] 实现完整性检查 `check_completeness()`
- [ ] 实现准确性检查 `check_accuracy()`
- [ ] 实现一致性检查 `check_consistency()`
- [ ] 实现自动修复策略 `AutoFixStrategy`
- [ ] 实现质检报告生成 `generate_quality_report()`
- [ ] 实现告警通知（企业微信）
- [ ] 编写单元测试（4个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置

**输出文件**:
- `workflows/data_quality_inspector.py`
- `workflows/auto_fix_strategy.py`
- `tests/test_data_quality_inspector.py`

---

### 任务 2.3: 实现数据清洗流水线
**优先级**: 🟠 高  
**预计耗时**: 3小时  
**依赖**: 2.2

**描述**: 实现数据清洗和标准化流水线

**DoD**:
- [ ] 创建 `workflows/data_cleaning_pipeline.py`
- [ ] 实现 `DataCleaningPipeline` 类
- [ ] 实现去重处理 `remove_duplicates()`
- [ ] 实现标准化处理 `standardize_format()`
- [ ] 实现缺失值填充 `fill_missing_values()`
- [ ] 实现异常值处理 `handle_outliers()`
- [ ] 实现 Polars 优化转换 `optimize_with_polars()`
- [ ] 创建清洗规则配置 `config/cleaning_rules.yaml`
- [ ] 实现清洗统计报告
- [ ] 编写单元测试（3个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置

**输出文件**:
- `workflows/data_cleaning_pipeline.py`
- `config/cleaning_rules.yaml`
- `tests/test_data_cleaning_pipeline.py`

---

### 任务 2.4: 实现复盘分析系统
**优先级**: 🟠 高  
**预计耗时**: 4小时  
**依赖**: 2.3

**描述**: 实现昨日选股验证和大盘分析

**DoD**:
- [ ] 创建 `workflows/review_analysis_system.py`
- [ ] 实现 `ReviewAnalysisSystem` 类
- [ ] 实现昨日选股验证 `verify_yesterday_picks()`
- [ ] 实现持仓状态更新 `update_positions()`
- [ ] 实现大盘预测验证 `verify_market_prediction()`
- [ ] 实现热门板块分析 `analyze_hot_sectors()`
- [ ] 实现龙虎榜解析 `parse_dragon_tiger_list()`
- [ ] 实现复盘报告生成 `generate_review_report()`
- [ ] 实现持仓跟踪更新（SQLite）
- [ ] 编写单元测试（4个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置

**输出文件**:
- `workflows/review_analysis_system.py`
- `tests/test_review_analysis_system.py`

---

### 任务 2.5: 实现多因子选股评分引擎
**优先级**: 🔴 最高  
**预计耗时**: 4小时  
**依赖**: 2.4

**描述**: 实现技术/基本面/资金/情绪多因子评分

**DoD**:
- [ ] 创建 `workflows/multi_factor_scoring_engine.py`
- [ ] 实现 `MultiFactorScoringEngine` 类
- [ ] 实现技术因子计算 `calculate_technical_factors()`
  - MACD信号
  - RSI水平
  - KDJ金叉
  - 均线排列
- [ ] 实现基本面因子计算 `calculate_fundamental_factors()`
  - PE/PB分位数
  - ROE增长
  - 营收增长
- [ ] 实现资金因子计算 `calculate_fund_flow_factors()`
  - 主力净流入
  - 换手率
- [ ] 实现情绪因子计算 `calculate_sentiment_factors()`
  - CCTV情绪
  - 板块热度
- [ ] 实现综合评分计算 `calculate_total_score()`
- [ ] 实现因子报告生成
- [ ] 编写单元测试（5个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置

**输出文件**:
- `workflows/multi_factor_scoring_engine.py`
- `workflows/factors/` (因子模块目录)
- `tests/test_multi_factor_scoring_engine.py`

---

## 阶段3: 涨停板系统 (1天)

### 任务 3.1: 实现涨停板扫描引擎
**优先级**: 🔴 最高  
**预计耗时**: 3小时  
**依赖**: 1.1

**描述**: 实现09:25集合竞价涨停股票筛选

**DoD**:
- [ ] 创建 `workflows/limit_up_scanner.py`
- [ ] 实现 `LimitUpScanner` 类
- [ ] 实现集合竞价数据采集 `collect_auction_data()`
- [ ] 实现涨停股票筛选 `scan_limit_up_stocks()`
  - 主板涨停（>= 9.9%）
  - ST涨停（>= 4.9%）
  - 科创板/创业板涨停（>= 19.9%）
- [ ] 实现封单金额计算 `calculate_seal_amount()`
- [ ] 实现流通市值获取 `get_float_market_cap()`
- [ ] 输出涨停候选股票列表
- [ ] 编写单元测试（4个测试用例）

**输出文件**:
- `workflows/limit_up_scanner.py`
- `tests/test_limit_up_scanner.py`

---

### 任务 3.2: 实现开板概率预测模型
**优先级**: 🔴 最高  
**预计耗时**: 4小时  
**依赖**: 3.1

**描述**: 实现涨停板开板概率预测算法

**DoD**:
- [ ] 创建 `workflows/limit_up_predictor.py`
- [ ] 实现 `LimitUpPredictor` 类
- [ ] 实现封单比计算 `calculate_seal_ratio()` (权重40%)
- [ ] 实现历史开板率查询 `get_history_open_rate()` (权重30%)
- [ ] 实现板块热度获取 `get_sector_heat()` (权重20%)
- [ ] 实现大盘情绪获取 `get_market_sentiment()` (权重10%)
- [ ] 实现开板概率计算 `predict_open_probability()`
- [ ] 实现强度分级 `get_grade()` (S/A/B/C)
- [ ] 实现操作建议 `get_suggestion()`
- [ ] 编写单元测试（6个测试用例）

**输出文件**:
- `workflows/limit_up_predictor.py`
- `tests/test_limit_up_predictor.py`

---

### 任务 3.3: 实现涨停板分析工作流
**优先级**: 🔴 最高  
**预计耗时**: 3小时  
**依赖**: 3.2

**描述**: 整合扫描+预测，实现完整涨停板分析工作流

**DoD**:
- [ ] 创建 `workflows/limit_up_analysis_system.py`
- [ ] 实现 `LimitUpAnalysisSystem` 类（继承 WorkflowExecutor）
- [ ] 实现依赖检查（09:15-09:25时间窗口）
- [ ] 集成扫描引擎 `LimitUpScanner`
- [ ] 集成预测引擎 `LimitUpPredictor`
- [ ] 实现 GE 检查点（扫描结果非空、预测完整）
- [ ] 实现涨停板报告生成 `generate_limit_up_report()`
  - 涨停股票列表
  - 开板概率
  - 强度分级
  - 操作建议
- [ ] 实现与 `fund_behavior_report` 集成
- [ ] 编写单元测试（3个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置（09:26触发）

**输出文件**:
- `workflows/limit_up_analysis_system.py`
- `tests/test_limit_up_analysis_system.py`

---

## 阶段4: 盘中监控 (1天)

### 任务 4.1: 实现热点板块监控
**优先级**: 🟠 高  
**预计耗时**: 3小时  
**依赖**: 1.1

**描述**: 实现实时热点板块监控

**DoD**:
- [ ] 创建 `workflows/intraday_sector_monitor.py`
- [ ] 实现 `IntradaySectorMonitor` 类
- [ ] 实现板块涨幅计算 `calculate_sector_change()`
- [ ] 实现板块资金流向 `calculate_sector_fund_flow()`
- [ ] 实现热点板块排名 `get_hot_sectors()` (Top 10)
- [ ] 实现板块异动检测 `detect_sector_anomaly()`
- [ ] 编写单元测试（3个测试用例）

**输出文件**:
- `workflows/intraday_sector_monitor.py`
- `tests/test_intraday_sector_monitor.py`

---

### 任务 4.2: 实现持仓监控与信号生成
**优先级**: 🟠 高  
**预计耗时**: 4小时  
**依赖**: 4.1

**描述**: 实现持仓股监控和交易信号生成

**DoD**:
- [ ] 创建 `workflows/intraday_position_monitor.py`
- [ ] 实现 `IntradayPositionMonitor` 类
- [ ] 实现持仓数据加载 `load_positions()`
- [ ] 实现止盈检测 `check_take_profit()`
- [ ] 实现止损检测 `check_stop_loss()`
- [ ] 实现涨停开板检测 `check_limit_up_open()`
- [ ] 实现建仓信号生成 `generate_open_signal()`
- [ ] 实现加仓信号生成 `generate_add_signal()`
- [ ] 实现减仓信号生成 `generate_reduce_signal()`
- [ ] 实现平仓信号生成 `generate_close_signal()`
- [ ] 编写单元测试（4个测试用例）

**输出文件**:
- `workflows/intraday_position_monitor.py`
- `tests/test_intraday_position_monitor.py`

---

### 任务 4.3: 实现盘中实时监控系统
**优先级**: 🔴 最高  
**预计耗时**: 3小时  
**依赖**: 4.2

**描述**: 整合监控模块，实现完整盘中监控工作流

**DoD**:
- [ ] 创建 `workflows/intraday_monitor_system.py`
- [ ] 实现 `IntradayMonitorSystem` 类（继承 WorkflowExecutor）
- [ ] 集成热点板块监控 `IntradaySectorMonitor`
- [ ] 集成持仓监控 `IntradayPositionMonitor`
- [ ] 实现信号聚合 `aggregate_signals()`
- [ ] 实现信号优先级排序
- [ ] 实现实时推送（企业微信）`send_realtime_notification()`
- [ ] 实现信号日志记录 `log_trading_signals()`
- [ ] 实现交易时段控制（09:30-11:30, 13:00-15:00）
- [ ] 编写单元测试（3个测试用例）
- [ ] 更新 `config/cron_tasks.yaml` 配置（每5分钟循环）

**输出文件**:
- `workflows/intraday_monitor_system.py`
- `tests/test_intraday_monitor_system.py`

---

## 阶段5: 集成测试 (1天)

### 任务 5.1: 实现工作流集成测试
**优先级**: 🟠 高  
**预计耗时**: 4小时  
**依赖**: 2.1-2.5, 3.3, 4.3

**描述**: 实现端到端集成测试

**DoD**:
- [ ] 创建 `tests/integration/test_workflow_pipeline.py`
- [ ] 实现盘后流程端到端测试
- [ ] 实现涨停板系统端到端测试
- [ ] 实现盘中监控端到端测试
- [ ] 实现故障注入测试
- [ ] 实现性能基准测试
- [ ] 所有测试通过率100%

**输出文件**:
- `tests/integration/test_workflow_pipeline.py`

---

### 任务 5.2: 实现工作流统一入口
**优先级**: 🟡 中  
**预计耗时**: 2小时  
**依赖**: 5.1

**描述**: 实现统一的工作流运行入口

**DoD**:
- [ ] 更新 `workflows/workflow_runner.py`
- [ ] 注册所有7个工作流
- [ ] 实现命令行接口 `python -m workflows run <workflow_name>`
- [ ] 实现工作流状态查询 `python -m workflows status`
- [ ] 实现工作流重试 `python -m workflows retry <execution_id>`
- [ ] 编写使用文档

**输出文件**:
- `workflows/workflow_runner.py` (更新)
- `docs/workflow_usage.md`

---

### 任务 5.3: 性能优化与调优
**优先级**: 🟡 中  
**预计耗时**: 2小时  
**依赖**: 5.2

**描述**: 性能优化，确保满足指标要求

**DoD**:
- [ ] 数据采集时间 < 60分钟
- [ ] 选股评分时间 < 15分钟
- [ ] 涨停板分析时间 < 3分钟
- [ ] 系统吞吐量 > 1000只/秒
- [ ] 生成性能测试报告

**输出文件**:
- `reports/performance_test_report.md`

---

## 任务依赖图

```
阶段1: 基础框架
├── 1.1 工作流执行器基类
├── 1.2 工作流状态管理 (依赖1.1)
├── 1.3 工作流调度器 (依赖1.2)
└── 1.4 监控面板 (依赖1.2)

阶段2: 盘后流程
├── 2.1 统一数据采集 (依赖1.1,1.2,1.3)
├── 2.2 数据质检 (依赖2.1)
├── 2.3 数据清洗 (依赖2.2)
├── 2.4 复盘分析 (依赖2.3)
└── 2.5 选股评分 (依赖2.4)

阶段3: 涨停板系统
├── 3.1 涨停板扫描 (依赖1.1)
├── 3.2 开板预测 (依赖3.1)
└── 3.3 涨停板工作流 (依赖3.2)

阶段4: 盘中监控
├── 4.1 热点监控 (依赖1.1)
├── 4.2 持仓监控 (依赖4.1)
└── 4.3 盘中系统 (依赖4.2)

阶段5: 集成测试
├── 5.1 集成测试 (依赖阶段2,3,4)
├── 5.2 统一入口 (依赖5.1)
└── 5.3 性能优化 (依赖5.2)
```

---

## 执行计划

| 阶段 | 工期 | 任务数 | 关键路径 |
|-----|------|--------|---------|
| 阶段1 | 1天 | 4 | 1.1 → 1.2 → 1.3 |
| 阶段2 | 2天 | 5 | 2.1 → 2.2 → 2.3 → 2.4 → 2.5 |
| 阶段3 | 1天 | 3 | 3.1 → 3.2 → 3.3 |
| 阶段4 | 1天 | 3 | 4.1 → 4.2 → 4.3 |
| 阶段5 | 1天 | 3 | 5.1 → 5.2 → 5.3 |

---

## 风险与缓解

| 风险 | 影响任务 | 缓解措施 |
|-----|---------|---------|
| 09:26性能不达标 | 3.3 | 提前压力测试，准备简化版预测模型 |
| API限流 | 2.1, 4.3 | 实现请求队列，控制并发数 |
| 数据质量问题 | 2.2 | 配置化阈值，支持人工覆盖 |

---

## 下一步

1. 审批任务清单
2. 开始执行任务 1.1
3. 使用 `/opsx:apply` 开始实施
