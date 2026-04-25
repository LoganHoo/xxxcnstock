# XCNStock 项目架构分析

## 一、项目整体结构

```
xcnstock/
├── config/                 # 配置文件
│   ├── factors/           # 因子配置
│   ├── strategies/        # 策略配置
│   ├── cron_tasks.yaml    # 定时任务配置
│   └── datasource.yaml    # 数据源配置
│
├── core/                   # 核心模块
│   ├── indicators/        # 技术指标
│   ├── storage/           # 存储工具
│   ├── cache/             # 缓存管理
│   ├── monitoring/        # 监控指标
│   └── notification/      # 通知系统
│
├── services/              # 业务服务层
│   ├── data_service/      # 数据服务
│   ├── stock_service/     # 股票服务
│   ├── analysis_service/  # 分析服务
│   ├── risk_service/      # 风控服务
│   ├── notify_service/    # 通知服务
│   ├── backtest_service/  # 回测服务
│   ├── scheduler/         # 调度服务
│   └── strategy_service/  # 策略服务
│
├── workflows/             # 工作流
│   ├── enhanced_data_collection_workflow.py
│   ├── enhanced_selection_workflow.py
│   ├── enhanced_scoring_workflow.py
│   └── quant_trading_system_v2.py
│
├── filters/               # 过滤器
├── models/                # 数据模型
├── patterns/              # 形态识别
├── tests/                 # 测试
├── scripts/               # 脚本
└── data/                  # 数据存储
```

## 二、数据流分析

### 2.1 主数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              数据流向图                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   数据源     │────▶│  数据服务层  │────▶│   工作流层   │                │
│  │              │     │              │     │              │                │
│  │ • Baostock   │     │ • 采集       │     │ • 采集工作流 │                │
│  │ • Tencent    │     │ • 清洗       │     │ • 评分工作流 │                │
│  │ • AKShare    │     │ • 存储       │     │ • 选股工作流 │                │
│  │ • Tushare    │     │ • 质量检查   │     │ • 交易工作流 │                │
│  └──────────────┘     └──────────────┘     └──────┬───────┘                │
│                                                   │                         │
│                                                   ▼                         │
│                                          ┌──────────────┐                  │
│                                          │   业务输出   │                  │
│                                          │              │                  │
│                                          │ • 选股报告   │                  │
│                                          │ • 交易信号   │                  │
│                                          │ • 风险预警   │                  │
│                                          │ • 数据报告   │                  │
│                                          └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 详细数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           详细数据流向                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 数据采集流程                                                            │
│  ════════════════                                                           │
│                                                                             │
│  数据源 ──▶ 数据服务层                                                      │
│     │         │                                                             │
│     │         ├─▶ providers.py (Baostock/Tencent/AKShare)                  │
│     │         ├─▶ manager.py (数据源管理器)                                 │
│     │         ├─▶ fetchers/ (各类数据获取器)                                │
│     │         └─▶ quality/ (数据质量检查)                                   │
│     │                    ├─▶ ge_checkpoint_validators.py (GE验证)          │
│     │                    └─▶ checkpoint_validators.py (检查点验证)          │
│     │                                                                       │
│     └────────▶ storage/ (存储层)                                            │
│                  ├─▶ parquet_manager.py (Parquet管理)                      │
│                  ├─▶ enhanced_storage.py (增强存储)                        │
│                  └─▶ data/kline/ (数据文件)                                │
│                                                                             │
│  2. 工作流流程                                                              │
│  ═══════════════                                                            │
│                                                                             │
│  enhanced_data_collection_workflow.py                                       │
│     │                                                                       │
│     ├─▶ 升级检测和修复                                                      │
│     ├─▶ GE检查点1: 采集前检查 (pre_collection_check)                        │
│     ├─▶ 数据采集 (异步并发)                                                 │
│     │         ├─▶ _collect_single_stock_async (单股采集)                   │
│     │         ├─▶ _collect_batch_async (批量采集)                          │
│     │         └─▶ Rich进度条监控                                           │
│     ├─▶ 断点续传 (Checkpoint)                                              │
│     └─▶ GE检查点2: 采集后验证 (post_collection_validation)                  │
│                                                                             │
│  enhanced_scoring_workflow.py                                               │
│     │                                                                       │
│     ├─▶ 加载K线数据                                                         │
│     ├─▶ 计算技术指标                                                        │
│     │         └─▶ core/indicators/technical.py                             │
│     ├─▶ 计算市场因子                                                        │
│     │         └─▶ core/factor_engine.py                                    │
│     ├─▶ 多因子评分                                                          │
│     └─▶ 保存评分结果                                                        │
│                                                                             │
│  enhanced_selection_workflow.py                                             │
│     │                                                                       │
│     ├─▶ 加载评分数据                                                        │
│     ├─▶ 应用过滤器                                                          │
│     │         └─▶ filters/ (各类过滤器)                                    │
│     ├─▶ 选股策略                                                            │
│     │         └─▶ services/strategy_service/                               │
│     └─▶ 生成选股报告                                                        │
│                                                                             │
│  3. 服务层交互                                                              │
│  ═══════════════                                                            │
│                                                                             │
│  services/                                                                  │
│     │                                                                       │
│     ├─▶ data_service/ (数据服务)                                            │
│     │         ├─▶ unified_data_service.py (统一数据服务)                   │
│     │         ├─▶ historical_data_loader.py (历史数据加载)                 │
│     │         └─▶ realtime/market_data_stream.py (实时数据流)              │
│     │                                                                       │
│     ├─▶ stock_service/ (股票服务)                                           │
│     │         ├─▶ engine.py (选股引擎)                                     │
│     │         ├─▶ scorer.py (评分器)                                       │
│     │         └─▶ filters/ (过滤器)                                        │
│     │                                                                       │
│     ├─▶ analysis_service/ (分析服务)                                        │
│     │         ├─▶ sentiment/ (情感分析)                                    │
│     │         ├─▶ fundamental/ (基本面分析)                                │
│     │         └─▶ macro/ (宏观分析)                                        │
│     │                                                                       │
│     ├─▶ risk_service/ (风控服务)                                            │
│     │         ├─▶ circuit_breaker/ (熔断管理)                              │
│     │         ├─▶ stoploss/ (止损管理)                                     │
│     │         └─▶ position/ (仓位管理)                                     │
│     │                                                                       │
│     ├─▶ notify_service/ (通知服务)                                          │
│     │         ├─▶ channels/ (通知渠道)                                     │
│     │         └─▶ templates/ (报告模板)                                    │
│     │                                                                       │
│     └─▶ backtest_service/ (回测服务)                                        │
│               ├─▶ engine/ (回测引擎)                                       │
│               └─▶ optimization/ (优化器)                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 三、代码入口和出口

### 3.1 主要入口

| 入口文件 | 用途 | 调用方式 |
|---------|------|---------|
| `workflows/enhanced_data_collection_workflow.py` | 数据采集工作流 | `python -m workflows.enhanced_data_collection_workflow` |
| `workflows/enhanced_scoring_workflow.py` | 评分工作流 | `python -m workflows.enhanced_scoring_workflow` |
| `workflows/enhanced_selection_workflow.py` | 选股工作流 | `python -m workflows.enhanced_selection_workflow` |
| `workflows/quant_trading_system_v2.py` | 量化交易系统V2 | `python -m workflows.quant_trading_system_v2` |
| `services/data_service/main.py` | 数据服务主程序 | `python -m services.data_service.main` |
| `scripts/scheduler.py` | 定时任务调度器 | `python scripts/scheduler.py` |

### 3.2 主要出口

| 出口 | 类型 | 位置 |
|-----|------|------|
| K线数据 | Parquet文件 | `data/kline/*.parquet` |
| 评分数据 | Parquet文件 | `data/scores/*.parquet` |
| 选股结果 | CSV/数据库 | `data/selections/` |
| 报告 | HTML/PDF/邮件 | `services/notify_service/templates/` |
| 日志 | 文本文件 | `logs/` |
| 断点 | JSON文件 | `data/checkpoints/` |

## 四、流水线 vs 并行策略

### 4.1 当前架构：混合模式

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           执行策略分析                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 流水线 (Pipeline) - 工作流之间                                          │
│  ═══════════════════════                                                    │
│                                                                             │
│  数据采集工作流 ──▶ 评分工作流 ──▶ 选股工作流 ──▶ 交易工作流               │
│       │                │              │              │                      │
│       ▼                ▼              ▼              ▼                      │
│    依赖: 无        依赖: 数据        依赖: 评分    依赖: 选股               │
│                                                                             │
│  特点：                                                                       │
│  • 工作流之间有明确的依赖关系                                                │
│  • 上游工作流完成后才能启动下游                                              │
│  • 通过GE检查点保证数据质量                                                  │
│                                                                             │
│  2. 并行 (Parallel) - 工作流内部                                            │
│  ═══════════════════════                                                    │
│                                                                             │
│  enhanced_data_collection_workflow.py                                       │
│     │                                                                       │
│     ├─▶ 异步采集 (_collect_single_stock_async)                              │
│     │         │                                                             │
│     │         ├─▶ 股票1 (并发)                                              │
│     │         ├─▶ 股票2 (并发)                                              │
│     │         ├─▶ 股票3 (并发)                                              │
│     │         └─▶ ...                                                       │
│     │                                                                       │
│     └─▶ 信号量控制 (Semaphore: max_workers=10)                              │
│                                                                             │
│  特点：                                                                       │
│  • 单股采集使用asyncio并发                                                   │
│  • 通过信号量控制并发数                                                      │
│  • Rich进度条实时监控                                                        │
│                                                                             │
│  3. 定时触发 (Scheduled) - 整体调度                                         │
│  ════════════════════════════                                               │
│                                                                             │
│  cron_tasks.yaml                                                            │
│     │                                                                       │
│     ├─▶ 16:00 数据采集                                                      │
│     ├─▶ 16:50 数据审计                                                      │
│     ├─▶ 17:30 复盘分析                                                      │
│     ├─▶ 20:00 预计算                                                        │
│     └─▶ 20:35 晚间分析                                                      │
│                                                                             │
│  特点：                                                                       │
│  • 按时间顺序触发                                                            │
│  • 依赖检查确保前置任务完成                                                  │
│  • 失败重试和断点续传                                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 执行策略建议

| 场景 | 推荐策略 | 原因 |
|-----|---------|------|
| 数据采集 | **并行** | IO密集型，网络等待时间长 |
| 数据计算 | **并行** | CPU密集型，可充分利用多核 |
| 工作流之间 | **流水线** | 有依赖关系，需要数据传递 |
| 定时任务 | **流水线** | 按时间顺序执行，保证数据新鲜度 |
| 多股票处理 | **并行** | 股票之间无依赖，可并发 |
| 单股票多因子 | **流水线** | 因子计算有依赖关系 |

## 五、核心依赖关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           模块依赖关系                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  workflows/                                                                 │
│     │                                                                       │
│     ├─▶ services/data_service/ (数据服务)                                   │
│     │         │                                                             │
│     │         ├─▶ core/ (核心工具)                                          │
│     │         │         ├─▶ indicators/ (指标计算)                          │
│     │         │         ├─▶ storage/ (存储)                                 │
│     │         │         └─▶ cache/ (缓存)                                   │
│     │         │                                                             │
│     │         └─▶ services/data_service/datasource/ (数据源)                │
│     │                   ├─▶ providers.py                                    │
│     │                   └─▶ manager.py                                      │
│     │                                                                       │
│     ├─▶ services/stock_service/ (股票服务)                                  │
│     │         │                                                             │
│     │         ├─▶ core/factor_engine.py (因子引擎)                          │
│     │         └─▶ filters/ (过滤器)                                         │
│     │                                                                       │
│     ├─▶ services/notify_service/ (通知服务)                                 │
│     │         └─▶ templates/ (报告模板)                                     │
│     │                                                                       │
│     └─▶ core/workflow_framework.py (工作流框架)                             │
│               ├─▶ 依赖检查                                                   │
│               ├─▶ 断点续传                                                   │
│               └─▶ 自动修复                                                   │
│                                                                             │
│  依赖方向：上层 ──▶ 下层                                                      │
│  • 工作流层依赖服务层                                                         │
│  • 服务层依赖核心层                                                           │
│  • 核心层依赖基础设施                                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 六、总结

### 6.1 从哪里开始

1. **配置文件** (`config/`) - 了解系统配置
2. **工作流入口** (`workflows/`) - 了解业务流程
3. **数据服务** (`services/data_service/`) - 了解数据流
4. **核心模块** (`core/`) - 了解基础工具

### 6.2 从哪里结束

1. **数据输出** (`data/`) - 最终数据产物
2. **报告生成** (`services/notify_service/`) - 最终报告
3. **日志记录** (`logs/`) - 执行记录

### 6.3 执行策略

- **工作流之间**：流水线（有依赖）
- **工作流内部**：并行（IO/CPU密集型）
- **定时调度**：流水线（时间顺序）

### 6.4 关键特性

- ✅ **退市股票过滤** - 在股票列表阶段过滤
- ✅ **失败重试机制** - 双层重试（数据源层+工作流层）
- ✅ **断点续传机制** - 每100只股票保存断点
- ✅ **GE自动验证** - 采集前后自动检查
- ✅ **Rich进度监控** - 异步并发可视化
