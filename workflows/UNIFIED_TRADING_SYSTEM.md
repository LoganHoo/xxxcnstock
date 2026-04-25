# 统一交易系统 - 完整版

## 📋 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      统一交易系统                                │
│                    UnifiedTradingSystem                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│  │ 数据采集    │ │ 数据质量    │ │ 数据清洗    │ │ 复盘系统   │ │
│  │ Collection  │ │ Quality     │ │ Cleaning    │ │ Review    │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│  │ 选股评分    │ │ 盘前系统    │ │ 盘中系统    │ │ 报告服务   │ │
│  │ Selection   │ │ PreMarket   │ │ Intraday    │ │ Report    │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎯 功能模块

### 1. 数据采集模块 (UnifiedDataCollectionModule)

**数据源**: 7类数据源统一采集

| 数据源 | 方法 | 说明 | 状态 |
|:---|:---|:---|:---:|
| 股票列表 | `_collect_stock_list()` | 验证有效性，过滤退市/ST | ✅ |
| 个股K线 | `_collect_stock_kline()` | 批量采集，支持断点续传 | ✅ |
| 基本面数据 | `_collect_fundamental()` | 财务指标数据 | 🔄 |
| CCTV财经 | `_collect_cctv()` | 财经新闻数据 | 🔄 |
| 大盘指数 | `_collect_market_index()` | 上证指数、深证成指等 | ✅ |
| 外盘指数 | `_collect_global_index()` | 美股、港股指数 | 🔄 |
| 大宗商品 | `_collect_commodity()` | 黄金、原油等 | 🔄 |

**股票有效性验证**:
- ✅ 过滤退市股票（名称含"退市"）
- ✅ 过滤ST风险股（名称含"ST"）
- ✅ 过滤新股（上市不足60天）
- ✅ 检查数据新鲜度

---

### 2. 数据质量模块 (DataQualityModule)

**功能**:
- 🔍 质量检查：成功率、数据完整性
- 🔄 自动重试：失败数据源自动重试
- 📊 质量评分：0-100分质量评分
- 📧 异常报告：发送质量异常通知

**重试策略**:
```python
# 触发条件
- 采集状态为 failed/partial
- 质量评分 < 90分

# 重试机制
- 延迟2秒后重试
- 最多重试3次
- 记录重试结果
```

---

### 3. 数据清洗模块 (DataCleaningModule)

**清洗流程**:
1. **K线数据清洗**
   - 去除异常值（涨跌幅超±20%）
   - 填充缺失值
   - 统一数据格式

2. **基本面数据清洗**
   - 财务指标标准化
   - 异常财报标记

3. **数据对齐**
   - 日期对齐
   - 股票代码对齐
   - 多数据源合并

---

### 4. 复盘系统模块 (ReviewSystemModule)

**昨日选股复盘**:
- 获取昨日选股结果
- 更新当天价格和状态
- 验证选股效果
- 计算收益率

**大盘预测复盘**:
- 验证大盘趋势预测
- 分析热门板块准确性
- 龙虎榜数据回顾
- 生成复盘报告

---

### 5. 选股评分模块 (StockSelectionModule)

**多因子评分体系**:

| 因子类别 | 权重 | 包含指标 |
|:---|:---:|:---|
| 财务评分 | 40% | ROE、毛利率、成长性、偿债能力 |
| 市场评分 | 30% | 资金流向、龙虎榜、技术指标 |
| 公告评分 | 20% | 业绩预告、重大事项 |
| 技术评分 | 10% | 量价关系、趋势 |

**选股流程**:
1. 全市场股票初筛
2. 多因子打分
3. 加权计算总分
4. 排序选出Top N

---

### 6. 盘前系统模块 (PreMarketModule)

**执行时间**: 9:26（集合竞价结束后）

**功能**:
- 📈 涨停股票分析
- 🔮 开板概率预测
- 🎯 打板标的推荐
- ⚠️ 风险提示

**涨停板报告内容**:
- 涨停股票列表
- 涨停原因分析
- 封单金额/比例
- 开板概率预测

---

### 7. 盘中监控模块 (IntradayModule)

**监控功能**:

| 功能 | 说明 |
|:---|:---|
| 热点板块监控 | 实时追踪热点板块轮动 |
| 个股异动预警 | 价格/成交量异常预警 |
| 持仓监控 | 跟踪持仓股票状态 |

**交易信号**:
- 🟢 建仓信号：符合买入条件
- 🔵 加仓信号：趋势确认加强
- 🟡 减仓信号：风险信号出现
- 🔴 平仓信号：止损/止盈触发

---

## 🚀 使用方法

### 完整流程执行

```bash
# 执行完整交易流程
python workflows/unified_trading_system.py --date 2026-04-23

# 输出：完整执行报告（JSON格式）
```

### 盘前系统（9:26）

```bash
# 生成涨停板报告
python workflows/unified_trading_system.py --mode pre_market --date 2026-04-23
```

### 盘中监控

```bash
# 启动盘中监控（持续运行）
python workflows/unified_trading_system.py --mode intraday
```

### Python API调用

```python
from workflows.unified_trading_system import UnifiedTradingSystem

# 初始化系统
system = UnifiedTradingSystem()

# 执行完整流程
result = system.execute(date='2026-04-23')

# 执行盘前系统
pre_market_report = system.run_pre_market(date='2026-04-23')

# 启动盘中监控
system.run_intraday()
```

---

## 📊 执行流程

```
┌─────────────────────────────────────────────────────────────────┐
│                        执行流程图                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐                                                │
│  │ 依赖检查    │───❌失败──→ 自动修复 ──→ 重试                  │
│  └──────┬──────┘                                                │
│         ✅通过                                                  │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 阶段1: 采集 │──→ 股票列表、K线、基本面、指数...              │
│  └──────┬──────┘                                                │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 阶段2: 质检 │──→ 检查质量 ──→ 自动重试                      │
│  └──────┬──────┘                                                │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 阶段3: 清洗 │──→ 清洗数据、对齐                              │
│  └──────┬──────┘                                                │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 阶段4: 复盘 │──→ 昨日选股复盘、大盘预测复盘                  │
│  └──────┬──────┘                                                │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 阶段5: 选股 │──→ 多因子评分 ──→ Top N股票                   │
│  └──────┬──────┘                                                │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │ 生成报告    │──→ 保存结果、发送通知                          │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 输出文件

| 文件路径 | 说明 |
|:---|:---|
| `data/kline/{code}.parquet` | 个股K线数据 |
| `data/market/index_*.parquet` | 大盘指数数据 |
| `data/stock_list_{date}.parquet` | 当日股票列表 |
| `data/selection_report.db` | SQLite选股结果数据库 |

---

## 🔧 系统特性

### 增强功能（废弃基础版）

| 特性 | 说明 |
|:---|:---|
| ✅ 依赖检查 | 启动前检查数据源、存储空间 |
| ✅ 自动修复 | 依赖问题自动修复尝试 |
| ✅ 自动重试 | 失败任务自动重试（最多3次） |
| ✅ 断点续传 | 支持中断后从断点恢复 |
| ✅ 质量检查 | GE数据质量验证 |
| ✅ 报告发送 | 执行完成后发送报告 |

### 废弃的基础版

以下基础版工作流已废弃，功能由统一交易系统替代：

- ❌ `data_collection_workflow.py` → 使用 `UnifiedDataCollectionModule`
- ❌ `stock_selection_workflow.py` → 使用 `StockSelectionModule`
- ❌ `enhanced_data_collection_workflow.py` → 整合到统一系统
- ❌ `enhanced_selection_workflow.py` → 整合到统一系统

---

## 📝 待实现功能

| 功能 | 优先级 | 状态 |
|:---|:---:|:---:|
| CCTV财经数据采集 | 中 | 🔄 |
| 外盘指数数据采集 | 中 | 🔄 |
| 大宗商品数据采集 | 低 | 🔄 |
| 基本面数据详细采集 | 高 | 🔄 |
| 复盘系统详细实现 | 高 | 🔄 |
| 多因子评分详细算法 | 高 | 🔄 |
| 涨停板预测模型 | 中 | 🔄 |
| 盘中实时监控 | 中 | 🔄 |

---

## 🎓 架构说明

### 继承关系

```
WorkflowExecutor (抽象基类)
    └── UnifiedTradingSystem (统一交易系统)
            ├── UnifiedDataCollectionModule
            ├── DataQualityModule
            ├── DataCleaningModule
            ├── ReviewSystemModule
            ├── StockSelectionModule
            ├── PreMarketModule
            └── IntradayModule
```

### 设计模式

- **模板方法模式**: `WorkflowExecutor` 定义执行框架
- **策略模式**: 各模块可独立替换实现
- **观察者模式**: 盘中监控的事件通知

---

## 📞 使用示例

### 完整示例

```python
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from workflows.unified_trading_system import UnifiedTradingSystem
from datetime import datetime

# 初始化
system = UnifiedTradingSystem()

# 获取今天日期
today = datetime.now().strftime('%Y-%m-%d')

# 执行完整流程
print(f"🚀 启动统一交易系统 - {today}")
result = system.execute(date=today)

# 查看结果
print(f"\n执行状态:")
print(f"  日期: {result['date']}")
print(f"  耗时: {result['duration_seconds']:.2f}秒")
print(f"  采集: {len(result['collection'])} 个数据源")
print(f"  质量: {result['quality']['overall_status']}")
```

---

**创建时间**: 2026-04-23  
**版本**: v1.0  
**状态**: 核心框架完成，部分功能待实现
