# 数据采集完整指南

## 1. 概述

本文档详细描述 xcnstock 项目的数据采集系统，包括采集类型、采集对象、采集场景区分等。

## 2. 采集类型

### 2.1 按数据类型分类

| 数据类型 | 说明 | 采集频率 | 存储格式 |
|---------|------|---------|---------|
| **K线数据** | 日K线、周K线、月K线 | 每日收盘后 | Parquet |
| **股票列表** | 全市场股票基础信息 | 每日1次 | Parquet |
| **基本面数据** | PE、PB、市值等 | 每日1次 | Parquet |
| **实时行情** | 最新价格、涨跌幅 | 5分钟/实时 | Parquet |
| **涨停池** | 涨停股票列表 | 实时 | Parquet |
| **资金流向** | 主力净流入等 | 15分钟 | Parquet |
| **Tick数据** | 逐笔成交 | 秒级 | Parquet |
| **委托队列** | 买卖五档 | 秒级 | Parquet |

### 2.2 按采集对象分类

| 采集对象 | 数据范围 | 数据量 | 优先级 |
|---------|---------|--------|--------|
| **A股全市场** | 5000+只股票 | ~120MB | 高 |
| **指数数据** | 主要指数 | ~5MB | 高 |
| **ETF基金** | 场内ETF | ~20MB | 中 |
| **可转债** | 可转债列表 | ~10MB | 中 |
| **期货数据** | 股指期货 | ~5MB | 低 |
| **期权数据** | 股票期权 | ~5MB | 低 |

### 2.3 按数据源分类

| 数据源 | 数据类型 | 稳定性 | 优先级 |
|--------|---------|--------|--------|
| **Tushare** | K线、基本面 | 高 | 主数据源 |
| **Baostock** | K线、股票列表 | 中 | 备用源1 |
| **AKShare** | 实时行情、涨停池 | 中 | 备用源2 |
| **东方财富** | 资金流向、Tick | 低 | 备用源3 |

## 3. 三种采集场景

### 3.1 场景对比

```
┌─────────────────────────────────────────────────────────────────┐
│                    三种采集场景对比                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  维度          历史采集          实时采集          盘中采集      │
│  ─────────────────────────────────────────────────────────────  │
│  时间          收盘后            定时              交易时段      │
│  频率          每日1次           5-15分钟          秒级/逐笔     │
│  数据完整性    完整              快照              实时          │
│  验证级别      完整验证          基础验证          轻量验证      │
│  失败处理      重试+记录         降级+告警          缓存+补采     │
│  存储策略      按股票分文件      按时间分文件       内存+批量     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 历史数据采集

**定义**: 采集已收盘的历史K线数据、基本面数据等

**采集时间**:
```
工作日 15:30 后（收盘后30分钟）
非交易日全天可采
```

**采集内容**:

| 数据项 | 说明 | 存储位置 | 更新方式 |
|--------|------|---------|---------|
| 日K线 | 开高低收量 | data/kline/{code}.parquet | 增量更新 |
| 股票列表 | 代码、名称、行业 | data/stock_list.parquet | 全量替换 |
| 基本面 | PE、PB、市值 | data/fundamental/ | 增量更新 |

**采集流程**:
```
检查市场状态(收盘) → 获取股票列表 → 计算增量范围 → 批量采集 → 质量验证 → 存储
```

**使用命令**:
```bash
# 每日增量采集
python scripts/collect.py historical --mode daily

# 全量重新采集
python scripts/collect.py historical --mode full

# 指定股票增量采集
python scripts/collect.py historical --mode incremental --codes 000001,000002
```

**代码示例**:
```python
from services.data_service.collectors import HistoricalCollector

async def collect_historical():
    collector = HistoricalCollector()
    await collector.initialize()

    # 运行每日采集
    stats = await collector.run_daily_collection()

    print(f"总股票: {stats['total_stocks']}")
    print(f"成功: {stats['success_count']}")
    print(f"失败: {stats['failed_count']}")
    print(f"质量: {stats['quality_success_rate']:.1%}")
```

### 3.3 实时数据采集

**定义**: 定时采集实时行情数据、涨停池等快照数据

**采集时间**:
```
交易日 9:30-15:00
频率: 5分钟/15分钟
```

**采集内容**:

| 数据项 | 说明 | 存储位置 | 更新方式 |
|--------|------|---------|---------|
| 实时行情 | 最新价、涨跌幅 | data/realtime/{timestamp}.parquet | 增量追加 |
| 涨停池 | 涨停股票列表 | data/limitup/{date}.parquet | 增量追加 |
| 资金流向 | 主力净流入 | data/fundflow/{date}.parquet | 增量追加 |

**采集流程**:
```
定时触发 → 获取快照 → 基础验证 → 追加存储
```

**使用命令**:
```bash
# 单次采集
python scripts/collect.py realtime --types quotes,limitup

# 定时采集（每5分钟）
python scripts/collect.py realtime --schedule --interval 5
```

**代码示例**:
```python
from services.data_service.collectors import RealtimeCollector

async def collect_realtime():
    collector = RealtimeCollector()
    await collector.initialize()

    # 采集实时行情和涨停池
    stats = await collector.run_collection(['quotes', 'limitup'])

    print(f"任务数: {stats['total_tasks']}")
    print(f"成功: {stats['success_count']}")
```

### 3.4 盘中数据采集

**定义**: 交易时段内高频采集实时数据

**采集时间**:
```
交易日 9:30-11:30, 13:00-15:00
频率: 秒级/逐笔
```

**采集内容**:

| 数据项 | 说明 | 存储位置 | 更新方式 |
|--------|------|---------|---------|
| Tick数据 | 逐笔成交 | data/intraday/ticks/ | 批量写入 |
| 委托队列 | 买卖五档 | data/intraday/orders/ | 批量写入 |
| 成交明细 | 逐笔成交 | data/intraday/trades/ | 批量写入 |

**采集流程**:
```
WebSocket连接 → 实时推送 → 内存缓存 → 批量写入
```

**使用命令**:
```bash
# 盘中采集（指定股票，运行60分钟）
python scripts/collect.py intraday --codes 000001,000002 --duration 60
```

**代码示例**:
```python
from services.data_service.collectors import IntradayCollector

async def collect_intraday():
    collector = IntradayCollector()

    # 采集热门股票
    codes = ['000001', '000002', '600000', '600519']

    # 运行采集（1分钟）
    result = await collector.run_collection(mode='tick', codes=codes)

    print(f"成功: {result.success}")
    print(f"采集数量: {result.count}")
```

**⚠️ 重要提示**: 盘中不采集当日K线数据，因为数据不完整！

## 4. 时间规则

### 4.1 交易日时间线

```
┌─────────────────────────────────────────────────────────────────┐
│  交易日时间线                                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  09:30        11:30   13:00        15:00      15:30            │
│    │───────────│       │───────────│            │              │
│    ▼           ▼       ▼           ▼            ▼              │
│  ┌─────────┐  休市  ┌─────────┐  收盘      可采集当日数据       │
│  │ 盘中采集 │──────▶│ 盘中采集 │────────▶  历史数据采集         │
│  │ (实时)  │       │ (实时)  │          (K线/基本面)           │
│  └─────────┘       └─────────┘                                 │
│                                                                 │
│  盘中采集: Tick、委托、成交                                      │
│  历史采集: K线、基本面（必须收盘后）                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 采集时间矩阵

| 当前时间 | 可采集数据 | 禁止采集 |
|---------|-----------|---------|
| 9:30-11:30 | Tick、实时行情 | 当日K线 |
| 11:30-13:00 | 历史数据 | 当日K线 |
| 13:00-15:00 | Tick、实时行情 | 当日K线 |
| 15:00-15:30 | 等待 | 当日K线 |
| 15:30+ | 所有数据 | 无 |
| 非交易日 | 历史数据 | 无 |

## 5. 质量验证

### 5.1 验证级别

| 采集场景 | 验证级别 | 验证内容 | 阈值 |
|---------|---------|---------|------|
| 历史采集 | 完整验证 | 列存在、类型、范围、OHLC逻辑 | 成功率≥95% |
| 实时采集 | 基础验证 | 列存在、非空检查 | 成功率≥90% |
| 盘中采集 | 轻量验证 | 格式检查 | 基础检查 |

### 5.2 验证规则

**K线数据验证**:
- 必需列: trade_date, code, open, high, low, close, volume
- 价格范围: 0-5000
- 成交量范围: 0-1000亿
- OHLC逻辑: high ≥ max(open,close) ≥ min(open,close) ≥ low

**股票列表验证**:
- 必需列: code, name
- code唯一性
- 无空值

### 5.3 验证命令

```bash
# 运行数据验证
python scripts/run_gx_validation.py

# 指定抽样数量
python scripts/run_gx_validation.py --sample-size 200

# 验证指定股票
python scripts/run_gx_validation.py --code 000001
```

## 6. 存储结构

### 6.1 目录结构

```
data/
├── kline/                      # 历史K线数据
│   ├── 000001.parquet         # 单只股票K线
│   ├── 000002.parquet
│   └── ...
├── realtime/                   # 实时行情数据
│   ├── 20260419_093000.parquet
│   └── ...
├── intraday/                   # 盘中数据
│   ├── ticks/                 # Tick数据
│   └── trades/                # 成交明细
├── fundamental/                # 基本面数据
├── limitup/                    # 涨停池数据
├── stock_list.parquet          # 股票列表
├── collection_report.json      # 采集报告
└── quality_report.json         # 质量报告
```

### 6.2 文件命名规范

| 数据类型 | 命名格式 | 示例 |
|---------|---------|------|
| K线数据 | {code}.parquet | 000001.parquet |
| 实时行情 | {date}_{time}.parquet | 20260419_093000.parquet |
| 涨停池 | {date}.parquet | 20260419.parquet |
| Tick数据 | ticks_{date}.parquet | ticks_20260419.parquet |

## 7. 使用指南

### 7.1 命令行使用

```bash
# 检查市场状态
python scripts/collect.py check-market

# 历史数据采集
python scripts/collect.py historical --mode daily
python scripts/collect.py historical --mode full
python scripts/collect.py historical --mode incremental --codes 000001,000002

# 实时数据采集
python scripts/collect.py realtime --types quotes,limitup
python scripts/collect.py realtime --schedule --interval 5

# 盘中数据采集
python scripts/collect.py intraday --codes 000001 --duration 60
```

### 7.2 Python API 使用

```python
import asyncio
from services.data_service.collectors import (
    HistoricalCollector,
    RealtimeCollector,
    IntradayCollector
)

async def main():
    # 历史数据采集
    historical = HistoricalCollector()
    await historical.initialize()
    await historical.run_daily_collection()

    # 实时数据采集
    realtime = RealtimeCollector()
    await realtime.initialize()
    await realtime.run_collection(['quotes', 'limitup'])

    # 盘中数据采集
    intraday = IntradayCollector()
    await intraday.run_collection(mode='tick', codes=['000001'])

asyncio.run(main())
```

### 7.3 调度配置

**DolphinScheduler 工作流**:
```bash
# 部署工作流
python scripts/deploy_workflows.py

# 测试连接
python scripts/deploy_workflows.py --test-connection

# 列出工作流
python scripts/deploy_workflows.py --list
```

**定时任务**:
| 任务 | 调度表达式 | 说明 |
|------|-----------|------|
| 历史采集 | 0 30 15 * * 1-5 | 工作日 15:30 |
| 实时采集 | 0 */5 9-15 * * 1-5 | 交易日每5分钟 |
| 盘中采集 | */1 9-15 * * 1-5 | 交易日每分钟 |

## 8. 监控与告警

### 8.1 监控指标

| 指标 | 说明 | 正常范围 |
|------|------|---------|
| 采集成功率 | 成功采集的股票比例 | > 95% |
| 数据质量 | 验证通过率 | > 95% |
| 采集延迟 | 数据更新延迟 | < 1天 |
| 存储容量 | 数据文件总大小 | < 10GB |

### 8.2 告警规则

| 条件 | 级别 | 处理方式 |
|------|------|---------|
| 成功率 < 90% | 错误 | 发送告警，停止采集 |
| 成功率 90-95% | 警告 | 记录日志，继续采集 |
| 数据源故障 | 错误 | 自动切换备用源 |
| 存储失败 | 错误 | 重试3次，记录错误 |

### 8.3 日志查看

```bash
# 采集日志
tail -f logs/system/collection_controller.log

# 验证日志
tail -f logs/system/quality_validator.log

# 错误日志
tail -f logs/system/error.log
```

## 9. 故障处理

### 9.1 常见问题

**Q: 盘中可以采集当日K线吗？**
A: 不可以。当日K线数据必须收盘后采集，盘中数据不完整。

**Q: 采集失败怎么办？**
A: 系统自动重试3次，仍失败则记录错误并继续下一个。

**Q: 如何验证数据质量？**
A: 使用 `python scripts/run_gx_validation.py` 运行验证。

**Q: 如何重新采集某只股票？**
A: `python scripts/collect.py historical --mode incremental --codes {code}`

### 9.2 应急处理

```bash
# 手动重新采集某只股票
python scripts/collect.py historical --mode incremental --codes 000001

# 验证数据质量
python scripts/run_gx_validation.py

# 查看采集报告
cat data/collection_report.json

# 清空缓存重新采集
rm -rf data/kline/*.parquet
python scripts/collect.py historical --mode full
```

## 10. 附录

### 10.1 环境变量

```bash
# DolphinScheduler 配置
DOLPHINSCHEDULER_URL=http://192.168.1.168:12345
DOLPHINSCHEDULER_USER=admin
DOLPHINSCHEDULER_PASSWORD=dolphinscheduler123

# 数据采集配置
DATA_DIR=data
MIN_SUCCESS_RATE=0.95
VALIDATION_ENABLED=true
```

### 10.2 相关文件

| 文件 | 说明 |
|------|------|
| `scripts/collect.py` | 统一采集入口 |
| `services/data_service/collectors/` | 采集器模块 |
| `services/scheduler/dolphinscheduler_client.py` | DS 客户端 |
| `scripts/deploy_workflows.py` | 工作流部署 |
| `scripts/run_gx_validation.py` | 数据验证 |

### 10.3 相关文档

- [数据采集架构](data_collection_architecture.md)
- [DolphinScheduler 集成](dolphinscheduler_integration.md)
