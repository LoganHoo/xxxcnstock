# 数据采集架构文档

## 1. 概述

本文档描述 xcnstock 项目的数据采集架构，包括三种采集场景：历史数据采集、实时数据采集、盘中数据采集。

## 2. 采集场景

### 2.1 历史数据采集 (Historical Collection)

**定义**: 采集已收盘的历史K线数据、基本面数据等

**特点**:
- 数据完整性高，可验证
- 采集时间灵活（非交易时间）
- 支持增量更新

**采集内容**:
| 数据类型 | 频率 | 存储位置 | 验证方式 |
|---------|------|---------|---------|
| 日K线数据 | 每日收盘后 | data/kline/{code}.parquet | gx_validator |
| 股票列表 | 每日1次 | data/stock_list.parquet | 完整性检查 |
| 基本面数据 | 每日1次 | data/fundamental/ | 基础检查 |

**采集流程**:
```
检查市场状态(收盘) → 获取股票列表 → 计算增量范围 → 批量采集 → 质量验证 → 存储
```

**使用命令**:
```bash
# 每日增量采集
python scripts/data_collection_controller.py --mode daily

# 全量重新采集
python scripts/data_collection_controller.py --mode full

# 指定股票增量采集
python scripts/data_collection_controller.py --mode incremental --codes 000001,000002
```

### 2.2 实时数据采集 (Real-time Collection)

**定义**: 定时采集实时行情数据、涨停池等快照数据

**特点**:
- 定时触发（5分钟/15分钟）
- 数据为快照，不保证连续性
- 轻量级验证

**采集内容**:
| 数据类型 | 频率 | 存储位置 | 验证方式 |
|---------|------|---------|---------|
| 实时行情 | 5分钟 | data/realtime/{timestamp}.parquet | 基础检查 |
| 涨停池 | 实时 | data/limitup/{date}.parquet | 基础检查 |
| 资金流向 | 15分钟 | data/fundflow/{date}.parquet | 基础检查 |

**采集流程**:
```
定时触发 → 获取快照 → 基础验证 → 追加存储
```

**使用方式**:
```python
from services.data_service.fetchers.quote import QuoteFetcher

fetcher = QuoteFetcher()
quotes = await fetcher.fetch_realtime_quotes()
```

### 2.3 盘中数据采集 (Intraday Collection)

**定义**: 交易时段内高频采集实时数据

**特点**:
- 秒级/逐笔频率
- 数据量大
- 轻量验证，快速存储

**采集内容**:
| 数据类型 | 频率 | 存储位置 | 验证方式 |
|---------|------|---------|---------|
| 实时报价 | 秒级 | data/intraday/ticks/ | 轻量检查 |
| 委托队列 | 逐笔 | data/intraday/orders/ | 轻量检查 |
| 成交明细 | 逐笔 | data/intraday/trades/ | 轻量检查 |

**采集流程**:
```
WebSocket连接 → 实时推送 → 缓存 → 批量写入
```

**注意事项**:
- 盘中不采集当日K线（数据不完整）
- 仅采集实时行情和事件数据

## 3. 架构组件

### 3.1 统一采集控制器

**文件**: `scripts/data_collection_controller.py`

**职责**:
- 协调各采集模块
- 管理采集流程
- 生成采集报告

**核心方法**:
```python
class DataCollectionController:
    async def run_daily_collection()      # 每日采集
    async def run_realtime_collection()   # 实时采集
    async def run_intraday_collection()   # 盘中采集
```

### 3.2 数据获取器

**文件**: `services/data_service/fetchers/`

| 获取器 | 用途 | 适用场景 |
|--------|------|---------|
| `unified_fetcher.py` | 统一获取接口 | 所有场景 |
| `kline_fetcher.py` | K线数据获取 | 历史采集 |
| `quote_fetcher.py` | 实时行情获取 | 实时/盘中 |
| `limitup_fetcher.py` | 涨停池获取 | 实时采集 |

### 3.3 质量验证器

**文件**: `services/data_service/quality/gx_validator.py`

**验证级别**:
| 级别 | 验证内容 | 适用场景 |
|------|---------|---------|
| 完整验证 | 列存在、类型、范围、OHLC逻辑 | 历史采集 |
| 基础验证 | 列存在、非空检查 | 实时采集 |
| 轻量验证 | 格式检查 | 盘中采集 |

### 3.4 存储管理器

**文件**: `services/data_service/storage/parquet_manager.py`

**存储策略**:
| 数据类型 | 存储格式 | 压缩方式 |
|---------|---------|---------|
| K线数据 | Parquet | snappy |
| 实时数据 | Parquet | snappy |
| 盘中数据 | Parquet + 内存缓存 | lz4 |

## 4. 采集规则

### 4.1 时间规则

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
│  盘中采集: 实时报价、委托、成交                                  │
│  历史采集: 日K线、基本面（必须收盘后）                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 数据质量规则

**历史数据质量阈值**:
- 成功率 ≥ 95%: 通过
- 成功率 90-95%: 警告
- 成功率 < 90%: 失败

**实时数据质量阈值**:
- 成功率 ≥ 90%: 通过
- 成功率 < 90%: 告警

## 5. 使用指南

### 5.1 历史数据采集

```bash
# 标准每日采集（收盘后执行）
python scripts/data_collection_controller.py --mode daily

# 指定日期采集
python scripts/data_collection_controller.py --mode daily --date 2026-04-18

# 全量重新采集
python scripts/data_collection_controller.py --mode full

# 指定股票采集
python scripts/data_collection_controller.py --mode incremental --codes 000001,000002,600000
```

### 5.2 实时数据采集

```bash
# 启动实时采集服务
python services/data_service/main.py

# 或者使用调度器
python scripts/run_scheduler.py
```

### 5.3 盘中数据采集

```bash
# 启动盘中采集（交易时段）
python scripts/collect_intraday.py

# 注意：盘中不采集K线数据
```

## 6. 监控与告警

### 6.1 日志记录

- 采集日志: `logs/system/collection_controller.log`
- 验证日志: `logs/system/quality_validator.log`
- 错误日志: `logs/system/error.log`

### 6.2 报告输出

- 采集报告: `data/collection_report.json`
- 质量报告: `data/quality_report.json`

### 6.3 告警条件

| 条件 | 级别 | 处理方式 |
|------|------|---------|
| 成功率 < 90% | 错误 | 发送告警，停止采集 |
| 成功率 90-95% | 警告 | 记录日志，继续采集 |
| 数据源故障 | 错误 | 自动切换备用源 |
| 存储失败 | 错误 | 重试3次，记录错误 |

## 7. 故障处理

### 7.1 常见问题

**Q: 盘中可以采集当日K线吗？**
A: 不可以。当日K线数据必须收盘后采集，盘中数据不完整。

**Q: 采集失败怎么办？**
A: 系统自动重试3次，仍失败则记录错误并继续下一个。

**Q: 如何验证数据质量？**
A: 使用 `python scripts/run_gx_validation.py` 运行验证。

### 7.2 应急处理

```bash
# 手动重新采集某只股票
python scripts/data_collection_controller.py --mode incremental --codes {code}

# 验证数据质量
python scripts/run_gx_validation.py

# 查看采集报告
cat data/collection_report.json
```

## 8. 附录

### 8.1 目录结构

```
data/
├── kline/              # 历史K线数据
│   ├── 000001.parquet
│   └── ...
├── realtime/           # 实时行情数据
│   ├── 20260419_093000.parquet
│   └── ...
├── intraday/           # 盘中数据
│   ├── ticks/
│   └── trades/
├── fundamental/        # 基本面数据
├── limitup/            # 涨停池数据
├── stock_list.parquet  # 股票列表
├── collection_report.json  # 采集报告
└── quality_report.json     # 质量报告
```

### 8.2 配置文件

```python
# core/config.py
class Settings:
    DATA_DIR = "data"
    COLLECTION_MODE = "daily"  # daily/full/incremental
    VALIDATION_ENABLED = True
    MIN_SUCCESS_RATE = 0.95
```
