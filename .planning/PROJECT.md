# XCNStock 量化交易平台 - GSD 项目

## 项目概述

**项目名称**: XCNStock 量化交易平台  
**项目类型**: A股量化选股分析系统  
**技术栈**: Python + Polars + Kestra + MySQL + Redis  
**创建日期**: 2026-04-25  
**最后更新**: 2026-04-25

## 项目目标

构建高性能、可扩展的A股量化选股分析平台，支持：
- 实时/历史行情数据采集
- 多因子选股策略
- 技术指标计算（Polars高性能实现）
- 工作流编排（Kestra）
- 双调度器架构（Kestra + APScheduler）

## 核心模块

### 1. 数据采集层
- **并行数据获取**: `core/parallel_fetcher.py`
- **增量处理**: `core/incremental_processor.py`
- **数据质量检查**: `core/data_quality_checker.py`

### 2. 指标计算层
- **Polars优化器**: `core/polars_optimizer.py` - 高性能技术指标
- **因子引擎**: `core/factor_engine.py`
- **策略引擎**: `core/strategy_engine.py`

### 3. 工作流编排层
- **Kestra工作流**: `kestra/flows/`
- **统一数据采集**: `xcnstock_data_collection_unified.yml`
- **统一监控**: `xcnstock_monitoring_unified.yml`

### 4. 缓存与存储层
- **多级缓存**: `core/cache/`
- **Parquet存储**: `data/kline/`
- **分布式锁**: `core/distributed_lock.py`

## 当前状态

### 已完成工作

#### ✅ 数据流水线性能优化 (2026-04-25)
- 并行数据获取引擎
- 增量数据处理
- Polars技术指标实现
- 多级缓存系统
- 分布式锁协调
- 监控告警系统

#### ✅ Kestra工作流合并 (2026-04-25)
- 18个 → 11个工作流 (减少38.9%)
- 统一数据采集工作流
- 统一监控工作流
- 迁移脚本和验证

### 待办事项

| 优先级 | 任务 | 计划日期 | 状态 |
|-------|------|---------|------|
| 中 | 删除废弃Kestra工作流文件 | 2025-06-25 | ⏳ |
| 中 | 归档OpenSpec变更文档 | 2025-06-25 | ⏳ |
| 低 | 添加更多技术指标 | 待定 | 📋 |
| 低 | 性能基准测试报告 | 待定 | 📋 |

## 技术规范

### 代码规范
- Python 3.11+
- PEP8 代码风格
- Type hints 类型注解
- 单元测试覆盖率 >80%

### 数据规范
- 历史数据: Parquet格式
- 实时数据: API查询
- 更新频率: 每日收盘后
- 数据范围: 最近3年

### 工作流规范
- 盘中禁止采集当日数据
- 强制收盘后采集（15:30后）
- 退市股票过滤
- 数据新鲜度检查（30天内）

## 项目结构

```
xcnstock/
├── .planning/          # GSD项目管理
├── core/               # 核心模块
│   ├── cache/          # 缓存系统
│   ├── indicators/     # 技术指标
│   ├── storage/        # 存储工具
│   └── ...
├── kestra/flows/       # Kestra工作流
├── scripts/pipeline/   # 流水线脚本
├── config/             # 配置文件
├── data/               # 数据目录
├── api/                # API接口
└── openspec/           # OpenSpec规范
```

## 关键指标

### 性能指标
- 数据采集: 5000+股票 < 5分钟
- 指标计算: 10,000条 < 2ms (Polars)
- 缓存命中: >90%

### 质量指标
- 测试通过率: 100% (13/13)
- 代码覆盖率: >80%
- 工作流健康: ✅ 全部通过

## 团队与联系

- **项目 Owner**: [待填写]
- **技术负责人**: [待填写]
- **运维负责人**: [待填写]

## 相关文档

- [CLAUDE.md](../CLAUDE.md) - 编码规范
- [AGENTS.md](../AGENTS.md) - AI代理指南
- [DEPLOYMENT.md](../DEPLOYMENT.md) - 部署文档
- [WORKLOAD_ANALYSIS.md](../WORKLOAD_ANALYSIS.md) - 工作负载分析
