## Why

当前数据流水线在处理 5000+ 只 A 股股票的日 K 线数据时存在性能瓶颈，导致每日收盘后的数据采集和因子计算任务耗时过长（超过 30 分钟），影响晚间选股报告的及时生成。同时，Kestra 工作流与 APScheduler 定时任务的协调机制不够完善，存在任务重复执行或状态不同步的风险。本次优化旨在将整体流水线执行时间缩短 60% 以上，并确保双调度器架构下任务的可靠完成。

## What Changes

- **并行数据采集**: 将串行股票数据采集改为批量并行模式，使用异步 HTTP 客户端和连接池
- **增量数据处理**: 实现增量更新机制，只处理新增或变更的数据，避免全量重新计算
- **Polars 性能优化**: 全面迁移至 Polars 进行数据处理，利用其 SIMD 优化和并行计算能力
- **缓存策略优化**: 引入多级缓存（内存 + Redis），缓存频繁访问的股票元数据和计算结果
- **双调度器协调**: 完善 Kestra 与 APScheduler 的任务状态同步机制，防止重复执行
- **任务监控告警**: 添加任务执行时间监控和超时告警，及时发现性能退化
- **数据库连接池**: 优化 SQLite 和 MySQL 连接池配置，减少连接开销
- **Parquet 分区优化**: 按日期和股票代码分区存储 Parquet 文件，提升查询效率

## Capabilities

### New Capabilities
- `parallel-data-fetch`: 并行数据采集引擎，支持批量异步获取多只股票数据
- `incremental-processing`: 增量数据处理引擎，智能识别数据变更并只处理差异部分
- `multi-level-cache`: 多级缓存系统，整合内存缓存和 Redis 分布式缓存
- `scheduler-coordination`: 双调度器协调服务，确保 Kestra 和 APScheduler 状态同步
- `pipeline-monitoring`: 流水线性能监控，实时追踪任务执行时间和成功率

### Modified Capabilities
- `data-collection`: 更新数据采集流程，集成并行获取和增量更新机制
- `factor-calculation`: 优化因子计算引擎，使用 Polars 替代 Pandas 进行批量计算
- `workflow-orchestration`: 增强工作流编排逻辑，支持任务依赖和失败重试

## Impact

- **代码模块**: `core/data_loader.py`, `services/data_service/`, `factors/`, `kestra/flows/`
- **配置文件**: `config/dual_scheduler.yaml`, `config/cron_tasks.yaml`
- **依赖变更**: 新增 `aiohttp` (异步 HTTP), `cachetools` (内存缓存)
- **API 变更**: 数据采集接口增加批量参数，保持向后兼容
- **数据库**: 新增缓存元数据表，现有表结构不变
- **部署**: 需要调整 Docker 资源限制（增加内存和 CPU 配额）
- **监控**: 新增 Prometheus 指标暴露端点 `/metrics`
