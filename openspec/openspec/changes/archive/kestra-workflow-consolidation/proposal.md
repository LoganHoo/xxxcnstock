## Why

当前 XCNStock 项目存在 22 个 Kestra 工作流，其中存在大量功能重叠和冗余。数据采集类工作流有 5 个（data_collection、daily_update、data_collection_with_ge、smart_pipeline、data_pipeline_simple），功能重叠度高达 80%；盘前报告有 2 个版本（morning_report 和 morning_report_simple）；监控维护类工作流也有重复。这种重复导致维护成本增加、配置复杂、容易出错。需要对工作流进行合并优化，减少数量、统一入口、简化配置。

## What Changes

- **合并数据采集工作流**：将 `xcnstock_data_collection`、`xcnstock_daily_update`、`xcnstock_data_collection_with_ge` 合并为统一的 `xcnstock_data_collection_unified`，通过参数控制不同模式（标准采集/每日更新/GE验证）
- **整合智能流水线**：将 `xcnstock_smart_pipeline` 的智能检查功能整合到 `xcnstock_data_pipeline`，删除独立的 smart_pipeline
- **统一盘前报告**：将 `xcnstock_morning_report_simple` 合并到 `xcnstock_morning_report`，添加 `debug_mode` 参数控制调试输出
- **合并监控维护**：将 `xcnstock_system_monitor` 和 `xcnstock_data_inspection` 合并为 `xcnstock_monitoring_unified`
- **删除冗余工作流**：删除 `xcnstock_data_pipeline_simple`、`xcnstock_debug`（使用频率低，可临时创建）
- **更新调度配置**：将所有定时任务指向新的统一工作流
- **保留过渡期**：原工作流保留 1 个月，添加 deprecated 标签，便于回滚

**BREAKING**: 工作流 ID 变更，需要更新所有调度配置和外部调用

## Capabilities

### New Capabilities
- `unified-data-collection`: 统一数据采集工作流，支持标准采集、每日更新、GE验证三种模式
- `unified-monitoring`: 统一监控维护工作流，整合系统监控和数据巡检功能

### Modified Capabilities
- `data-pipeline`: 添加智能健康检查功能（原 smart_pipeline 功能）
- `morning-report`: 添加 debug_mode 参数，支持调试模式输出

## Impact

- **Kestra 工作流**: 22 个 → 14 个，减少 36%
- **调度配置**: 需要更新所有 cron 触发器指向新工作流 ID
- **监控面板**: 需要更新工作流名称引用
- **文档**: 需要更新运维文档和流程图
- **回滚策略**: 保留原工作流 1 个月，添加 `deprecated: true` 标签
