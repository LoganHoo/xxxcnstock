## Context

XCNStock 项目使用 Kestra 作为工作流编排引擎，目前管理着 22 个工作流。随着项目发展，工作流数量不断增加，出现了以下问题：

1. **功能重叠严重**：数据采集类工作流有 5 个（data_collection、daily_update、data_collection_with_ge、smart_pipeline、data_pipeline_simple），核心逻辑都是调用 `scripts/pipeline/data_collect.py`，只是参数和附加功能不同

2. **维护成本高**：修改数据采集逻辑需要同步修改多个工作流，容易遗漏

3. **配置复杂**：新成员难以选择合适的采集工作流，需要理解各工作流的细微差别

4. **盘前报告重复**：morning_report 和 morning_report_simple 功能几乎相同，只是调试输出不同

5. **监控分散**：system_monitor 和 data_inspection 都是监控类任务，却分开维护

## Goals / Non-Goals

**Goals:**
- 将工作流数量从 22 个减少到 14 个（减少 36%）
- 统一数据采集入口，通过参数控制不同模式
- 简化工作流选择，降低认知负担
- 保持所有现有功能，确保向后兼容
- 提供清晰的迁移路径和回滚策略

**Non-Goals:**
- 不修改底层 Python 采集脚本逻辑
- 不改变调度时间（cron 表达式）
- 不引入新的外部依赖
- 不修改监控指标和告警规则

## Decisions

### Decision 1: 统一数据采集工作流

**方案**: 创建 `xcnstock_data_collection_unified`，通过 `mode` 参数控制行为

**参数设计**:
```yaml
inputs:
  - id: mode
    type: STRING
    defaults: "standard"  # standard/daily/ge/full
  - id: enable_ge_validation
    type: BOOLEAN
    defaults: false
  - id: update_stock_list
    type: BOOLEAN
    defaults: true
  - id: retry_failed
    type: BOOLEAN
    defaults: true
```

**理由**:
- 单一入口，降低认知负担
- 参数化设计，灵活适应不同场景
- 代码复用，减少维护成本

**替代方案**: 保留多个工作流，添加文档说明使用场景（ rejected：维护成本高）

### Decision 2: 智能检查功能整合

**方案**: 将 `smart_pipeline` 的智能健康检查作为 `data_pipeline` 的第一个阶段

**实现**:
```yaml
tasks:
  - id: smart_health_check
    type: io.kestra.plugin.scripts.python.Script
    description: 智能数据健康检查（原smart_pipeline功能）
    # 检查数据新鲜度，决定是否需要采集
```

**理由**:
- 避免重复检查，提高效率
- 保持流水线完整性
- 删除独立工作流，减少数量

### Decision 3: 保留过渡期

**方案**: 原工作流保留 1 个月，添加 `deprecated: true` 标签

**理由**:
- 提供回滚能力，降低风险
- 给运维团队适应时间
- 便于对比验证新工作流正确性

**清理计划**:
- Week 1-2: 并行运行，监控新工作流
- Week 3-4: 逐步切换流量到新工作流
- Week 5: 删除 deprecated 工作流

### Decision 4: 统一监控维护

**方案**: 合并 `system_monitor` 和 `data_inspection` 为 `xcnstock_monitoring_unified`

**任务设计**:
```yaml
tasks:
  - id: data_freshness_check      # 原 data_inspection
  - id: data_completeness_check   # 原 data_inspection
  - id: cache_cleanup             # 原 system_monitor
  - id: generate_dashboard        # 原 system_monitor
```

**触发器**: 保持原有的双触发（每天 03:00 + 每10分钟）

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| 参数配置错误导致采集失败 | 高 | 添加参数校验，提供默认值，保留原工作流作为备份 |
| 调度配置更新遗漏 | 中 | 创建配置检查脚本，列出所有需要更新的触发器 |
| 新工作流性能问题 | 中 | 保留过渡期，对比监控指标，发现问题立即回滚 |
| 团队成员不熟悉新参数 | 低 | 更新文档，提供使用示例，添加参数说明 |
| 回滚时数据不一致 | 低 | 避免在过渡期修改数据结构，保持向后兼容 |

**Trade-offs**:
- **复杂度 vs 简洁性**: 统一工作流增加了参数复杂度，但减少了工作流数量
- **灵活性 vs 一致性**: 参数化设计提供了灵活性，但需要更严格的参数管理
- **迁移成本 vs 长期收益**: 短期内需要投入迁移成本，长期降低维护成本

## Migration Plan

### Phase 1: 准备阶段 (Week 1)

1. **创建新工作流**
   - `xcnstock_data_collection_unified.yml`
   - `xcnstock_monitoring_unified.yml`
   - 更新 `xcnstock_data_pipeline.yml`（添加智能检查）
   - 更新 `xcnstock_morning_report.yml`（添加 debug 参数）

2. **标记旧工作流**
   ```yaml
   labels:
     deprecated: "true"
     replacement: "xcnstock_data_collection_unified"
   ```

3. **创建验证脚本**
   - 对比新旧工作流输出
   - 检查参数传递正确性

### Phase 2: 灰度阶段 (Week 2)

1. **并行运行**
   - 新工作流启用，但不删除旧工作流
   - 对比执行结果和性能指标

2. **更新调度配置（测试环境）**
   - 测试环境切换到新工作流
   - 验证所有场景正常

### Phase 3: 切换阶段 (Week 3)

1. **生产环境切换**
   - 逐个更新调度配置
   - 监控执行状态

2. **文档更新**
   - 更新运维文档
   - 添加新工作流使用说明

### Phase 4: 清理阶段 (Week 5)

1. **删除 deprecated 工作流**
   - 确认新工作流稳定运行 2 周
   - 删除旧工作流文件

2. **归档**
   - 记录变更日志
   - 更新架构文档

### Rollback Strategy

**触发条件**:
- 新工作流连续失败 3 次
- 数据质量问题
- 性能下降超过 20%

**回滚步骤**:
1. 立即切换调度配置回旧工作流 ID
2. 调查问题原因
3. 修复后重新部署

## Open Questions

1. **GE 验证的默认行为**: `enable_ge_validation` 是否应该默认开启？
   - 建议：默认关闭，避免影响现有流程

2. **监控统一后的触发频率**: 是否保持每10分钟生成监控面板？
   - 建议：保持现状，避免影响监控连续性

3. **过渡期长度**: 1个月是否足够？
   - 建议：根据实际运行情况调整，最短 2 周
