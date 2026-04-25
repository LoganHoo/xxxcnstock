## ADDED Requirements

### Requirement: 任务执行时间监控
系统 SHALL 监控每个任务的执行时间，识别性能退化。

#### Scenario: 执行时间记录
- **WHEN** 任务开始执行
- **THEN** 系统 SHALL 记录开始时间戳
- **AND** 任务完成时记录结束时间戳
- **AND** 计算并存储执行时长

#### Scenario: 超时告警
- **WHEN** 任务执行时间超过阈值
- **THEN** 系统 SHALL 触发告警
- **AND** 告警 SHALL 包含任务名称、预期时间、实际时间
- **AND** 阈值 SHALL 可配置（默认 10 分钟）

#### Scenario: 性能趋势分析
- **WHEN** 系统每日汇总
- **THEN** 系统 SHALL 计算平均执行时间
- **AND** 与历史数据对比
- **AND** 生成性能趋势报告

### Requirement: 任务成功率监控
系统 SHALL 监控任务执行成功率，及时发现系统性问题。

#### Scenario: 成功率统计
- **WHEN** 任务执行完成
- **THEN** 系统 SHALL 记录成功/失败状态
- **AND** 计算最近 24 小时成功率
- **AND** 成功率低于 95% 时触发告警

#### Scenario: 失败原因分类
- **WHEN** 任务执行失败
- **THEN** 系统 SHALL 记录失败原因
- **AND** 分类统计（网络错误、数据错误、超时等）

### Requirement: 监控指标暴露
系统 SHALL 暴露 Prometheus 格式的监控指标。

#### Scenario: 指标端点
- **WHEN** 访问 `/metrics` 端点
- **THEN** 系统 SHALL 返回 Prometheus 格式指标
- **AND** 包含任务执行时间、成功率、缓存命中率等

#### Scenario: 自定义指标
- **GIVEN** 以下业务指标
- **THEN** 系统 SHALL 暴露 `xcnstock_task_duration_seconds`（任务执行时间）
- **AND** `xcnstock_task_success_rate`（任务成功率）
- **AND** `xcnstock_cache_hit_ratio`（缓存命中率）
- **AND** `xcnstock_api_requests_total`（API 调用次数）

### Requirement: 告警通知
系统 SHALL 支持多渠道告警通知。

#### Scenario: 告警触发
- **WHEN** 监控指标触发告警条件
- **THEN** 系统 SHALL 发送告警通知
- **AND** 支持 Webhook、邮件等通知方式
- **AND** 告警 SHALL 包含上下文信息便于排查

#### Scenario: 告警抑制
- **WHEN** 相同告警在短时间内重复触发
- **THEN** 系统 SHALL 抑制重复告警
- **AND** 聚合相同类型的告警
- **AND** 发送摘要通知
