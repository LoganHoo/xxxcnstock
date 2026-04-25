## ADDED Requirements

### Requirement: 批量异步数据采集
系统 SHALL 支持使用异步 HTTP 客户端批量采集多只股票数据，将数据采集时间缩短至 10 分钟以内。

#### Scenario: 正常批量采集
- **WHEN** 系统启动每日数据采集任务
- **THEN** 系统 SHALL 使用异步并发方式同时采集多只股票数据
- **AND** 并发数 SHALL 控制在 50 以内以避免触发 API 限流
- **AND** 整体采集时间 SHALL 小于 10 分钟

#### Scenario: API 限流处理
- **WHEN** Tushare API 返回 429 限流错误
- **THEN** 系统 SHALL 自动等待并重试
- **AND** 重试间隔 SHALL 使用指数退避策略（1s, 2s, 4s, 8s）
- **AND** 最大重试次数 SHALL 为 3 次

#### Scenario: 部分失败处理
- **WHEN** 批量采集中部分股票失败
- **THEN** 系统 SHALL 记录失败股票代码
- **AND** 对失败股票进行单独重试
- **AND** 最终生成失败报告

### Requirement: 连接池管理
系统 SHALL 使用 HTTP 连接池复用连接，减少 TCP 握手开销。

#### Scenario: 连接复用
- **WHEN** 系统发起多个 API 请求
- **THEN** 系统 SHALL 复用现有 TCP 连接
- **AND** 连接池大小 SHALL 配置为 100
- **AND** 连接超时 SHALL 设置为 30 秒

### Requirement: 流量控制
系统 SHALL 实现流量控制机制，确保不超过 Tushare API 的调用限制（每分钟 500 次）。

#### Scenario: 流量限制
- **WHEN** 系统执行 API 调用
- **THEN** 系统 SHALL 监控调用频率
- **AND** 当接近限流阈值时 SHALL 自动降速
- **AND** 系统 SHALL 确保每分钟调用不超过 480 次（预留 20 次缓冲）
