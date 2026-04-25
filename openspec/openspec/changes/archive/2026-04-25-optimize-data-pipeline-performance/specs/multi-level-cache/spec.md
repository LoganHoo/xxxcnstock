## ADDED Requirements

### Requirement: L1 内存缓存
系统 SHALL 使用 LRU 内存缓存存储热点数据，减少重复计算。

#### Scenario: 缓存股票列表
- **WHEN** 系统首次获取股票列表
- **THEN** 系统 SHALL 将结果存入内存缓存
- **AND** 缓存 TTL SHALL 为 1 小时
- **AND** 缓存容量 SHALL 为 1000 条

#### Scenario: 缓存命中
- **WHEN** 系统再次请求已缓存的数据
- **THEN** 系统 SHALL 从内存缓存直接返回
- **AND** 响应时间 SHALL 小于 1 毫秒

#### Scenario: 缓存失效
- **WHEN** 缓存数据超过 TTL
- **THEN** 系统 SHALL 自动从源刷新数据
- **AND** 刷新期间 SHALL 返回旧数据（异步更新）

### Requirement: L2 Redis 缓存
系统 SHALL 使用 Redis 作为分布式缓存，支持跨进程数据共享。

#### Scenario: 跨进程缓存共享
- **WHEN** 多个服务实例需要访问相同数据
- **THEN** 系统 SHALL 从 Redis 读取缓存
- **AND** 缓存键 SHALL 包含版本号便于更新

#### Scenario: Redis 故障降级
- **WHEN** Redis 连接失败
- **THEN** 系统 SHALL 降级到内存缓存
- **AND** 记录告警日志
- **AND** 定期尝试重连 Redis

### Requirement: 缓存一致性
系统 SHALL 确保缓存数据与源数据的一致性。

#### Scenario: 缓存刷新
- **WHEN** 管理员手动触发缓存刷新
- **THEN** 系统 SHALL 清除所有相关缓存
- **AND** 重新加载最新数据
- **AND** 返回刷新结果统计

#### Scenario: 缓存穿透防护
- **WHEN** 查询不存在的数据
- **THEN** 系统 SHALL 缓存空结果（短时间）
- **AND** 防止重复查询源系统
