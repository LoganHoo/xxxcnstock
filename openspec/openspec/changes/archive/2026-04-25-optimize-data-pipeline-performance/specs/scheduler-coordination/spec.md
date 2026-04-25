## ADDED Requirements

### Requirement: 分布式任务锁
系统 SHALL 使用 Redis 分布式锁确保同一任务只有一个实例执行。

#### Scenario: 获取任务锁
- **WHEN** 任务调度器尝试执行任务
- **THEN** 系统 SHALL 先获取分布式锁
- **AND** 锁的 key SHALL 格式为 `lock:task:{task_name}`
- **AND** 锁的超时时间 SHALL 为任务最大执行时间的 2 倍

#### Scenario: 锁冲突处理
- **WHEN** 另一个实例已持有任务锁
- **THEN** 当前实例 SHALL 跳过执行
- **AND** 记录跳过日志
- **AND** 返回状态 "SKIPPED_DUPLICATE"

#### Scenario: 锁自动释放
- **WHEN** 任务执行完成（成功或失败）
- **THEN** 系统 SHALL 自动释放锁
- **AND** 记录任务执行结果到状态表

### Requirement: 任务状态同步
系统 SHALL 维护任务执行状态表，实现 Kestra 与 APScheduler 状态同步。

#### Scenario: 状态记录
- **WHEN** 任务开始执行
- **THEN** 系统 SHALL 写入状态 "RUNNING"
- **AND** 包含开始时间、执行节点信息

#### Scenario: 状态查询
- **WHEN** 调度器查询任务状态
- **THEN** 系统 SHALL 返回当前状态
- **AND** 如果状态为 "RUNNING" 且超时，SHALL 标记为 "TIMEOUT"

#### Scenario: 心跳续期
- **WHEN** 长时间运行任务
- **THEN** 系统 SHALL 定期发送心跳
- **AND** 自动续期分布式锁
- **AND** 心跳间隔 SHALL 为锁超时的 1/3

### Requirement: 死锁检测
系统 SHALL 实现死锁检测机制，自动释放异常持有的锁。

#### Scenario: 死锁检测
- **WHEN** 锁持有时间超过超时时间
- **THEN** 系统 SHALL 自动释放锁
- **AND** 标记原任务为 "TIMEOUT"
- **AND** 发送告警通知

#### Scenario: 任务重试
- **WHEN** 任务因超时失败
- **THEN** 系统 SHALL 根据重试策略重新调度
- **AND** 最大重试次数 SHALL 为 3 次
