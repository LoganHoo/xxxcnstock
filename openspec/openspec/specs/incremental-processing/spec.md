## ADDED Requirements

### Requirement: 增量数据检测
系统 SHALL 智能检测本地数据状态，只采集缺失或变更的数据。

#### Scenario: 检测缺失数据
- **WHEN** 系统检查某只股票的数据状态
- **THEN** 系统 SHALL 获取本地最新数据日期
- **AND** 计算需要补采的日期范围
- **AND** 只采集缺失日期的数据

#### Scenario: 首次全量采集
- **WHEN** 某只股票本地无历史数据
- **THEN** 系统 SHALL 执行全量采集
- **AND** 采集最近 3 年的历史数据

#### Scenario: 数据变更检测
- **WHEN** 股票发生除权除息
- **THEN** 系统 SHALL 检测数据哈希值变化
- **AND** 重新采集受影响的历史数据

### Requirement: 数据一致性校验
系统 SHALL 在增量更新后执行数据一致性校验。

#### Scenario: 日期连续性检查
- **WHEN** 增量采集完成后
- **THEN** 系统 SHALL 检查日期连续性
- **AND** 确保无缺失交易日
- **AND** 发现缺失时 SHALL 告警并补采

#### Scenario: 数据完整性验证
- **WHEN** 写入 Parquet 文件后
- **THEN** 系统 SHALL 验证文件完整性
- **AND** 检查记录数是否符合预期
- **AND** 校验关键字段（open, close, volume）无空值
