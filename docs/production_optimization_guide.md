# 生产环境优化和部署指南

## 概述

本指南介绍如何使用生产环境优化和部署系统，通过遗传算法自动搜索最优因子组合和参数，并将冠军策略部署到生产环境。

## 功能特性

1. **自动优化**：使用遗传算法搜索最优因子组合、权重和参数
2. **自动部署**：将冠军策略自动更新到生产配置文件
3. **配置备份**：在更新前自动备份现有配置
4. **股价更新**：更新前一天选股的当天股价
5. **完整报告**：生成详细的优化和部署报告

## 工作流程

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  1. 运行优化  │ -> │  2. 更新配置  │ -> │  3. 更新股价  │ -> │  4. 生成报告  │
│  (遗传算法)   │    │  (生产环境)   │    │  (前一天选股)  │    │  (详细说明)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 使用方法

### 1. 快速开始

```bash
# 运行完整的优化和部署工作流
python scripts/production_optimization.py

# 自定义参数
python scripts/production_optimization.py --population 50 --generations 30

# 只运行优化，不更新股价
python scripts/production_optimization.py --no-update-prices
```

### 2. 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|---------|
| `--population` | 种群大小 | 30 |
| `--generations` | 迭代代数 | 20 |
| `--data-dir` | 数据目录 | data |
| `--no-update-prices` | 不更新股价 | False |

### 3. Docker 环境

```bash
# 构建镜像
docker build -t xcnstock:latest .

# 运行优化和部署
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/optimization/results:/app/optimization/results \
  xcnstock:latest \
  python scripts/production_optimization.py
```

## 输出文件

### 1. 优化结果

```
optimization/results/
├── champion_strategy_YYYYMMDD_HHMMSS.yaml    # 冠军策略配置
├── optimization_report_YYYYMMDD_HHMMSS.md  # 优化报告
└── production_deployment_YYYYMMDD_HHMMSS.txt # 生产部署报告
```

### 2. 配置备份

```
config/backups/
├── multi_factor_YYYYMMDD_HHMMSS.yaml
└── xcn_comm_YYYYMMDD_HHMMSS.yaml
```

### 3. 更新的配置文件

```
config/
├── strategies/multi_factor.yaml    # 多因子策略配置
└── xcn_comm.yaml                # 推荐系统配置
```

## 优化参数说明

### 遗传算法参数

- **种群大小 (population)**: 每代包含的染色体数量
  - 推荐值：30-50
  - 更大值：搜索空间更大，但耗时更长
  - 更小值：搜索更快，但可能错过最优解

- **迭代代数 (generations)**: 进化迭代次数
  - 推荐值：20-30
  - 更大值：收敛更稳定，但耗时更长
  - 更小值：快速收敛，但可能未达到最优

### 适应度函数

综合评分 = 0.4 × 年化收益 + 0.3 × 夏普比率 - 0.2 × 最大回撤 + 0.1 × 胜率

- **年化收益**：策略的年化收益率
- **夏普比率**：风险调整后收益
- **最大回撤**：最大损失幅度
- **胜率**：盈利交易占比

## 冠军策略配置

### 因子配置

```yaml
factors:
  selected: [rsi, macd, kdj]  # 选中的因子
  weights:
    rsi: 0.4                    # 因子权重
    macd: 0.35
    kdj: 0.25
  params:
    rsi:
      period: 14                  # 因子参数
    macd:
      fast_period: 12
      slow_period: 26
      signal_period: 9
```

### 过滤器配置

```yaml
filters:
  selected:
    - suspension_filter            # 停牌过滤
    - price_filter                # 价格过滤
    - volume_filter               # 成交量过滤
```

### 执行参数

```yaml
execution:
  holding_days: 5               # 持仓天数
  position_size: 10             # 持仓数量
```

## 监控和验证

### 1. 查看优化历史

```bash
# 查看所有优化结果
ls -la optimization/results/

# 查看最新报告
cat optimization/results/production_deployment_*.txt | tail -50
```

### 2. 验证配置更新

```bash
# 查看多因子策略配置
cat config/strategies/multi_factor.yaml

# 查看推荐系统配置
cat config/xcn_comm.yaml
```

### 3. 回滚配置

```bash
# 恢复之前的配置
cp config/backups/multi_factor_YYYYMMDD_HHMMSS.yaml config/strategies/multi_factor.yaml
cp config/backups/xcn_comm_YYYYMMDD_HHMMSS.yaml config/xcn_comm.yaml
```

## 定期优化建议

### 优化频率

- **每周优化**：适应市场变化
- **月度优化**：深度参数调优
- **季度优化**：策略全面评估

### 优化时机

- **收盘后**：使用最新数据
- **周末**：有充足时间运行
- **非交易时段**：避免影响实时系统

## 性能优化

### 加速优化

1. **减少数据范围**：使用最近1-2年数据
2. **减少股票数量**：使用代表性股票子集
3. **减少种群/代数**：快速迭代验证

### 提高精度

1. **增加种群/代数**：扩大搜索空间
2. **增加数据范围**：使用3年历史数据
3. **增加股票数量**：提高策略普适性

## 故障排查

### 问题 1: 优化不收敛

**症状**：适应度不提升或波动大

**解决方案**：
- 增加迭代代数
- 调整交叉/变异率
- 检查数据质量

### 问题 2: 配置更新失败

**症状**：配置文件未更新

**解决方案**：
- 检查文件权限
- 检查配置文件格式
- 查看备份目录

### 问题 3: 股价更新失败

**症状**：股价未更新

**解决方案**：
- 检查数据文件存在
- 检查日期格式
- 查看日志错误

## 最佳实践

1. **定期备份**：在优化前手动备份配置
2. **分步验证**：先小规模测试，再全面部署
3. **监控效果**：跟踪冠军策略的实际表现
4. **版本控制**：使用 Git 管理配置变更
5. **文档记录**：记录每次优化的参数和结果

## 相关文档

- [因子系统设计](../docs/plans/2026-03-26-factor-system-design.md)
- [遗传算法优化](../optimization/genetic_optimizer.py)
- [因子组合优化](../optimization/factor_combination_optimizer.py)
- [配置管理](../config/README.md)

## 支持

如有问题，请查看：
- 日志文件：`logs/`
- 错误信息：控制台输出
- 测试脚本：`scripts/test_production_optimization.py`
