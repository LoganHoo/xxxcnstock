# 冠军因子组合优化与股价更新流水线设计

## 日期：2026-03-31

## 一、需求概述

1. **因子组合优化**：通过遗传算法测试不同因子组合、参数配置，找到冠军组合并更新到生产环境
2. **股价更新**：对前一天的选股结果，更新当天的股价

## 二、设计决策

| 项目 | 选择 |
|------|------|
| 优先级 | 先优化冠军策略，再实现股价更新 |
| 回测范围 | 全量5383只股票 |
| 优化规模 | 大规模（种群50，迭代30代） |
| 股价更新 | 更新本地Parquet文件 |
| 方案 | 混合方案（增强现有优化器 + 独立更新脚本） |

## 三、架构设计

```
┌─────────────────────────────────────────────────────┐
│              scripts/run_champion_pipeline.py        │
│                   一键冠军流水线                      │
├─────────────┬───────────────────┬───────────────────┤
│  Step 1     │    Step 2         │    Step 3          │
│ 优化器增强   │  冠军策略生成      │  股价更新+选股     │
│             │                   │                    │
│ FactorComb  │ champion.yaml     │ update_kline +     │
│ inationOpt  │ → run_strategy    │ run_strategy       │
│ imizer(增强) │                   │                    │
└─────────────┴───────────────────┴───────────────────┘
```

## 四、Step 1 — 优化器增强

### 改动文件
- `optimization/factor_combination_optimizer.py`

### 核心改动
1. **全量股票加载**：移除 `sample_files[:500]` 限制
2. **回测性能优化**：
   - 因子计算结果缓存
   - 批量日期筛选（Polars groupby_dynamic）
   - 选股采样优化（每轮200只候选）
3. **适应度函数增强**：
   - 增加 Calmar Ratio
   - 增加交易成本模拟（印花税0.1% + 佣金0.025%）

## 五、Step 2 — 冠军策略生成

### 输出
- `config/strategies/champion.yaml`（标准策略YAML格式，兼容 StrategyEngine）

### 改动
- `optimization/factor_combination_optimizer.py` 的 `_save_results` 方法

## 六、Step 3 — 股价更新 + 选股

### 新增文件
- `scripts/update_kline_today.py`

### 功能
1. 通过 akshare API 拉取所有股票最新交易日K线
2. 增量更新 `data/kline/*.parquet`
3. 读取前一天选股结果 `reports/strategy_result.json`
4. 查询最新收盘价、涨跌幅
5. 输出更新后的选股报告

## 七、一键流水线

### 新增文件
- `scripts/run_champion_pipeline.py`

### 流程
1. 运行优化器（种群50，迭代30代，全量股票）
2. 生成 champion.yaml
3. 用冠军策略运行选股
4. 更新Parquet数据
5. 输出最终报告

## 八、文件改动清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `optimization/factor_combination_optimizer.py` | 修改 | 全量加载、性能优化、输出格式兼容 |
| `scripts/update_kline_today.py` | 新增 | 增量更新Parquet + 选股结果更新 |
| `scripts/run_champion_pipeline.py` | 新增 | 一键流水线 |
| `config/strategies/champion.yaml` | 自动生成 | 冠军策略配置 |
