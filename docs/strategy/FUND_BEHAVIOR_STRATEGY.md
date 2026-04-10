# 资金行为学策略 - 详细设计文档

## 概述

资金行为学策略是一个基于市场情绪、筹码分布和价格行为的量化选股系统。策略分为三个核心层次：**因子层**（数据特征）、**指标层**（逻辑阈值）、**策略层**（执行算法）。

## 系统架构 - 断点流水线

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    资金行为学策略执行脚本 (run_fund_behavior_strategy.py)              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  阶段        组件                      职责                    断点文件               │
│  ────────    ────────────────          ──────────────           ──────────────       │
│  1. 加载     load_data()              批量加载K线数据          *_load.parquet       │
│  2. 验证     validate_data()          数据质量检查             *_validate.json       │
│  3. 变换     calculate_factors()       计算市场因子             *_factor.parquet     │
│  4. 暂存     buffer_factors()         因子持久化               *_factor.parquet     │
│  5. 执行     execute_strategy()        策略信号生成             *_execute.json       │
│  6. 分发     distribute_results()      MySQL/HTML/邮件         *_distribute.json   │
│                                                                                     │
│  【断点机制】                                                                      │
│  - 每个阶段完成后保存状态到 JSON 文件                                                │
│  - 支持从断点恢复，跳过已完成的阶段                                                  │
│  - 使用 --reset 参数强制从头开始                                                     │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          因子层 (FactorEngine)                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  v_ratio10 │ v_total │ cost_peak │ limit_up_score │ pioneer_status │ ma5_bias     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         指标层 (IndicatorEngine)                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  market_sentiment │ 10am_pivot │ exit_lines                                        │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         策略层 (StrategyEngine)                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  select_trend_stocks │ select_short_term_stocks │ four_step_exit                   │
│  calculate_hedge_effect │ calculate_position_size                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## 状态流转图

```
start → loaded → validated → transformed → buffered → executed → distributed → done
        │           │            │            │            │            │
        ↓           ↓            ↓            ↓            ↓            ↓
      [checkpoint] [checkpoint] [checkpoint] [checkpoint] [checkpoint] [checkpoint]
```

## 断点文件存储位置

```
data/checkpoints/
  ├── fund_behavior_2026-04-09_load.json
  ├── fund_behavior_2026-04-09_validate.json
  ├── fund_behavior_2026-04-09_transform.json
  ├── fund_behavior_2026-04-09_factor.parquet
  ├── fund_behavior_2026-04-09_execute.json
  └── fund_behavior_2026-04-09_distribute.json
```

## 一、因子系统

### 1.1 因子列表

| 因子名称 | 计算公式 | 数据来源 | 用途 |
|----------|----------|----------|------|
| **v_ratio10** | volume / volume.shift(1) | volume | 早盘动能爆发识别 |
| **v_total** | Σ(volume × close) / 1e8 | volume, close | 市场整体承载力 |
| **cost_peak** | 筹码分布峰值 | close | 筹码荣枯判定 |
| **limit_up_score** | 涨停-跌停+连板高度 | close, open | 情绪得分 |
| **pioneer_status** | mean((close/open - 1) × 100) | close, open | 领涨股状态 |
| **ma5_bias** | (close - ma5) / ma5 | close | 短线超买超卖 |

### 1.2 因子配置

```yaml
factors:
  v_ratio10:
    enabled: true
    category: volume_price
  v_total:
    enabled: true
    category: market
  cost_peak:
    enabled: true
    category: market
  limit_up_score:
    enabled: true
    category: market
  pioneer_status:
    enabled: true
    category: market
  ma5_bias:
    enabled: true
    category: technical
```

### 1.3 因子计算流程

```
输入数据 (200行 × 8列)
    ↓
FactorEngine.calculate_all_factors()
    ↓
for each factor_name:
    factor = get_factor(factor_name)
    data = factor.calculate(data)
    ↓
输出数据 (200行 × 20列)  # 新增6个factor_xxx列
```

## 二、指标系统

### 2.1 市场情绪指标 (calculate_market_sentiment)

**输入列:** factor_v_total, factor_limit_up_score, factor_pioneer_status, factor_cost_peak, close, open

**处理步骤:**

| 步骤 | 计算内容 | 公式 |
|------|----------|------|
| 1 | 分组聚合 | 按 trade_date 分组计算均值 |
| 2 | 情绪温度 | (avg_limit_up_score + 50) / 100, clip(0, 100) |
| 3 | 温度变化 | sentiment - prev_sentiment |
| 4 | 先锋修正 | pioneer < -5 → multiplier=0.5, else 1.0 |
| 5 | 调整温度 | temperature × multiplier |
| 6 | 惯性信号 | delta_temp < -30 AND open > expected_drop → "Inertia_Sell" |

**市场状态判定:**

```
pioneer < -5 OR temp > 80  →  "risk"
v_total > 2.85 AND temp > 50  →  "strong"
2.5 < v_total < 2.8 AND cost_peak > avg_mean × 0.995  →  "oscillating"
其他  →  "weak"
```

**输出列:** trade_date, avg_v_total, avg_limit_up_score, avg_pioneer_status, avg_cost_peak, sentiment_temperature, adjusted_temperature, delta_temperature, market_state

### 2.2 10点定基调 (calculate_10am_pivot)

**输入列:** factor_v_ratio10, close, factor_ma5_bias

**判定条件:**

| 条件 | 阈值 | 含义 |
|------|------|------|
| v_ratio10 > 1.1 | 早盘放量1.1倍 | 放量信号 |
| close > 4081 | 价格站上4081 | 价格强势 |
| ma5_bias > 0 | MA5之上 | 趋势向上 |

**三者同时满足 → upward_pivot = True**

### 2.3 减仓线 (calculate_exit_lines)

**输入列:** volume, close, open

| 计算项 | 公式 | 含义 |
|--------|------|------|
| VWAP | Σ(volume×price) / Σ(volume) | 均价线 |
| 预期线 | close/open > 1.095 | 封板信号 |
| 均价线 | close < vwap | 破黄线 |
| 收盘线 | 封板 OR 翻绿 | 收盘条件 |

## 三、策略执行

### 3.1 主流程 (execute_strategy)

```python
def execute_strategy(data, total_capital, current_time):
    # 1. 计算指标
    indicators = calculate_all_indicators(data)

    # 2. 获取10点定基调
    upward_pivot = indicators["10am_pivot"]["upward_pivot"][-1]

    # 3. 计算对冲效果
    hedge_effect = calculate_hedge_effect(data)

    # 4. 选股
    trend_stocks = select_trend_stocks(data)
    short_term_stocks = select_short_term_stocks(data, upward_pivot)

    # 5. 计算仓位
    position_size = calculate_position_size(total_capital)

    # 6. 生成减仓信号
    exit_signals = {}
    for code in short_term_stocks:
        exit_signals[code] = four_step_exit_strategy(stock_data, current_time)

    # 7. 筹码荣枯线判定
    is_strong_region = current_price > cost_peak * 0.995

    return {
        "market_state": ...,
        "upward_pivot": ...,
        "hedge_effect": ...,
        "is_strong_region": ...,
        "trend_stocks": ...,
        "short_term_stocks": ...,
        "position_size": ...,
        "exit_signals": ...
    }
```

### 3.2 波段趋势选股 (select_trend_stocks)

```
筛选条件: factor_ma5_bias > 0
排序方式: 按 MA5偏差率降序
```

### 3.3 短线打板选股 (select_short_term_stocks)

```
前置条件: upward_pivot == True
筛选条件: factor_limit_up_score > 0
排序方式: 按情绪得分降序

如果 10点定基调向下 → 返回空列表 []
```

### 3.4 对冲效果 (calculate_hedge_effect)

```
条件: v_ratio10 > 1.1 AND price > 4067 AND v_total >= 2.8万亿
```

### 3.5 仓位计算 (calculate_position_size)

```yaml
strategy:
  position:
    trend: 0.5      # 50% 波段
    short_term: 0.4 # 40% 短线
    cash: 0.1       # 10% 现金
```

### 3.6 四步取关法 (four_step_exit_strategy)

| 时间点 | 条件 | 动作 | 减仓比例 |
|--------|------|------|----------|
| 09:26 | 未封一字板 | 撤1/4 | 25% |
| 盘中 | 跌破VWAP | 撤1/4 | 25% |
| 10:00 | 未涨停/炸板 | 撤1/4 | 25% |
| 14:56 | 未封板或翻绿 | 清仓 | 100% |

## 四、过程监控

### 4.1 日志监控点

| 监控点 | 文件位置 | 日志级别 | 输出内容 |
|--------|----------|----------|----------|
| 配置加载 | run_fund_behavior_strategy.py | INFO | 成功加载配置文件 |
| 数据加载 | run_fund_behavior_strategy.py | INFO | 数据形状 |
| 因子计算 | factor_engine.py | INFO | 加载了 N 个因子配置 |
| 因子注册 | factor_library.py | INFO | 注册因子: xxx |
| 因子计算完成 | run_fund_behavior_strategy.py | INFO | 数据形状 (200, 20) |
| 波段选股 | fund_behavior_strategy.py | INFO | 波段趋势选股: N 只 |
| 短线选股 | fund_behavior_strategy.py | INFO | 短线打板选股: N 只 |
| 策略结果 | run_fund_behavior_strategy.py | INFO | 全部结果字段 |

### 4.2 日志输出示例

```
2026-03-31 12:59:19,697 - core.factor_engine - INFO - 加载了 32 个因子配置
2026-03-31 12:59:19,698 - __main__ - INFO - 计算因子: ['v_ratio10', 'v_total', ...]
2026-03-31 12:59:19,797 - __main__ - INFO - 因子计算完成，数据形状: (200, 20)
2026-03-31 12:59:19,797 - __main__ - INFO - 执行策略...
2026-03-31 12:59:46,151 - ... - INFO - 波段趋势选股: 2 只股票
2026-03-31 12:59:46,152 - ... - INFO - 10点定基调向下，不进行短线选股
2026-03-31 12:59:47,557 - __main__ - INFO -
策略执行结果:
2026-03-31 12:59:47,557 - __main__ - INFO - 市场状态: ['weak', ...]
2026-03-31 12:59:47,558 - __main__ - INFO - 向上变盘信号: False
2026-03-31 12:59:47,558 - __main__ - INFO - 对冲效果: False
2026-03-31 12:59:47,558 - __main__ - INFO - 强势区域: True
```

## 五、结果验证

### 5.1 验证清单

| 验证项 | 预期结果 | 实际结果 |
|--------|----------|----------|
| 因子数量 | 6 | 6 |
| 数据形状 | (200, 20) | (200, 20) |
| 市场状态 | 非空列表 | ['weak', ...] |
| 向上变盘 | bool | False |
| 对冲效果 | bool | False |
| 强势区域 | bool | True |
| 波段股票 | list | ['000001', '300255'] |
| 短线股票 | list | [] (10点定基调向下) |
| 仓位分配 | dict | {trend: 50万, short_term: 40万, cash: 10万} |

### 5.2 数据流验证

```
输入: 200行 (2只股票 × 100天)
  ↓
因子计算: 6个因子 → 20列
  ↓
指标计算: 3个指标
  ↓
策略执行: 7个结果字段
  ↓
输出验证: 所有字段非空
```

### 5.3 配置验证

```python
# 验证配置读取
config_manager.get('strategy.position.trend')        # 期望: 0.5
config_manager.get('strategy.position.short_term')   # 期望: 0.4
config_manager.get('indicators.hedge.support_level') # 期望: 4067
config_manager.get('indicators.10am_pivot.v_ratio10_threshold')  # 期望: 1.1
```

## 六、配置文件结构

```yaml
# config/fund_behavior_config.yaml

factors:
  v_ratio10: {enabled: true, category: volume_price}
  v_total: {enabled: true, category: market}
  cost_peak: {enabled: true, category: market}
  limit_up_score: {enabled: true, category: market}
  pioneer_status: {enabled: true, category: market}
  ma5_bias: {enabled: true, category: technical}

indicators:
  market_sentiment:
    thresholds:
      strong_v_total: 2.85
      oscillating_v_total_min: 2.5
      oscillating_v_total_max: 2.8
      sentiment_temperature_strong: 50
      sentiment_temperature_overheat: 80
      cost_peak_support: 0.995
  10am_pivot:
    v_ratio10_threshold: 1.1
    price_threshold: 4081
  hedge:
    support_level: 4067
    v_total_threshold: 2.8

strategy:
  position:
    trend: 0.5
    short_term: 0.4
    cash: 0.1
  four_step_exit:
    time_points: ["09:26", "10:00", "14:56"]

backtest:
  initial_capital: 1000000
```

## 七、执行命令

```bash
# 基本执行
python scripts/run_fund_behavior_strategy.py

# 指定数据文件
python scripts/run_fund_behavior_strategy.py --data /path/to/data.parquet

# 指定资金
python scripts/run_fund_behavior_strategy.py --capital 500000

# 指定配置
python scripts/run_fund_behavior_strategy.py --config config/custom.yaml
```

## 八、技术要点

### 8.1 Polars API 注意事项

| 旧API (Polars < 1.0) | 新API (Polars >= 1.0) |
|----------------------|----------------------|
| `groupby()` | `group_by()` |
| `apply()` | `map_elements()` |
| `pl.String` | `pl.Utf8` |

### 8.2 因子注册机制

```python
@register_factor("v_ratio10")
class VRatio10Factor(BaseFactor):
    ...
```

通过装饰器注册到 `FactorRegistry`，实现动态发现。

### 8.3 模拟数据说明

当前使用模拟数据进行测试，实际使用需要替换 `load_data()` 函数从真实数据源加载 Parquet 文件。

---

**文档版本:** v1.0
**最后更新:** 2026-03-31