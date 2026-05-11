# 漏斗选股系统设计

## 概述

5层漏斗选股系统，每层过滤15%，最后AI综合评分生成持仓建议，输出到 `daily_prediction` 表。

## 架构

```
全市场 ~4000 只
    ↓ 第1层：基础过滤（ST/退市/停牌/涨跌停） 保留85%
3400 只
    ↓ 第2层：基本面过滤（市值/PE/换手率） 保留85%
2890 只
    ↓ 第3层：技术面过滤（均线/量比/MACD） 保留85%
2456 只
    ↓ 第4层：资金+板块过滤（主力净流入/板块） 保留85%
2088 只
    ↓ 第5层：特殊形态（涨停基因/突破/回调） 保留85%
1775 只
    ↓ AI综合评分 + 持仓建议
最终输出 → daily_prediction 表
```

## 各层详细设计

### 第1层：基础过滤

**条件**：
- 排除 ST/*ST 股票
- 排除 退市 股票
- 排除 停牌（成交量=0）
- 排除 涨停（涨幅=10%）
- 排除 跌停（涨幅=-10%）

**输出**：3400只股票 + 每只的基础评分

### 第2层：基本面过滤

**条件**：
- 换手率：3% ~ 15%
- 市值：50亿 ~ 500亿
- PE：0 ~ 60（排除亏损股）

**输出**：2890只股票

### 第3层：技术面过滤

**条件**：
- 均线多头排列（5日 > 10日 > 20日）
- 量比 > 1.5
- MACD 金叉 或 红柱

**输出**：2456只股票

### 第4层：资金+板块过滤

**条件**：
- 主力资金净流入 > 0
- 所在板块涨停数 > 1

**输出**：2088只股票

### 第5层：特殊形态

**条件**：
- 涨停基因：20日内有涨停记录
- 突破形态：盘中突破20日高点
- 回调形态：回踩5日均线获得支撑

**输出**：1775只股票

### AI综合评分层

**输入**：5层漏斗筛选后的1775只股票

**AI任务**：
1. 综合各维度评分
2. 生成持仓建议：
   - 买入价（收盘价 ± 2%）
   - 止损价（买入价 - 5%）
   - 止盈1（买入价 + 10%）
   - 止盈2（买入价 + 20%）
   - 关键支撑位
   - 关键压力位

**输出**：最终推荐股票池写入 `daily_prediction` 表

## 数据模型

### daily_prediction 表更新

新增字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| funnel_score | Float | 漏斗综合评分 |
| layer1_score | Float | 基础过滤评分 |
| layer2_score | Float | 基本面评分 |
| layer3_score | Float | 技术面评分 |
| layer4_score | Float | 资金面评分 |
| layer5_score | Float | 形态评分 |
| ai_score | Float | AI综合评分 |
| entry_price | Float | 建议买入价 |
| stoploss_price | Float | 止损价 |
| take_profit_1 | Float | 止盈1 |
| take_profit_2 | Float | 止盈2 |
| support_price | Float | 关键支撑位 |
| resistance_price | Float | 关键压力位 |

## 实现文件

- `services/stock_service/funnel_selector.py` - 漏斗选股核心类
- `services/ai/funnel_ai_scorer.py` - AI评分模块
- `scripts/pipeline/funnel_stock_pick.py` - 执行脚本

## 复用现有组件

- `filters/market_filter.py` - 第1层基础过滤
- `filters/valuation_filter.py` - 第2层基本面过滤
- `filters/technical_filter.py` - 第3层技术面过滤
- `filters/market_behavior_filter.py` - 第4层资金过滤
- `services/limit_service/analyzers/pre_limit.py` - AI评分
