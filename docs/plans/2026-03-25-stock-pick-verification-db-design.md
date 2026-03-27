# 股票推荐验证系统数据库设计

## 概述

设计用于跟踪股票推荐在未来30个交易日内的表现，支持每日价格更新和收益验证。

## 需求

1. **推荐记录**: 交易日当晚存入推荐股票
2. **验证周期**: 第2-30个交易日（共30天）
3. **每日更新**: 每天更新后续交易日的收盘价
4. **收益计算**: 计算涨跌幅和累计收益率

## 表结构

### 1. stock_recommendation - 推荐主表

存储每日推荐的股票信息和技术指标快照。

```sql
CREATE TABLE stock_recommendation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 推荐基本信息
    recommend_date DATE NOT NULL COMMENT '推荐日期(T日)',
    code VARCHAR(10) NOT NULL COMMENT '股票代码',
    name VARCHAR(50) COMMENT '股票名称',
    grade VARCHAR(5) NOT NULL COMMENT '评级(S/A/B/C)',
    score INT COMMENT '推荐评分',
    recommend_price DECIMAL(10,3) NOT NULL COMMENT '推荐日收盘价(T日)',
    recommend_change DECIMAL(8,2) COMMENT '推荐日涨跌幅%',
    
    -- 股票基本信息
    is_st TINYINT DEFAULT 0 COMMENT '是否ST股票(0否/1是)',
    industry VARCHAR(50) COMMENT '所属行业',
    market_cap DECIMAL(15,2) COMMENT '总市值(万元)',
    float_cap DECIMAL(15,2) COMMENT '流通市值(万元)',
    
    -- 技术指标快照
    support_strong DECIMAL(10,3) COMMENT '强支撑位',
    resistance_strong DECIMAL(10,3) COMMENT '强压力位',
    ma20 DECIMAL(10,3) COMMENT '20日均线',
    ma60 DECIMAL(10,3) COMMENT '60日均线',
    cvd_signal VARCHAR(20) COMMENT 'CVD信号(buy_dominant/sell_dominant/neutral)',
    reasons TEXT COMMENT '推荐理由(JSON数组)',
    
    -- 交易设置
    stop_loss_price DECIMAL(10,3) COMMENT '止损价',
    take_profit_price DECIMAL(10,3) COMMENT '止盈价',
    stop_loss_pct DECIMAL(5,2) DEFAULT -5.00 COMMENT '止损线(%，默认-5%)',
    take_profit_pct DECIMAL(5,2) DEFAULT 10.00 COMMENT '止盈线(%，默认+10%)',
    
    -- 统计结果
    max_profit DECIMAL(8,2) DEFAULT 0 COMMENT '最大收益%(T+1~T+30)',
    max_loss DECIMAL(8,2) DEFAULT 0 COMMENT '最大亏损%',
    final_profit DECIMAL(8,2) DEFAULT 0 COMMENT '最终收益%(T+30)',
    best_day INT DEFAULT 0 COMMENT '最佳收益日期(T+天数)',
    worst_day INT DEFAULT 0 COMMENT '最差收益日期',
    
    -- 状态跟踪
    status VARCHAR(20) DEFAULT 'tracking' COMMENT '状态(tracking/stopped/completed)',
    stop_reason VARCHAR(50) COMMENT '终止原因(loss_triggered/profit_triggered/manual/expired)',
    stopped_at DATE COMMENT '终止日期',
    
    -- 用户标记
    user_buy_date DATE COMMENT '用户实际买入日期',
    user_sell_date DATE COMMENT '用户实际卖出日期',
    user_notes TEXT COMMENT '用户备注',
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 索引
    UNIQUE KEY uk_recommend (recommend_date, code),
    INDEX idx_date (recommend_date),
    INDEX idx_code (code),
    INDEX idx_grade (grade),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票推荐主表';
```

### 2. stock_pick_tracking - 跟踪明细表

存储每个交易日的价格和收益数据。

```sql
CREATE TABLE stock_pick_tracking (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 关联信息
    recommend_id BIGINT NOT NULL COMMENT '推荐记录ID',
    recommend_date DATE NOT NULL COMMENT '推荐日期',
    code VARCHAR(10) NOT NULL COMMENT '股票代码',
    
    -- 跟踪信息
    track_day INT NOT NULL COMMENT '跟踪天数(1-30, T+1=1)',
    track_date DATE NOT NULL COMMENT '跟踪日期(交易日)',
    
    -- 价格数据
    open_price DECIMAL(10,3) COMMENT '开盘价',
    high_price DECIMAL(10,3) COMMENT '最高价',
    low_price DECIMAL(10,3) COMMENT '最低价',
    close_price DECIMAL(10,3) NOT NULL COMMENT '收盘价',
    prev_close_price DECIMAL(10,3) COMMENT '前一日收盘价',
    
    -- 涨跌数据
    daily_change DECIMAL(8,2) COMMENT '当日涨跌幅%',
    cumulative_profit DECIMAL(8,2) NOT NULL COMMENT '累计收益%(相对于推荐日)',
    
    -- 成交量数据
    volume BIGINT COMMENT '成交量(手)',
    amount DECIMAL(15,2) COMMENT '成交额(万元)',
    turnover_rate DECIMAL(8,2) COMMENT '换手率%',
    
    -- 交易信号
    signal_type VARCHAR(20) COMMENT '信号类型(buy/sell/hold/none)',
    signal_reason VARCHAR(100) COMMENT '信号原因',
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE KEY uk_track (recommend_id, track_day),
    INDEX idx_recommend_date (recommend_date),
    INDEX idx_code (code),
    INDEX idx_track_date (track_date),
    INDEX idx_cumulative_profit (cumulative_profit),
    FOREIGN KEY (recommend_id) REFERENCES stock_recommendation(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票推荐跟踪明细表';
```

## 设计说明

### 数据流程

```
T日(推荐日)
    │
    ├─> 存入 stock_recommendation (推荐信息 + 技术指标快照)
    │
    ├─> T+1~T+30 每日
    │       │
    │       ├─> 获取当日收盘价
    │       │
    │       └─> 更新 stock_pick_tracking (价格 + 收益)
    │           │
    │           └─> 检查是否触发止盈/止损
    │               │
    │               └─> 更新 stock_recommendation.status
    │
    └─> T+30 或 触发终止条件
            │
            └─> 标记为 completed/stopped
```

### 关键字段说明

| 字段 | 说明 |
|------|------|
| recommend_date | 推荐日期(T日) |
| track_day | 跟踪天数(1= T+1交易日) |
| cumulative_profit | 累计收益 = (当日收盘价 - 推荐日收盘价) / 推荐日收盘价 × 100% |
| stop_loss_pct | 默认-5%，可自定义 |
| take_profit_pct | 默认+10%，可自定义 |
| status | tracking=跟踪中, stopped=已终止, completed=已完成30天 |

### 触发条件

1. **止盈触发**: cumulative_profit >= take_profit_pct
2. **止损触发**: cumulative_profit <= stop_loss_pct  
3. **手动终止**: 用户手动标记卖出
4. **到期完成**: T+30交易日结束

## 使用示例

### 1. 插入推荐记录

```sql
INSERT INTO stock_recommendation (
    recommend_date, code, name, grade, score, 
    recommend_price, recommend_change, industry,
    support_strong, resistance_strong, ma20, ma60,
    cvd_signal, reasons
) VALUES (
    '2026-03-24', '000601', '韶能股份', 'S', 130,
    8.06, 9.96, '电力',
    4.75, 6.92, 6.43, 5.48,
    'buy_dominant', '["强势上涨","多头排列"]'
);
```

### 2. 每日更新跟踪数据

```sql
INSERT INTO stock_pick_tracking (
    recommend_id, recommend_date, code,
    track_day, track_date,
    open_price, high_price, low_price, close_price, prev_close_price,
    daily_change, cumulative_profit,
    volume, amount, turnover_rate
) VALUES (
    1, '2026-03-24', '000601',
    1, '2026-03-25',
    8.60, 8.87, 8.51, 8.87, 8.06,
    10.05, 10.05,
    2202378, 19360.5, 2.35
);
```

### 3. 查询收益统计

```sql
-- 按推荐日期统计
SELECT 
    recommend_date,
    COUNT(*) as total_picks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
    AVG(final_profit) as avg_profit,
    MAX(max_profit) as best_profit,
    MIN(max_loss) as worst_loss,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) as win_count
FROM stock_recommendation
GROUP BY recommend_date
ORDER BY recommend_date DESC;

-- 按评级统计
SELECT 
    grade,
    COUNT(*) as count,
    AVG(final_profit) as avg_profit,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100 as win_rate
FROM stock_recommendation
WHERE status = 'completed'
GROUP BY grade;
```
