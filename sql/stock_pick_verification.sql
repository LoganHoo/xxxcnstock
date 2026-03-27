-- =============================================================================
-- 股票推荐验证系统 - 数据库建表脚本
-- 创建日期: 2026-03-25
-- 数据库: xcn_db
-- =============================================================================

-- 1. 推荐主表
CREATE TABLE IF NOT EXISTS stock_recommendation (
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

-- 2. 跟踪明细表
CREATE TABLE IF NOT EXISTS stock_pick_tracking (
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
    
    CONSTRAINT fk_recommend_id FOREIGN KEY (recommend_id) 
        REFERENCES stock_recommendation(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票推荐跟踪明细表';

-- =============================================================================
-- 常用查询视图
-- =============================================================================

-- 推荐收益汇总视图
CREATE OR REPLACE VIEW v_recommend_summary AS
SELECT 
    r.id,
    r.recommend_date,
    r.code,
    r.name,
    r.grade,
    r.score,
    r.recommend_price,
    r.status,
    r.max_profit,
    r.max_loss,
    r.final_profit,
    r.best_day,
    r.worst_day,
    r.stop_reason,
    COUNT(t.id) as tracked_days
FROM stock_recommendation r
LEFT JOIN stock_pick_tracking t ON r.id = t.recommend_id
GROUP BY r.id;

-- 按日期统计视图
CREATE OR REPLACE VIEW v_daily_stats AS
SELECT 
    recommend_date,
    COUNT(*) as total_picks,
    SUM(CASE WHEN grade = 'S' THEN 1 ELSE 0 END) as s_count,
    SUM(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) as a_count,
    AVG(final_profit) as avg_profit,
    MAX(max_profit) as best_profit,
    MIN(max_loss) as worst_loss,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) as win_count,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100 as win_rate
FROM stock_recommendation
WHERE status IN ('completed', 'stopped')
GROUP BY recommend_date
ORDER BY recommend_date DESC;

-- 按评级统计视图
CREATE OR REPLACE VIEW v_grade_stats AS
SELECT 
    grade,
    COUNT(*) as total_count,
    AVG(final_profit) as avg_profit,
    AVG(max_profit) as avg_max_profit,
    MAX(max_profit) as best_profit,
    MIN(max_loss) as worst_loss,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) as win_count,
    SUM(CASE WHEN final_profit > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100 as win_rate
FROM stock_recommendation
WHERE status IN ('completed', 'stopped')
GROUP BY grade;
