"""
关键位与CVD指标计算 - 优化版

基于 Fabio Valentini 策略实现：

一、关键位识别方法：
1. 供需区域 (Supply and Demand Zones)：价格曾出现剧烈反应或转向的区域
2. 箱体结构 (Box Structures)：窄幅区间波动（盘整区）
3. 大额订单位置：大额成交量指标辅助确认

二、CVD (Cumulative Volume Delta) 计算逻辑：
1. 基础数据：买家发起的成交量与卖家发起的成交量差异
2. 计算方式：累积计算 Delta
3. 核心用途：观察大资金意图、捕捉背离
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import duckdb
import polars as pl
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.freshness_check_decorator import check_data_freshness


class KeyLevelsCalculator:
    """
    关键位计算器
    
    实现三种关键位识别方法：
    1. 供需区域：基于价格剧烈反应区域
    2. 箱体结构：识别窄幅区间波动
    3. 大额订单位置：成交量集中区域
    """
    
    def __init__(self, kline_dir: str, output_dir: str, stock_list_path: str = None):
        self.kline_dir = Path(kline_dir)
        self.output_dir = Path(output_dir)
        self.stock_list_path = Path(stock_list_path) if stock_list_path else None
        self.conn = duckdb.connect(":memory:")
        self.logger = logging.getLogger(__name__)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_effective_date(self) -> str:
        """获取有效日期（15点前用昨天，15点后用今天）"""
        now = datetime.now()
        if now.hour < 15:
            effective_date = now - timedelta(days=1)
        else:
            effective_date = now
        return effective_date.strftime('%Y-%m-%d')
    
    def calculate_all(self, effective_date: str = None) -> pl.DataFrame:
        """
        计算所有关键位
        
        Returns:
            包含供需区域、箱体结构、大额订单位置的DataFrame
        """
        if effective_date is None:
            effective_date = self.get_effective_date()
        
        kline_pattern = str(self.kline_dir / "*.parquet")
        
        query = f"""
        WITH base_data AS (
            SELECT 
                code,
                trade_date,
                open,
                close,
                high,
                low,
                volume,
                ABS(close - open) as body,
                high - low as range,
                CASE WHEN high - low > 0 THEN ABS(close - open) * 1.0 / (high - low) ELSE 0 END as body_ratio,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
            FROM '{kline_pattern}'
            WHERE trade_date <= '{effective_date}'
        ),
        
        latest AS (
            SELECT * FROM base_data WHERE rn = 1
        ),
        
        stats_5d AS (
            SELECT 
                code,
                AVG(close) as ma5,
                AVG(volume) as vol_ma5,
                MIN(low) as low_5d,
                MAX(high) as high_5d,
                SUM(volume) as total_vol_5d
            FROM base_data WHERE rn <= 5 GROUP BY code
        ),
        
        stats_20d AS (
            SELECT 
                code,
                AVG(close) as ma20,
                STDDEV(close) as std_20,
                MIN(low) as low_20d,
                MAX(high) as high_20d,
                AVG(volume) as vol_ma20,
                AVG(ABS(close - open)) as avg_body_20d,
                AVG(high - low) as avg_range_20d
            FROM base_data WHERE rn <= 20 GROUP BY code
        ),
        
        stats_60d AS (
            SELECT 
                code,
                AVG(close) as ma60,
                MIN(low) as low_60d,
                MAX(high) as high_60d,
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY low) as support_percentile,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY high) as resistance_percentile,
                AVG(volume) as vol_ma60,
                SUM(volume) as total_vol_60d
            FROM base_data WHERE rn <= 60 GROUP BY code
        ),
        
        supply_demand_zones AS (
            SELECT 
                code,
                MIN(CASE WHEN body_ratio > 0.7 AND ABS((close - open) / NULLIF(open, 0)) > 0.03 THEN low END) as demand_zone,
                MAX(CASE WHEN body_ratio > 0.7 AND ABS((close - open) / NULLIF(open, 0)) > 0.03 THEN high END) as supply_zone,
                AVG(CASE WHEN body_ratio > 0.6 THEN volume END) as strong_candle_vol
            FROM base_data WHERE rn <= 60 GROUP BY code
        ),
        
        box_structures AS (
            SELECT 
                code,
                CASE 
                    WHEN STDDEV(close) / NULLIF(AVG(close), 0) < 0.05 THEN 1
                    ELSE 0
                END as is_consolidating,
                AVG(high - low) as avg_daily_range,
                STDDEV(high - low) as range_std
            FROM base_data WHERE rn <= 20 GROUP BY code
        ),
        
        volume_clusters AS (
            SELECT 
                code,
                AVG(volume) * 1.5 as high_vol_threshold
            FROM base_data WHERE rn <= 60 GROUP BY code
        ),
        
        prev_day AS (
            SELECT * FROM base_data WHERE rn = 2
        )
        
        SELECT 
            l.code,
            l.trade_date,
            l.open,
            l.close as price,
            l.high,
            l.low,
            l.volume,
            
            COALESCE(s5.ma5, l.close) as ma5,
            COALESCE(s20.ma20, l.close) as ma10,
            COALESCE(s20.ma20, l.close) as ma20,
            COALESCE(s60.ma60, l.close) as ma60,
            
            COALESCE(s5.low_5d, l.low) as support_5d,
            COALESCE(s20.low_20d, l.low) as support_20d,
            COALESCE(s60.low_60d, l.low) as support_60d,
            COALESCE(s60.support_percentile, l.low) as support_strong,
            
            COALESCE(s5.high_5d, l.high) as resistance_5d,
            COALESCE(s20.high_20d, l.high) as resistance_20d,
            COALESCE(s60.high_60d, l.high) as resistance_60d,
            COALESCE(s60.resistance_percentile, l.high) as resistance_strong,
            
            COALESCE(sd.demand_zone, l.low) as demand_zone,
            COALESCE(sd.supply_zone, l.high) as supply_zone,
            COALESCE(sd.strong_candle_vol, l.volume) as strong_candle_vol,
            
            COALESCE(bs.is_consolidating, 0) as is_consolidating,
            COALESCE(bs.avg_daily_range, l.high - l.low) as avg_daily_range,
            
            COALESCE(s20.std_20, 0) as std_20,
            COALESCE(s20.ma20 + 2 * s20.std_20, l.high) as bb_upper,
            COALESCE(s20.ma20 - 2 * s20.std_20, l.low) as bb_lower,
            
            (l.high + l.low + l.close) / 3.0 as pivot,
            2.0 * (l.high + l.low + l.close) / 3.0 - l.low as pivot_r1,
            2.0 * (l.high + l.low + l.close) / 3.0 - l.high as pivot_s1,
            
            p.high as prev_high,
            p.low as prev_low,
            p.close as prev_close,
            
            (l.close - p.close) / NULLIF(p.close, 0) * 100.0 as change_pct,
            
            l.volume * 1.0 / NULLIF(s5.vol_ma5, 0) as vol_ratio
            
        FROM latest l
        LEFT JOIN stats_5d s5 ON l.code = s5.code
        LEFT JOIN stats_20d s20 ON l.code = s20.code
        LEFT JOIN stats_60d s60 ON l.code = s60.code
        LEFT JOIN supply_demand_zones sd ON l.code = sd.code
        LEFT JOIN box_structures bs ON l.code = bs.code
        LEFT JOIN prev_day p ON l.code = p.code
        ORDER BY l.code
        """
        
        result = self.conn.execute(query).pl()
        self.logger.info(f"关键位计算完成: {len(result)} 只股票")
        
        return result
    
    def add_stock_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """关联股票名称"""
        if self.stock_list_path and self.stock_list_path.exists():
            try:
                stock_list = pl.read_parquet(str(self.stock_list_path))
                if 'code' in stock_list.columns and 'name' in stock_list.columns:
                    df = df.join(
                        stock_list.select(['code', 'name']),
                        on='code',
                        how='left'
                    )
            except Exception as e:
                self.logger.warning(f"关联股票名称失败: {e}")
        
        if 'name' not in df.columns:
            df = df.with_columns(pl.lit('').alias('name'))
        
        return df
    
    def save_results(self, df: pl.DataFrame, effective_date: str):
        """保存结果"""
        output_file = self.output_dir / "key_levels_latest.parquet"
        df.write_parquet(str(output_file))
        self.logger.info(f"关键位数据已保存: {output_file}")


class CVDCalculator:
    """
    CVD (Cumulative Volume Delta) 计算器
    
    实现逻辑：
    1. 基础数据：估算主动买入量与主动卖出量的差异
       - K线实体法：delta = volume × (close - open) / (high - low)
       - 收盘位置法：delta = volume × (2 × (close - low) / (high - low) - 1)
    
    2. 累积计算：CVD = 前一周期 CVD + Delta
    
    3. 背离检测：
       - 顶背离：价格上涨但CVD下跌（卖压累积）
       - 底背离：价格下跌但CVD上涨（买压累积）
    """
    
    def __init__(self, kline_dir: str, output_dir: str):
        self.kline_dir = Path(kline_dir)
        self.output_dir = Path(output_dir)
        self.conn = duckdb.connect(":memory:")
        self.logger = logging.getLogger(__name__)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_effective_date(self) -> str:
        """获取有效日期"""
        now = datetime.now()
        if now.hour < 15:
            effective_date = now - timedelta(days=1)
        else:
            effective_date = now
        return effective_date.strftime('%Y-%m-%d')
    
    def calculate_cvd(self, effective_date: str = None, lookback_days: int = 60) -> pl.DataFrame:
        """
        计算 CVD 指标
        
        Args:
            effective_date: 有效日期
            lookback_days: 回溯天数
        
        Returns:
            包含 CVD 指标和背离信号的 DataFrame
        """
        if effective_date is None:
            effective_date = self.get_effective_date()
        
        kline_pattern = str(self.kline_dir / "*.parquet")
        
        query = f"""
        WITH base_data AS (
            SELECT 
                code,
                trade_date,
                open,
                close,
                high,
                low,
                volume,
                CASE 
                    WHEN high = low THEN 0
                    ELSE (close - open) * 1.0 / (high - low)
                END as body_ratio,
                CASE 
                    WHEN high = low THEN 0.5
                    ELSE (close - low) * 1.0 / (high - low)
                END as close_position
            FROM '{kline_pattern}'
            WHERE trade_date <= '{effective_date}'
        ),
        
        cvd_daily AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                volume * body_ratio as delta_body,
                volume * (2 * close_position - 1) as delta_position,
                (volume * body_ratio + volume * (2 * close_position - 1)) / 2.0 as delta_combined
            FROM base_data
        ),
        
        cvd_cumulative AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                delta_body,
                delta_position,
                delta_combined,
                SUM(delta_body) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_body_cum,
                SUM(delta_position) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_position_cum,
                SUM(delta_combined) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_combined_cum
            FROM cvd_daily
        ),
        
        cvd_with_trend AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                delta_body,
                cvd_body_cum,
                cvd_position_cum,
                cvd_combined_cum,
                LAG(cvd_body_cum, 1) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_prev1,
                LAG(cvd_body_cum, 5) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_prev5,
                LAG(cvd_body_cum, 10) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_prev10,
                LAG(close, 5) OVER (PARTITION BY code ORDER BY trade_date DESC) as close_prev5,
                LAG(close, 10) OVER (PARTITION BY code ORDER BY trade_date DESC) as close_prev10,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
            FROM cvd_cumulative
        ),
        
        latest AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                delta_body,
                cvd_body_cum,
                cvd_position_cum,
                cvd_combined_cum,
                cvd_prev1,
                cvd_prev5,
                cvd_prev10,
                close_prev5,
                close_prev10
            FROM cvd_with_trend
            WHERE rn = 1
        )
        
        SELECT 
            code,
            trade_date,
            close as price,
            volume,
            delta_body,
            cvd_body_cum as cvd_60d,
            cvd_position_cum,
            cvd_combined_cum,
            cvd_prev1 as cvd_60d_prev1,
            cvd_prev5 as cvd_60d_prev5,
            cvd_prev10 as cvd_60d_prev10,
            cvd_body_cum - COALESCE(cvd_prev1, cvd_body_cum) as cvd_change_1d,
            cvd_body_cum - COALESCE(cvd_prev5, cvd_body_cum) as cvd_change_5d,
            cvd_body_cum - COALESCE(cvd_prev10, cvd_body_cum) as cvd_change_10d,
            
            CASE 
                WHEN cvd_body_cum > 0 THEN 'buy_dominant'
                WHEN cvd_body_cum < 0 THEN 'sell_dominant'
                ELSE 'neutral'
            END as cvd_signal,
            
            CASE 
                WHEN cvd_body_cum > 0 AND (cvd_body_cum - COALESCE(cvd_prev5, cvd_body_cum)) > 0 THEN 'strong_buy'
                WHEN cvd_body_cum > 0 THEN 'weak_buy'
                WHEN cvd_body_cum < 0 AND (cvd_body_cum - COALESCE(cvd_prev5, cvd_body_cum)) < 0 THEN 'strong_sell'
                WHEN cvd_body_cum < 0 THEN 'weak_sell'
                ELSE 'neutral'
            END as cvd_trend,
            
            CASE 
                WHEN close > close_prev5 AND cvd_body_cum < cvd_prev5 THEN 'top_divergence'
                WHEN close < close_prev5 AND cvd_body_cum > cvd_prev5 THEN 'bottom_divergence'
                ELSE 'no_divergence'
            END as divergence_5d,
            
            CASE 
                WHEN close > close_prev10 AND cvd_body_cum < cvd_prev10 THEN 'top_divergence'
                WHEN close < close_prev10 AND cvd_body_cum > cvd_prev10 THEN 'bottom_divergence'
                ELSE 'no_divergence'
            END as divergence_10d
            
        FROM latest
        ORDER BY cvd_body_cum DESC
        """
        
        result = self.conn.execute(query).pl()
        self.logger.info(f"CVD 计算完成: {len(result)} 只股票")
        
        return result
    
    def save_results(self, df: pl.DataFrame):
        """保存结果"""
        output_file = self.output_dir / "cvd_latest.parquet"
        df.write_parquet(str(output_file))
        self.logger.info(f"CVD 数据已保存: {output_file}")
    
    def print_summary(self, df: pl.DataFrame):
        """打印汇总信息"""
        self.logger.info("\n" + "="*60)
        self.logger.info("CVD 指标汇总")
        self.logger.info("="*60)
        
        buy_count = len(df.filter(pl.col('cvd_60d') > 0))
        sell_count = len(df.filter(pl.col('cvd_60d') < 0))
        
        self.logger.info(f"买方占优: {buy_count} 只")
        self.logger.info(f"卖方占优: {sell_count} 只")
        
        top_div = len(df.filter(pl.col('divergence_5d') == 'top_divergence'))
        bottom_div = len(df.filter(pl.col('divergence_5d') == 'bottom_divergence'))
        
        self.logger.info(f"\n背离信号:")
        self.logger.info(f"  顶背离 (价格上涨+CVD下跌): {top_div} 只")
        self.logger.info(f"  底背离 (价格下跌+CVD上涨): {bottom_div} 只")


@check_data_freshness
def main():
    """主函数"""
    PROJECT_ROOT = Path(__file__).parent.parent
    KLINE_DIR = PROJECT_ROOT / "data" / "kline"
    OUTPUT_DIR = PROJECT_ROOT / "data"
    STOCK_LIST_PATH = PROJECT_ROOT / "data" / "stock_list.parquet"
    
    logger.info("="*60)
    logger.info("开始计算关键位和CVD指标")
    logger.info("="*60)
    
    key_levels_calc = KeyLevelsCalculator(
        str(KLINE_DIR), str(OUTPUT_DIR), str(STOCK_LIST_PATH)
    )
    effective_date = key_levels_calc.get_effective_date()
    logger.info(f"有效日期: {effective_date}")
    
    logger.info("\n计算关键位...")
    key_levels_df = key_levels_calc.calculate_all(effective_date)
    key_levels_df = key_levels_calc.add_stock_names(key_levels_df)
    key_levels_calc.save_results(key_levels_df, effective_date)
    
    logger.info("\n计算CVD指标...")
    cvd_calc = CVDCalculator(str(KLINE_DIR), str(OUTPUT_DIR))
    cvd_df = cvd_calc.calculate_cvd(effective_date)
    cvd_calc.save_results(cvd_df)
    cvd_calc.print_summary(cvd_df)
    
    logger.info("\n" + "="*60)
    logger.info("计算完成")
    logger.info("="*60)


if __name__ == '__main__':
    main()
