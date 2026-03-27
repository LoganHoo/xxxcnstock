"""
CVD (Cumulative Volume Delta) 累积成交量差指标计算

CVD 用于衡量市场买卖力量的累积差异：
- 正值：买方力量占优
- 负值：卖方力量占优

计算方法（无逐笔数据时的估算）：
1. K线实体法：delta = volume * (close - open) / (high - low)
2. 收盘位置法：delta = volume * (2 * (close - low) / (high - low) - 1)
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl
import duckdb
import logging
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class CVDCalculator:
    """CVD 指标计算器"""
    
    def __init__(self, kline_dir: str, output_path: str):
        self.kline_dir = Path(kline_dir)
        self.output_path = Path(output_path)
        self.conn = duckdb.connect()
        self.logger = logging.getLogger(__name__)
    
    def get_effective_date(self) -> str:
        """获取有效日期（15点前用昨天，15点后用今天）"""
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
            包含 CVD 指标的 DataFrame
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
                volume,
                close,
                open,
                high,
                low,
                body_ratio,
                close_position,
                volume * body_ratio as cvd_body,
                volume * (2 * close_position - 1) as cvd_position
            FROM base_data
        ),
        
        cvd_cumulative AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                cvd_body,
                cvd_position,
                SUM(cvd_body) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_body_cum,
                SUM(cvd_position) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_position_cum
            FROM cvd_daily
        ),
        
        latest AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                cvd_body,
                cvd_position,
                cvd_body_cum,
                cvd_position_cum,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
            FROM cvd_cumulative
        )
        
        SELECT 
            code,
            trade_date,
            close as price,
            volume,
            cvd_body,
            cvd_position,
            cvd_body_cum,
            cvd_position_cum,
            CASE 
                WHEN cvd_body_cum > 0 THEN 'buy_dominant'
                WHEN cvd_body_cum < 0 THEN 'sell_dominant'
                ELSE 'neutral'
            END as cvd_signal,
            cvd_body_cum * 1.0 / NULLIF(volume, 0) as cvd_ratio
        FROM latest
        WHERE rn = 1
        ORDER BY cvd_body_cum DESC
        """
        
        result = self.conn.execute(query).pl()
        self.logger.info(f"CVD 计算完成: {len(result)} 只股票")
        
        return result
    
    def calculate_cvd_trend(self, effective_date: str = None, lookback_days: int = 60) -> pl.DataFrame:
        """
        计算 CVD 趋势变化
        
        包括：
        - CVD 的 N 日变化
        - CVD 斜率
        - CVD 加速度（二阶导数）
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
                END as body_ratio
            FROM '{kline_pattern}'
            WHERE trade_date <= '{effective_date}'
        ),
        
        cvd_daily AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                volume * body_ratio as cvd_body
            FROM base_data
        ),
        
        cvd_cumulative AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                cvd_body,
                SUM(cvd_body) OVER (
                    PARTITION BY code 
                    ORDER BY trade_date 
                    ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW
                ) as cvd_cum
            FROM cvd_daily
        ),
        
        cvd_with_lags AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                cvd_body,
                cvd_cum,
                LAG(cvd_cum, 1) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_cum_prev1,
                LAG(cvd_cum, 5) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_cum_prev5,
                LAG(cvd_cum, 10) OVER (PARTITION BY code ORDER BY trade_date DESC) as cvd_cum_prev10,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
            FROM cvd_cumulative
        ),
        
        latest AS (
            SELECT 
                code,
                trade_date,
                close,
                volume,
                cvd_body,
                cvd_cum,
                cvd_cum_prev1,
                cvd_cum_prev5,
                cvd_cum_prev10
            FROM cvd_with_lags
            WHERE rn = 1
        )
        
        SELECT 
            code,
            trade_date,
            close as price,
            volume,
            cvd_body,
            cvd_cum as cvd_60d,
            cvd_cum_prev1 as cvd_60d_prev1,
            cvd_cum_prev5 as cvd_60d_prev5,
            cvd_cum_prev10 as cvd_60d_prev10,
            cvd_cum - COALESCE(cvd_cum_prev1, cvd_cum) as cvd_change_1d,
            cvd_cum - COALESCE(cvd_cum_prev5, cvd_cum) as cvd_change_5d,
            cvd_cum - COALESCE(cvd_cum_prev10, cvd_cum) as cvd_change_10d,
            CASE 
                WHEN cvd_cum > 0 AND (cvd_cum - COALESCE(cvd_cum_prev5, cvd_cum)) > 0 THEN 'strong_buy'
                WHEN cvd_cum > 0 THEN 'weak_buy'
                WHEN cvd_cum < 0 AND (cvd_cum - COALESCE(cvd_cum_prev5, cvd_cum)) < 0 THEN 'strong_sell'
                WHEN cvd_cum < 0 THEN 'weak_sell'
                ELSE 'neutral'
            END as cvd_trend
        FROM latest
        ORDER BY cvd_cum DESC
        """
        
        result = self.conn.execute(query).pl()
        self.logger.info(f"CVD 趋势计算完成: {len(result)} 只股票")
        
        return result
    
    def save_results(self, df: pl.DataFrame, filename: str = "cvd_latest.parquet"):
        """保存结果"""
        output_file = self.output_path.parent / filename
        df.write_parquet(str(output_file))
        self.logger.info(f"CVD 数据已保存: {output_file}")
    
    def run(self):
        """执行完整计算流程"""
        self.logger.info("="*60)
        self.logger.info("开始计算 CVD 指标")
        self.logger.info("="*60)
        
        effective_date = self.get_effective_date()
        self.logger.info(f"有效日期: {effective_date}")
        
        df_cvd = self.calculate_cvd(effective_date)
        df_trend = self.calculate_cvd_trend(effective_date)
        
        merged = df_cvd.join(
            df_trend.select(['code', 'cvd_60d_prev1', 'cvd_60d_prev5', 'cvd_60d_prev10',
                           'cvd_change_1d', 'cvd_change_5d', 'cvd_change_10d', 'cvd_trend']),
            on='code',
            how='left'
        )
        
        self.save_results(merged)
        
        self._print_summary(merged)
        
        return merged
    
    def _print_summary(self, df: pl.DataFrame):
        """打印汇总信息"""
        self.logger.info("\n" + "="*60)
        self.logger.info("CVD 指标汇总")
        self.logger.info("="*60)
        
        buy_count = len(df.filter(pl.col('cvd_body_cum') > 0))
        sell_count = len(df.filter(pl.col('cvd_body_cum') < 0))
        
        self.logger.info(f"买方占优: {buy_count} 只")
        self.logger.info(f"卖方占优: {sell_count} 只")
        
        strong_buy = len(df.filter(pl.col('cvd_trend') == 'strong_buy'))
        weak_buy = len(df.filter(pl.col('cvd_trend') == 'weak_buy'))
        strong_sell = len(df.filter(pl.col('cvd_trend') == 'strong_sell'))
        weak_sell = len(df.filter(pl.col('cvd_trend') == 'weak_sell'))
        
        self.logger.info(f"\n趋势分布:")
        self.logger.info(f"  强势买入: {strong_buy} 只")
        self.logger.info(f"  弱势买入: {weak_buy} 只")
        self.logger.info(f"  强势卖出: {strong_sell} 只")
        self.logger.info(f"  弱势卖出: {weak_sell} 只")
        
        self.logger.info(f"\nCVD Top 10 (买方最强):")
        top_buy = df.sort('cvd_body_cum', descending=True).head(10)
        for row in top_buy.iter_rows(named=True):
            self.logger.info(f"  {row['code']}: CVD={row['cvd_body_cum']:,.0f} 趋势={row['cvd_trend']}")
        
        self.logger.info(f"\nCVD Bottom 10 (卖方最强):")
        top_sell = df.sort('cvd_body_cum', descending=False).head(10)
        for row in top_sell.iter_rows(named=True):
            self.logger.info(f"  {row['code']}: CVD={row['cvd_body_cum']:,.0f} 趋势={row['cvd_trend']}")


def main():
    """主函数"""
    PROJECT_ROOT = Path(__file__).parent.parent
    KLINE_DIR = PROJECT_ROOT / "data" / "kline"
    OUTPUT_PATH = PROJECT_ROOT / "data" / "cvd_latest.parquet"
    
    calculator = CVDCalculator(str(KLINE_DIR), str(OUTPUT_PATH))
    calculator.run()


if __name__ == '__main__':
    main()
