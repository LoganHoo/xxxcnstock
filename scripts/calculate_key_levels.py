"""
关键位计算与追踪
使用 DuckDB + Parquet 计算每只股票的关键位，每日记录并对比变化
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


class KeyLevelsTracker:
    """关键位追踪器"""
    
    def __init__(self, kline_dir: str, output_dir: str, stock_list_path: str = None, cutoff_hour: int = 15):
        self.kline_dir = Path(kline_dir)
        self.output_dir = Path(output_dir)
        self.stock_list_path = Path(stock_list_path) if stock_list_path else None
        self.cutoff_hour = cutoff_hour
        self.conn = duckdb.connect(":memory:")
        self.logger = logging.getLogger(__name__)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.key_levels_dir = self.output_dir / "key_levels"
        self.key_levels_dir.mkdir(parents=True, exist_ok=True)
    
    def get_effective_date(self) -> str:
        """根据当前时间确定有效数据日期"""
        now = datetime.now()
        
        if now.hour < self.cutoff_hour:
            effective_date = now - timedelta(days=1)
        else:
            effective_date = now
        
        return effective_date.strftime('%Y-%m-%d')
    
    def run(self):
        """执行关键位计算"""
        self.logger.info("=" * 70)
        self.logger.info("开始计算股票关键位")
        self.logger.info("=" * 70)
        
        effective_date = self.get_effective_date()
        kline_pattern = str(self.kline_dir / "*.parquet")
        
        self.logger.info(f"有效数据日期: {effective_date}")
        
        self.logger.info("步骤1: 计算关键位...")
        key_levels_df = self._calculate_key_levels(kline_pattern, effective_date)
        
        if key_levels_df is None or len(key_levels_df) == 0:
            self.logger.error("未计算出关键位数据")
            return
        
        self.logger.info(f"计算完成: {len(key_levels_df)} 只股票")
        
        self.logger.info("步骤2: 关联股票名称...")
        key_levels_df = self._add_stock_names(key_levels_df)
        
        self.logger.info("步骤3: 对比历史变化...")
        key_levels_df = self._compare_with_history(key_levels_df, effective_date)
        
        self.logger.info("步骤4: 保存结果...")
        self._save_results(key_levels_df, effective_date)
        
        self._print_summary(key_levels_df)
        
        self.conn.close()
        
        self.logger.info("=" * 70)
        self.logger.info("关键位计算完成")
        self.logger.info("=" * 70)
    
    def _calculate_key_levels(self, kline_pattern: str, effective_date: str) -> pl.DataFrame:
        """使用 DuckDB 计算关键位"""
        
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
                MAX(high) as high_5d
            FROM base_data 
            WHERE rn <= 5
            GROUP BY code
        ),
        
        stats_10d AS (
            SELECT 
                code,
                AVG(close) as ma10,
                AVG(volume) as vol_ma10
            FROM base_data 
            WHERE rn <= 10
            GROUP BY code
        ),
        
        stats_20d AS (
            SELECT 
                code,
                AVG(close) as ma20,
                STDDEV(close) as std_20,
                MIN(low) as low_20d,
                MAX(high) as high_20d
            FROM base_data 
            WHERE rn <= 20
            GROUP BY code
        ),
        
        stats_60d AS (
            SELECT 
                code,
                AVG(close) as ma60,
                MIN(low) as low_60d,
                MAX(high) as high_60d,
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY low) as support_strong,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY high) as resistance_strong
            FROM base_data 
            WHERE rn <= 60
            GROUP BY code
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
            COALESCE(s10.ma10, l.close) as ma10,
            COALESCE(s20.ma20, l.close) as ma20,
            COALESCE(s60.ma60, l.close) as ma60,
            
            COALESCE(s5.low_5d, l.low) as support_5d,
            COALESCE(s20.low_20d, l.low) as support_20d,
            COALESCE(s60.low_60d, l.low) as support_60d,
            COALESCE(s60.support_strong, l.low) as support_strong,
            
            COALESCE(s5.high_5d, l.high) as resistance_5d,
            COALESCE(s20.high_20d, l.high) as resistance_20d,
            COALESCE(s60.high_60d, l.high) as resistance_60d,
            COALESCE(s60.resistance_strong, l.high) as resistance_strong,
            
            COALESCE(s20.std_20, 0) as std_20,
            COALESCE(s20.ma20 + 2 * s20.std_20, l.high) as bb_upper,
            COALESCE(s20.ma20 - 2 * s20.std_20, l.low) as bb_lower,
            
            p.high as prev_high,
            p.low as prev_low,
            p.close as prev_close,
            
            (l.high + l.low + l.close) / 3.0 as pivot,
            2.0 * (l.high + l.low + l.close) / 3.0 - l.low as pivot_r1,
            2.0 * (l.high + l.low + l.close) / 3.0 - l.high as pivot_s1,
            
            (l.close - p.close) / NULLIF(p.close, 0) * 100.0 as change_pct
            
        FROM latest l
        LEFT JOIN stats_5d s5 ON l.code = s5.code
        LEFT JOIN stats_10d s10 ON l.code = s10.code
        LEFT JOIN stats_20d s20 ON l.code = s20.code
        LEFT JOIN stats_60d s60 ON l.code = s60.code
        LEFT JOIN prev_day p ON l.code = p.code
        ORDER BY l.code
        """
        
        result = self.conn.execute(query).pl()
        return result
    
    def _add_stock_names(self, df: pl.DataFrame) -> pl.DataFrame:
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
                    self.logger.info(f"已关联股票名称")
            except Exception as e:
                self.logger.warning(f"关联股票名称失败: {e}")
        
        if 'name' not in df.columns:
            df = df.with_columns(pl.lit('').alias('name'))
        
        return df
    
    def _compare_with_history(self, df: pl.DataFrame, effective_date: str) -> pl.DataFrame:
        """对比历史关键位变化"""
        history_file = self.key_levels_dir / f"key_levels_history.parquet"
        
        if history_file.exists():
            try:
                history_df = pl.read_parquet(str(history_file))
                
                prev_date = self._get_previous_trading_day(history_df, effective_date)
                
                if prev_date:
                    prev_levels = history_df.filter(pl.col('trade_date') == prev_date)
                    
                    if len(prev_levels) > 0:
                        df = self._calculate_changes(df, prev_levels)
                        self.logger.info(f"已对比 {prev_date} 的关键位变化")
            except Exception as e:
                self.logger.warning(f"对比历史数据失败: {e}")
        
        if 'support_change' not in df.columns:
            df = df.with_columns([
                pl.lit(0.0).alias('support_change'),
                pl.lit(0.0).alias('resistance_change'),
                pl.lit('持平').alias('support_status'),
                pl.lit('持平').alias('resistance_status')
            ])
        
        return df
    
    def _get_previous_trading_day(self, history_df: pl.DataFrame, current_date: str) -> str:
        """获取上一个交易日"""
        dates = history_df['trade_date'].unique().sort(descending=True)
        for d in dates:
            d_str = str(d)
            if d_str < current_date:
                return d_str
        
        if len(dates) > 0:
            self.logger.info(f"历史数据中没有找到 {current_date} 之前的交易日，使用最新历史日期")
            return str(dates[0])
        
        return None
    
    def _calculate_changes(self, current_df: pl.DataFrame, prev_df: pl.DataFrame) -> pl.DataFrame:
        """计算关键位变化"""
        self.logger.info(f"当前数据: {len(current_df)} 条, 历史数据: {len(prev_df)} 条")
        
        prev_selected = prev_df.select([
            'code', 'support_strong', 'resistance_strong', 'ma20', 'ma60'
        ]).rename({
            'support_strong': 'prev_support_strong',
            'resistance_strong': 'prev_resistance_strong',
            'ma20': 'prev_ma20',
            'ma60': 'prev_ma60'
        })
        
        merged = current_df.join(prev_selected, on='code', how='left')
        
        matched = len(merged.filter(pl.col('prev_support_strong').is_not_null()))
        self.logger.info(f"匹配到历史数据: {matched} 条")
        
        merged = merged.with_columns([
            ((pl.col('support_strong') - pl.col('prev_support_strong')) / 
             pl.col('prev_support_strong') * 100).alias('support_change'),
            ((pl.col('resistance_strong') - pl.col('prev_resistance_strong')) / 
             pl.col('prev_resistance_strong') * 100).alias('resistance_change'),
        ])
        
        merged = merged.with_columns([
            pl.when(pl.col('support_change') > 2).then(pl.lit('上移'))
            .when(pl.col('support_change') < -2).then(pl.lit('下移'))
            .otherwise(pl.lit('持平')).alias('support_status'),
            
            pl.when(pl.col('resistance_change') > 2).then(pl.lit('上移'))
            .when(pl.col('resistance_change') < -2).then(pl.lit('下移'))
            .otherwise(pl.lit('持平')).alias('resistance_status'),
        ])
        
        return merged
    
    def _save_results(self, df: pl.DataFrame, effective_date: str):
        """保存结果"""
        daily_file = self.key_levels_dir / f"key_levels_{effective_date.replace('-', '')}.parquet"
        df.write_parquet(str(daily_file))
        self.logger.info(f"每日关键位已保存: {daily_file}")
        
        history_file = self.key_levels_dir / "key_levels_history.parquet"
        
        base_columns = [
            'code', 'trade_date', 'open', 'price', 'high', 'low', 'volume',
            'ma5', 'ma10', 'ma20', 'ma60',
            'support_5d', 'support_20d', 'support_60d', 'support_strong',
            'resistance_5d', 'resistance_20d', 'resistance_60d', 'resistance_strong',
            'std_20', 'bb_upper', 'bb_lower',
            'prev_high', 'prev_low', 'prev_close',
            'pivot', 'pivot_r1', 'pivot_s1', 'change_pct', 'name'
        ]
        
        df_base = df.select([c for c in base_columns if c in df.columns])
        
        if history_file.exists():
            history_df = pl.read_parquet(str(history_file))
            
            valid_dates = (
                history_df
                .group_by('trade_date')
                .agg(pl.col('code').n_unique().alias('count'))
                .filter(pl.col('count') > 100)
                .select('trade_date')
                .to_series()
                .implode()
            )
            
            history_df = history_df.filter(
                (pl.col('trade_date').is_in(valid_dates)) &
                (pl.col('trade_date') != effective_date)
            )
            
            common_cols = [c for c in df_base.columns if c in history_df.columns]
            if len(history_df) > 0:
                combined = pl.concat([history_df.select(common_cols), df_base.select(common_cols)])
            else:
                combined = df_base.select(common_cols)
            combined.write_parquet(str(history_file))
            self.logger.info(f"历史记录已更新: {history_file} ({len(combined)} 条)")
        else:
            df_base.write_parquet(str(history_file))
            self.logger.info(f"历史记录已创建: {history_file}")
        
        latest_file = self.output_dir / "key_levels_latest.parquet"
        df.write_parquet(str(latest_file))
        self.logger.info(f"最新关键位已保存: {latest_file}")
    
    def _print_summary(self, df: pl.DataFrame):
        """打印统计摘要"""
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("关键位统计摘要")
        self.logger.info("=" * 70)
        
        total = len(df)
        self.logger.info(f"总股票数: {total}")
        
        if 'support_status' in df.columns:
            support_up = len(df.filter(pl.col('support_status') == '上移'))
            support_down = len(df.filter(pl.col('support_status') == '下移'))
            support_flat = len(df.filter(pl.col('support_status') == '持平'))
            
            self.logger.info(f"支撑位上移: {support_up} ({support_up/total*100:.1f}%)")
            self.logger.info(f"支撑位下移: {support_down} ({support_down/total*100:.1f}%)")
            self.logger.info(f"支撑位持平: {support_flat} ({support_flat/total*100:.1f}%)")
        
        if 'resistance_status' in df.columns:
            res_up = len(df.filter(pl.col('resistance_status') == '上移'))
            res_down = len(df.filter(pl.col('resistance_status') == '下移'))
            res_flat = len(df.filter(pl.col('resistance_status') == '持平'))
            
            self.logger.info(f"压力位上移: {res_up} ({res_up/total*100:.1f}%)")
            self.logger.info(f"压力位下移: {res_down} ({res_down/total*100:.1f}%)")
            self.logger.info(f"压力位持平: {res_flat} ({res_flat/total*100:.1f}%)")
        
        self.logger.info("")
        self.logger.info("支撑位大幅上移 Top 10:")
        if 'support_change' in df.columns:
            top_support = df.filter(pl.col('support_change') > 0).sort('support_change', descending=True).head(10)
            for row in top_support.iter_rows(named=True):
                name = row.get('name', '')
                self.logger.info(f"  {row['code']} {name:8} 支撑位:{row['support_strong']:.2f} 变化:{row['support_change']:+.2f}%")
        
        self.logger.info("")
        self.logger.info("压力位大幅上移 Top 10:")
        if 'resistance_change' in df.columns:
            top_resistance = df.filter(pl.col('resistance_change') > 0).sort('resistance_change', descending=True).head(10)
            for row in top_resistance.iter_rows(named=True):
                name = row.get('name', '')
                self.logger.info(f"  {row['code']} {name:8} 压力位:{row['resistance_strong']:.2f} 变化:{row['resistance_change']:+.2f}%")


def main():
    """主函数"""
    project_root = Path(__file__).parent.parent
    kline_dir = project_root / "data" / "kline"
    output_dir = project_root / "data"
    stock_list_path = project_root / "data" / "stock_list.parquet"
    
    if not kline_dir.exists():
        logger.error(f"K线数据目录不存在: {kline_dir}")
        sys.exit(1)
    
    tracker = KeyLevelsTracker(
        kline_dir=str(kline_dir),
        output_dir=str(output_dir),
        stock_list_path=str(stock_list_path) if stock_list_path.exists() else None
    )
    tracker.run()


if __name__ == "__main__":
    main()
