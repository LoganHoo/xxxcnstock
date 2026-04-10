#!/usr/bin/env python3
"""直接运行precompute，跳过data_freshness_check"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import logging
import time
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PrecomputeEngine:
    def __init__(self, kline_dir, output_path, stock_list_path=None):
        self.kline_dir = Path(kline_dir)
        self.output_path = Path(output_path)
        self.stock_list_path = Path(stock_list_path) if stock_list_path else None
        self.conn = duckdb.connect(database=':memory:')
        self.logger = logger
        self.latest_date = None
        self.prev_date = None

    def run(self):
        self.logger.info("开始预计算...")
        start = time.time()

        df = self._load_data()
        df = self._calculate_scores(df)
        df = self._add_stock_names(df)
        self._save_results(df)
        self._print_summary(df)

        self.logger.info(f"总耗时: {time.time() - start:.1f}秒")

    def _load_data(self):
        self.logger.info("加载数据...")
        self.conn.execute(f"""
            CREATE TABLE klines AS SELECT * FROM read_parquet('{self.kline_dir}/*.parquet')
        """)
        self.latest_date = self.conn.execute("SELECT MAX(trade_date) FROM klines").fetchone()[0]
        self.logger.info(f"最新日期: {self.latest_date}")

        prev_dt = datetime.strptime(self.latest_date, '%Y-%m-%d') - timedelta(days=1)
        self.prev_date = prev_dt.strftime('%Y-%m-%d')

        prev_5d_dt = prev_dt - timedelta(days=4)
        prev_10d_dt = prev_dt - timedelta(days=9)
        prev_20d_dt = prev_dt - timedelta(days=19)

        mom_3d_dt = prev_dt - timedelta(days=2)
        mom_10d_dt = prev_dt - timedelta(days=9)
        mom_20d_dt = prev_dt - timedelta(days=19)

        rsi_start_dt = prev_dt - timedelta(days=13)

        query = f"""
        WITH latest AS (
            SELECT code, trade_date, open, close, high, low, volume
            FROM klines
            WHERE trade_date = '{self.latest_date}'
        ),
        prev_1d AS (
            SELECT code, close
            FROM klines
            WHERE trade_date = '{self.prev_date}'
        ),
        prev_5d AS (
            SELECT code,
                AVG(close) as ma5,
                AVG(volume) as vol_ma5
            FROM klines
            WHERE trade_date >= '{prev_5d_dt.strftime('%Y-%m-%d')}'
                AND trade_date <= '{self.prev_date}'
            GROUP BY code
        ),
        prev_10d AS (
            SELECT code,
                AVG(close) as ma10,
                AVG(volume) as vol_ma10
            FROM klines
            WHERE trade_date >= '{prev_10d_dt.strftime('%Y-%m-%d')}'
                AND trade_date <= '{self.prev_date}'
            GROUP BY code
        ),
        prev_20d AS (
            SELECT code,
                AVG(close) as ma20,
                MIN(low) as low_20,
                MAX(high) as high_20
            FROM klines
            WHERE trade_date >= '{prev_20d_dt.strftime('%Y-%m-%d')}'
                AND trade_date <= '{self.prev_date}'
            GROUP BY code
        ),
        momentum AS (
            SELECT code, close as close_3d_ago
            FROM klines
            WHERE trade_date = '{mom_3d_dt.strftime('%Y-%m-%d')}'
        ),
        momentum_10 AS (
            SELECT code, close as close_10d_ago
            FROM klines
            WHERE trade_date = '{mom_10d_dt.strftime('%Y-%m-%d')}'
        ),
        momentum_20 AS (
            SELECT code, close as close_20d_ago
            FROM klines
            WHERE trade_date = '{mom_20d_dt.strftime('%Y-%m-%d')}'
        )
        SELECT
            l.code,
            l.trade_date,
            l.open,
            l.close,
            l.high,
            l.low,
            l.volume,
            l.close as price,
            COALESCE(p1.close, l.close) as prev_close,
            ROUND((l.close - COALESCE(p1.close, l.close)) / NULLIF(COALESCE(p1.close, l.close), 0) * 100, 2) as change_pct,
            COALESCE(m5.ma5, l.close) as ma5,
            COALESCE(m10.ma10, l.close) as ma10,
            COALESCE(m20.ma20, l.close) as ma20,
            COALESCE(m5.vol_ma5, l.volume) as vol_ma5,
            COALESCE(m10.vol_ma10, l.volume) as vol_ma10,
            COALESCE(m20.low_20, l.low) as low_20,
            COALESCE(m20.high_20, l.high) as high_20,
            ROUND((l.close - COALESCE(mo.close_3d_ago, l.close)) / NULLIF(COALESCE(mo.close_3d_ago, l.close), 0) * 100, 2) as momentum_3d,
            ROUND((l.close - COALESCE(mo10.close_10d_ago, l.close)) / NULLIF(COALESCE(mo10.close_10d_ago, l.close), 0) * 100, 2) as momentum_10d,
            ROUND((l.close - COALESCE(mo20.close_20d_ago, l.close)) / NULLIF(COALESCE(mo20.close_20d_ago, l.close), 0) * 100, 2) as momentum_20d,
            ROUND((l.high - l.low) / NULLIF(l.low, 0) * 100, 2) as amplitude,
            ROUND(l.volume / NULLIF(COALESCE(m5.vol_ma5, l.volume), 0), 2) as volume_ratio,
            50.0 as rsi,
            ROUND((l.close - COALESCE(m20.low_20, l.low)) / NULLIF(COALESCE(m20.high_20, l.high) - COALESCE(m20.low_20, l.low), 0) * 100, 2) as position
        FROM latest l
        LEFT JOIN prev_1d p1 ON l.code = p1.code
        LEFT JOIN prev_5d m5 ON l.code = m5.code
        LEFT JOIN prev_10d m10 ON l.code = m10.code
        LEFT JOIN prev_20d m20 ON l.code = m20.code
        LEFT JOIN momentum mo ON l.code = mo.code
        LEFT JOIN momentum_10 mo10 ON l.code = mo10.code
        LEFT JOIN momentum_20 mo20 ON l.code = mo20.code
        ORDER BY l.code
        """
        result = self.conn.execute(query).pl()
        self.logger.info(f"计算完成: {len(result)} 条记录")
        return result

    def _calculate_scores(self, df):
        df = df.with_columns([
            pl.when(pl.col("change_pct") > 5).then(20)
            .when(pl.col("change_pct") > 3).then(15)
            .when(pl.col("change_pct") > 1).then(10)
            .when(pl.col("change_pct") > 0).then(5)
            .when(pl.col("change_pct") > -1).then(0)
            .when(pl.col("change_pct") > -3).then(-5)
            .otherwise(-10).alias("score_change"),

            pl.when((pl.col("price") > pl.col("ma5")) &
                    (pl.col("ma5") > pl.col("ma10")) &
                    (pl.col("ma10") > pl.col("ma20"))).then(20)
            .when((pl.col("price") > pl.col("ma5")) &
                  (pl.col("ma5") > pl.col("ma10"))).then(15)
            .when(pl.col("price") > pl.col("ma5")).then(10)
            .when(pl.col("price") > pl.col("ma20")).then(5)
            .otherwise(0).alias("score_trend"),

            pl.when(pl.col("momentum_10d") > 15).then(20)
            .when(pl.col("momentum_10d") > 10).then(15)
            .when(pl.col("momentum_10d") > 5).then(10)
            .when(pl.col("momentum_10d") > 0).then(5)
            .when(pl.col("momentum_10d") > -5).then(0)
            .otherwise(-5).alias("score_momentum"),

            pl.when((pl.col("rsi") > 30) & (pl.col("rsi") < 70)).then(15)
            .when((pl.col("rsi") > 20) & (pl.col("rsi") < 80)).then(10)
            .when(pl.col("rsi") < 30).then(5)
            .otherwise(0).alias("score_rsi"),

            pl.when((pl.col("volume_ratio") > 1.5) & (pl.col("change_pct") > 0)).then(15)
            .when((pl.col("volume_ratio") > 1) & (pl.col("change_pct") > 0)).then(10)
            .when(pl.col("volume_ratio") > 0.8).then(5)
            .otherwise(0).alias("score_volume"),

            pl.when(pl.col("position") < 20).then(15)
            .when(pl.col("position") < 40).then(10)
            .when(pl.col("position") < 60).then(5)
            .when(pl.col("position") > 80).then(-5)
            .otherwise(0).alias("score_position"),
        ])

        df = df.with_columns([
            (50 +
             pl.col("score_change") +
             pl.col("score_trend") +
             pl.col("score_momentum") +
             pl.col("score_rsi") +
             pl.col("score_volume") +
             pl.col("score_position")).alias("enhanced_score")
        ])

        df = df.with_columns([
            pl.when(pl.col("enhanced_score") >= 100).then(pl.lit("S"))
            .when(pl.col("enhanced_score") >= 85).then(pl.lit("A"))
            .when(pl.col("enhanced_score") >= 65).then(pl.lit("B"))
            .otherwise(pl.lit("C")).alias("grade")
        ])

        df = df.with_columns([
            pl.when((pl.col("price") > pl.col("ma5")) &
                    (pl.col("ma5") > pl.col("ma10")) &
                    (pl.col("ma10") > pl.col("ma20"))).then(pl.lit(100))
            .when((pl.col("price") > pl.col("ma5")) &
                  (pl.col("ma5") > pl.col("ma10"))).then(pl.lit(80))
            .when(pl.col("price") > pl.col("ma5")).then(pl.lit(60))
            .when(pl.col("price") > pl.col("ma20")).then(pl.lit(40))
            .otherwise(pl.lit(20)).alias("trend")
        ])

        df = df.with_columns([
            pl.when((pl.col("grade") == "S") & (pl.col("change_pct") > 3))
              .then(pl.lit("强势上涨,多头排列"))
            .when((pl.col("grade") == "A") & (pl.col("change_pct") > 1))
              .then(pl.lit("偏多趋势,量价齐升"))
            .when(pl.col("trend") == 100)
              .then(pl.lit("多头排列"))
            .when(pl.col("trend") >= 60)
              .then(pl.lit("偏多趋势"))
            .when(pl.col("trend") >= 40)
              .then(pl.lit("震荡整理"))
            .otherwise(pl.lit("偏空趋势")).alias("reasons")
        ])

        return df

    def _add_stock_names(self, df):
        if self.stock_list_path and self.stock_list_path.exists():
            try:
                stock_list = pl.read_parquet(str(self.stock_list_path))
                if 'code' in stock_list.columns and 'name' in stock_list.columns:
                    df = df.join(
                        stock_list.select(['code', 'name']),
                        on='code',
                        how='left'
                    )
            except:
                pass
        if 'name' not in df.columns:
            df = df.with_columns(pl.lit('').alias('name'))
        return df

    def _save_results(self, df):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(str(self.output_path))
        self.logger.info(f"结果已保存: {self.output_path}")

    def _print_summary(self, df):
        print("\n" + "=" * 70)
        print("统计摘要")
        print("=" * 70)

        total = len(df)
        s_count = len(df.filter(pl.col("grade") == "S"))
        a_count = len(df.filter(pl.col("grade") == "A"))
        b_count = len(df.filter(pl.col("grade") == "B"))
        c_count = len(df.filter(pl.col("grade") == "C"))

        print(f"总股票数: {total}")
        print(f"S级 (强烈推荐): {s_count} ({s_count/total*100:.1f}%)")
        print(f"A级 (建议关注): {a_count} ({a_count/total*100:.1f}%)")
        print(f"B级 (观望): {b_count} ({b_count/total*100:.1f}%)")
        print(f"C级 (谨慎): {c_count} ({c_count/total*100:.1f}%)")

        print("\nS级 Top 10:")
        top_s = df.filter(pl.col("grade") == "S").sort("enhanced_score", descending=True).head(10)
        for row in top_s.iter_rows(named=True):
            print(f"  {row['code']} 价格:{row['price']:.2f} 涨幅:{row['change_pct']:+.2f}% 评分:{row['enhanced_score']:.0f}")

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    kline_dir = project_root / "data" / "kline"
    output_path = project_root / "data" / "enhanced_full_temp.parquet"
    stock_list_path = project_root / "data" / "stock_list.parquet"

    if not kline_dir.exists():
        logger.error(f"K线数据目录不存在: {kline_dir}")
        sys.exit(1)

    engine = PrecomputeEngine(
        kline_dir=str(kline_dir),
        output_path=str(output_path),
        stock_list_path=str(stock_list_path) if stock_list_path.exists() else None
    )
    engine.run()