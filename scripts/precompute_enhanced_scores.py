"""
预计算股票技术指标评分
使用 DuckDB + Parquet 从 K 线数据计算技术指标并生成预计算文件
"""

import sys
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import duckdb
import polars as pl
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.freshness_check_decorator import check_data_freshness


class PrecomputeEngine:
    """DuckDB 预计算引擎"""
    
    def __init__(self, kline_dir: str, output_path: str, stock_list_path: str = None, cutoff_hour: int = 15):
        self.kline_dir = Path(kline_dir)
        self.output_path = Path(output_path)
        self.stock_list_path = Path(stock_list_path) if stock_list_path else None
        self.cutoff_hour = cutoff_hour
        self.conn = duckdb.connect(":memory:")
        self.logger = logging.getLogger(__name__)
    
    def get_effective_date(self) -> str:
        """根据当前时间确定有效数据日期"""
        now = datetime.now()
        
        if now.hour < self.cutoff_hour:
            effective_date = now - timedelta(days=1)
        else:
            effective_date = now
        
        effective_date_str = effective_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"有效数据日期: {effective_date_str} ({'15点前-使用昨日数据' if now.hour < self.cutoff_hour else '15点后-使用今日数据'})")
        
        return effective_date_str
    
    def run(self):
        """执行预计算"""
        self.logger.info("=" * 70)
        self.logger.info("开始预计算股票技术指标")
        self.logger.info("=" * 70)

        effective_date = self.get_effective_date()

        self.logger.info(f"K线数据路径: {self.kline_dir}")
        self.logger.info(f"输出文件: {self.output_path}")

        kline_pattern = str(self.kline_dir / "*.parquet")

        # 使用 Polars 直接读取和处理数据
        self.logger.info("步骤1: 读取K线数据...")
        all_stocks = []
        
        # 遍历所有Parquet文件
        for parquet_file in self.kline_dir.glob("*.parquet"):
            try:
                # 读取单个股票数据
                df = pl.read_parquet(str(parquet_file))
                
                # 过滤有效日期
                df = df.filter(pl.col("trade_date") <= effective_date)

                # 过滤掉数据过老的股票（最近30个交易日内无数据的股票）
                min_date = (datetime.strptime(effective_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
                df = df.filter(pl.col("trade_date") >= min_date)

                if len(df) > 0:
                    # 按日期降序排序
                    df = df.sort("trade_date", descending=True)

                    # 取最新一条数据并确保类型一致
                    latest_row = df.head(1).with_columns([
                        pl.col("volume").cast(pl.Int64).fill_null(0),
                        pl.col("open").cast(pl.Float64).fill_null(0),
                        pl.col("close").cast(pl.Float64).fill_null(0),
                        pl.col("high").cast(pl.Float64).fill_null(0),
                        pl.col("low").cast(pl.Float64).fill_null(0)
                    ])

                    # 添加到结果列表
                    all_stocks.append(latest_row)
            except Exception as e:
                self.logger.warning(f"处理文件 {parquet_file} 失败: {e}")
                self.logger.debug(f"异常详情: {traceback.format_exc()}")
                continue
        
        if not all_stocks:
            self.logger.error("未找到有效数据")
            return
        
        # 合并所有股票数据
        latest_df = pl.concat(all_stocks)
        self.logger.info(f"找到 {len(latest_df)} 只股票的最新数据")
        
        # 计算技术指标
        self.logger.info("步骤2: 计算技术指标...")
        min_date = (datetime.strptime(effective_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        indicators_df = self._calculate_indicators(kline_pattern, effective_date, min_date)
        
        self.logger.info("步骤3: 计算评分和等级...")
        scored_df = self._calculate_scores(indicators_df)
        
        self.logger.info("步骤4: 关联股票名称...")
        scored_df = self._add_stock_names(scored_df)
        
        self.logger.info("步骤5: 保存结果...")
        self._save_results(scored_df)
        
        self._print_summary(scored_df)
        
        self.conn.close()
        
        self.logger.info("=" * 70)
        self.logger.info("预计算完成")
        self.logger.info("=" * 70)
    
    def _get_latest_trading_data(self, kline_pattern: str, effective_date: str) -> pl.DataFrame:
        """获取最新交易日数据"""
        try:
            query = f"""
            WITH latest AS (
                SELECT code, MAX(trade_date) as latest_date
                FROM '{kline_pattern}'
                WHERE trade_date <= '{effective_date}'
                GROUP BY code
            )
            SELECT 
                k.code,
                k.trade_date,
                k.open,
                k.close,
                k.high,
                k.low,
                k.volume
            FROM '{kline_pattern}' k
            INNER JOIN latest l ON k.code = l.code AND k.trade_date = l.latest_date
            WHERE k.trade_date <= '{effective_date}'
            ORDER BY k.code
            """
            
            result = self.conn.execute(query).pl()
            return result
        except Exception as e:
            self.logger.error(f"获取最新交易日数据失败: {e}")
            # 尝试使用更简单的查询
            try:
                simple_query = f"""
                SELECT 
                    code,
                    MAX(trade_date) as trade_date,
                    MAX(open) as open,
                    MAX(close) as close,
                    MAX(high) as high,
                    MAX(low) as low,
                    MAX(volume) as volume
                FROM '{kline_pattern}'
                WHERE trade_date <= '{effective_date}'
                GROUP BY code
                ORDER BY code
                """
                self.logger.info("尝试使用简单查询")
                result = self.conn.execute(simple_query).pl()
                return result
            except Exception as e2:
                self.logger.error(f"简单查询也失败: {e2}")
                # 返回空DataFrame
                return pl.DataFrame({
                    'code': [],
                    'trade_date': [],
                    'open': [],
                    'close': [],
                    'high': [],
                    'low': [],
                    'volume': []
                })
    
    def _calculate_indicators(self, kline_pattern: str, effective_date: str, min_date: str = None) -> pl.DataFrame:
        """计算技术指标"""
        if min_date is None:
            min_date = (datetime.strptime(effective_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

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
              AND trade_date >= '{min_date}'
        ),
        
        latest AS (
            SELECT * FROM base_data WHERE rn = 1
        ),
        
        prev_1d AS (
            SELECT * FROM base_data WHERE rn = 2
        ),
        
        prev_5d AS (
            SELECT 
                code,
                AVG(close) as ma5,
                AVG(volume) as vol_ma5
            FROM base_data 
            WHERE rn <= 5
            GROUP BY code
        ),
        
        prev_10d AS (
            SELECT 
                code,
                AVG(close) as ma10,
                AVG(volume) as vol_ma10
            FROM base_data 
            WHERE rn <= 10
            GROUP BY code
        ),
        
        prev_20d AS (
            SELECT 
                code,
                AVG(close) as ma20,
                MIN(low) as low_20,
                MAX(high) as high_20
            FROM base_data 
            WHERE rn <= 20
            GROUP BY code
        ),
        
        momentum AS (
            SELECT 
                code,
                close as close_3d_ago
            FROM base_data 
            WHERE rn = 4
        ),
        
        momentum_10 AS (
            SELECT 
                code,
                close as close_10d_ago
            FROM base_data 
            WHERE rn = 11
        ),
        
        momentum_20 AS (
            SELECT 
                code,
                close as close_20d_ago
            FROM base_data 
            WHERE rn = 21
        ),
        
        price_range AS (
            SELECT 
                code,
                high - low as daily_range,
                ABS(close - LAG(close) OVER (PARTITION BY code ORDER BY trade_date)) as price_change,
                CASE WHEN close > LAG(close) OVER (PARTITION BY code ORDER BY trade_date) 
                     THEN high - low 
                     ELSE 0 END as up_move,
                CASE WHEN close < LAG(close) OVER (PARTITION BY code ORDER BY trade_date) 
                     THEN high - low 
                     ELSE 0 END as down_move
            FROM base_data
            WHERE rn <= 14
        ),
        
        rsi_calc AS (
            SELECT 
                code,
                AVG(up_move) as avg_up,
                AVG(down_move) as avg_down
            FROM price_range
            GROUP BY code
        )
        
        SELECT 
            l.code,
            l.trade_date,
            l.open,
            l.close as price,
            l.high,
            l.low,
            l.volume,
            
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
            
            CASE 
                WHEN COALESCE(r.avg_down, 0) = 0 THEN 50
                ELSE ROUND(100 - 100 / (1 + COALESCE(r.avg_up, 0) / NULLIF(r.avg_down, 0)), 2)
            END as rsi,
            
            ROUND((l.close - COALESCE(m20.low_20, l.low)) / NULLIF(COALESCE(m20.high_20, l.high) - COALESCE(m20.low_20, l.low), 0) * 100, 2) as position
            
        FROM latest l
        LEFT JOIN prev_1d p1 ON l.code = p1.code
        LEFT JOIN prev_5d m5 ON l.code = m5.code
        LEFT JOIN prev_10d m10 ON l.code = m10.code
        LEFT JOIN prev_20d m20 ON l.code = m20.code
        LEFT JOIN momentum mo ON l.code = mo.code
        LEFT JOIN momentum_10 mo10 ON l.code = mo10.code
        LEFT JOIN momentum_20 mo20 ON l.code = mo20.code
        LEFT JOIN rsi_calc r ON l.code = r.code
        ORDER BY l.code
        """
        
        result = self.conn.execute(query).pl()
        self.logger.info(f"计算完成: {len(result)} 条记录")
        return result
    
    def _calculate_scores(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算评分和等级"""
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
    
    def _save_results(self, df: pl.DataFrame):
        """保存结果"""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.write_parquet(str(self.output_path))
        self.logger.info(f"结果已保存: {self.output_path}")
        
        today = datetime.now().strftime("%Y%m%d")
        snapshot_path = self.output_path.parent / f"enhanced_scores_{today}.parquet"
        df.write_parquet(str(snapshot_path))
        self.logger.info(f"快照已保存: {snapshot_path}")
    
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
                    self.logger.info(f"已关联股票名称: {len(df.filter(pl.col('name').is_not_null()))} 只")
            except Exception as e:
                self.logger.warning(f"关联股票名称失败: {e}")
        
        if 'name' not in df.columns:
            df = df.with_columns(pl.lit('').alias('name'))
        
        return df
    
    def _print_summary(self, df: pl.DataFrame):
        """打印统计摘要"""
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("统计摘要")
        self.logger.info("=" * 70)
        
        total = len(df)
        s_count = len(df.filter(pl.col("grade") == "S"))
        a_count = len(df.filter(pl.col("grade") == "A"))
        b_count = len(df.filter(pl.col("grade") == "B"))
        c_count = len(df.filter(pl.col("grade") == "C"))
        
        self.logger.info(f"总股票数: {total}")
        self.logger.info(f"S级 (强烈推荐): {s_count} ({s_count/total*100:.1f}%)")
        self.logger.info(f"A级 (建议关注): {a_count} ({a_count/total*100:.1f}%)")
        self.logger.info(f"B级 (观望): {b_count} ({b_count/total*100:.1f}%)")
        self.logger.info(f"C级 (谨慎): {c_count} ({c_count/total*100:.1f}%)")
        
        self.logger.info("")
        self.logger.info("S级 Top 10:")
        top_s = df.filter(pl.col("grade") == "S").sort("enhanced_score", descending=True).head(10)
        for row in top_s.iter_rows(named=True):
            self.logger.info(f"  {row['code']} 价格:{row['price']:.2f} 涨幅:{row['change_pct']:+.2f}% 评分:{row['enhanced_score']:.0f} {row['reasons']}")


@check_data_freshness
def main():
    """主函数"""
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


if __name__ == "__main__":
    main()
