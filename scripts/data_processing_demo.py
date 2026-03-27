"""
数据处理示例脚本 - 使用Polars和DuckDB处理Parquet数据

演示如何使用Polars和DuckDB进行高效的数据处理和分析
"""

import time
import polars as pl
import duckdb
import pandas as pd
from pathlib import Path


def demo_polars():
    """Polars示例 - 高性能DataFrame操作"""
    print("\n" + "="*70)
    print("Polars 示例 - 高性能数据处理")
    print("="*70 + "\n")
    
    start_time = time.time()
    
    print("1. 读取单个股票数据")
    df = pl.read_parquet('data/kline/000001.parquet')
    print(f"   股票代码: {df['code'][0]}")
    print(f"   数据行数: {len(df)}")
    print(f"   日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
    print(f"   耗时: {time.time() - start_time:.4f}秒\n")
    
    start_time = time.time()
    print("2. 读取所有股票数据（懒加载）")
    df_all = pl.scan_parquet('data/kline/*.parquet')
    print(f"   懒加载完成，耗时: {time.time() - start_time:.4f}秒\n")
    
    start_time = time.time()
    print("3. 计算每只股票的平均收盘价")
    avg_close = df_all.group_by('code').agg(
        pl.col('close').mean().alias('avg_close')
    ).collect()
    print(f"   股票数量: {len(avg_close)}")
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(f"   示例数据:")
    print(avg_close.head(5))
    print()
    
    start_time = time.time()
    print("4. 计算每只股票的最新价格和涨跌幅")
    latest_prices = df_all.sort('trade_date', descending=True).group_by('code').first().collect()
    print(f"   股票数量: {len(latest_prices)}")
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(f"   示例数据:")
    print(latest_prices.head(5))
    print()


def demo_duckdb():
    """DuckDB示例 - SQL查询Parquet文件"""
    print("\n" + "="*70)
    print("DuckDB 示例 - SQL查询Parquet文件")
    print("="*70 + "\n")
    
    start_time = time.time()
    
    print("1. 查询单个股票的最新数据")
    result = duckdb.query("""
        SELECT * FROM 'data/kline/000001.parquet'
        ORDER BY trade_date DESC
        LIMIT 5
    """).df()
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(result)
    print()
    
    start_time = time.time()
    print("2. 查询所有股票的最新价格")
    result = duckdb.query("""
        SELECT code, trade_date, close, volume
        FROM 'data/kline/*.parquet'
        WHERE (code, trade_date) IN (
            SELECT code, MAX(trade_date)
            FROM 'data/kline/*.parquet'
            GROUP BY code
        )
        ORDER BY code
        LIMIT 10
    """).df()
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(result)
    print()
    
    start_time = time.time()
    print("3. 计算每只股票的涨跌幅（使用窗口函数）")
    result = duckdb.query("""
        SELECT 
            code,
            trade_date,
            close,
            LAG(close) OVER (PARTITION BY code ORDER BY trade_date) as prev_close,
            ROUND((close - LAG(close) OVER (PARTITION BY code ORDER BY trade_date)) / 
            LAG(close) OVER (PARTITION BY code ORDER BY trade_date) * 100, 2) as pct_change
        FROM 'data/kline/000001.parquet'
        ORDER BY trade_date DESC
        LIMIT 10
    """).df()
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(result)
    print()
    
    start_time = time.time()
    print("4. 统计所有股票的平均涨跌幅")
    result = duckdb.query("""
        WITH daily_changes AS (
            SELECT 
                code,
                trade_date,
                close,
                LAG(close) OVER (PARTITION BY code ORDER BY trade_date) as prev_close,
                (close - LAG(close) OVER (PARTITION BY code ORDER BY trade_date)) / 
                LAG(close) OVER (PARTITION BY code ORDER BY trade_date) * 100 as pct_change
            FROM 'data/kline/*.parquet'
        )
        SELECT 
            code,
            COUNT(*) as trading_days,
            ROUND(AVG(pct_change), 2) as avg_pct_change,
            ROUND(MIN(pct_change), 2) as min_pct_change,
            ROUND(MAX(pct_change), 2) as max_pct_change
        FROM daily_changes
        WHERE pct_change IS NOT NULL
        GROUP BY code
        ORDER BY avg_pct_change DESC
        LIMIT 10
    """).df()
    print(f"   耗时: {time.time() - start_time:.4f}秒")
    print(result)
    print()


def demo_pandas():
    """Pandas示例 - 兼容性读取"""
    print("\n" + "="*70)
    print("Pandas 示例 - 兼容性读取")
    print("="*70 + "\n")
    
    start_time = time.time()
    
    print("1. 读取单个股票数据")
    df = pd.read_parquet('data/kline/000001.parquet')
    print(f"   股票代码: {df['code'].iloc[0]}")
    print(f"   数据行数: {len(df)}")
    print(f"   日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
    print(f"   耗时: {time.time() - start_time:.4f}秒\n")
    
    print("2. 数据基本信息")
    print(df.info())
    print()
    
    print("3. 数据统计")
    print(df.describe())
    print()


def compare_performance():
    """性能对比"""
    print("\n" + "="*70)
    print("性能对比 - Polars vs Pandas")
    print("="*70 + "\n")
    
    print("任务: 读取所有股票数据并计算平均收盘价")
    print()
    
    print("1. Polars (懒加载 + 多线程)")
    start_time = time.time()
    df_polars = pl.scan_parquet('data/kline/*.parquet')
    result_polars = df_polars.group_by('code').agg(
        pl.col('close').mean().alias('avg_close')
    ).collect()
    polars_time = time.time() - start_time
    print(f"   耗时: {polars_time:.4f}秒")
    print(f"   结果行数: {len(result_polars)}")
    print()
    
    print("2. DuckDB (SQL查询)")
    start_time = time.time()
    result_duckdb = duckdb.query("""
        SELECT code, AVG(close) as avg_close
        FROM 'data/kline/*.parquet'
        GROUP BY code
    """).df()
    duckdb_time = time.time() - start_time
    print(f"   耗时: {duckdb_time:.4f}秒")
    print(f"   结果行数: {len(result_duckdb)}")
    print()
    
    print(f"性能对比:")
    print(f"  Polars: {polars_time:.4f}秒")
    print(f"  DuckDB: {duckdb_time:.4f}秒")
    print()


def main():
    """主函数"""
    print("\n" + "="*70)
    print("数据处理技术栈演示")
    print("="*70)
    
    if not Path('data/kline').exists():
        print("\n❌ 数据目录不存在，请先运行数据采集脚本")
        return
    
    demo_polars()
    demo_duckdb()
    demo_pandas()
    compare_performance()
    
    print("\n" + "="*70)
    print("演示完成！")
    print("="*70 + "\n")


if __name__ == '__main__':
    main()
