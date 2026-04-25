#!/usr/bin/env python3
"""
数据质量检查报告生成器
"""
import polars as pl
from pathlib import Path
from datetime import datetime


def generate_data_quality_report():
    """生成数据质量报告"""
    print('=' * 70)
    print('📊 数据质量检查报告')
    print('=' * 70)

    # 1. 股票数量
    kline_dir = Path('data/kline')
    stock_count = len(list(kline_dir.glob('*.parquet')))
    print(f'\n1. K线数据覆盖')
    print(f'   股票数量: {stock_count}')

    # 2. 样本数据检查
    sample_file = kline_dir / '000001.parquet'
    if sample_file.exists():
        df = pl.read_parquet(sample_file)
        print(f'\n2. 样本数据 (000001)')
        print(f'   数据条数: {len(df)}')
        print(f'   日期范围: {df["trade_date"].min()} ~ {df["trade_date"].max()}')
        print(f'   列数: {len(df.columns)}')
        print(f'   列名: {df.columns}')

    # 3. 数据新鲜度
    latest_date = df['trade_date'].max()
    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
    days_diff = (datetime.now() - latest_dt).days
    print(f'\n3. 数据新鲜度')
    print(f'   最新数据日期: {latest_date}')
    print(f'   距今: {days_diff} 天')
    print(f'   状态: {"✅ 正常" if days_diff <= 2 else "⚠️ 需要更新"}')

    # 4. 股票列表
    stock_list_path = Path('data/stock_list.parquet')
    if stock_list_path.exists():
        stock_list = pl.read_parquet(stock_list_path)
        print(f'\n4. 股票列表')
        print(f'   总数: {len(stock_list)}')
        print(f'   列: {stock_list.columns[:5]}...')

    print('\n' + '=' * 70)


if __name__ == "__main__":
    generate_data_quality_report()
