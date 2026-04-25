#!/usr/bin/env python3
"""
采集过去300天的K线数据
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
from services.data_service.fetchers.kline_fetcher import fetch_kline_data_parallel
from core.paths import DATA_DIR
import pandas as pd

def main():
    print("=" * 60)
    print("K线数据采集 - 过去300天")
    print("=" * 60)
    
    # 计算300天前的日期
    end_date = datetime.now()
    start_date = end_date - timedelta(days=300)
    
    print(f"\n时间范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    print(f"天数: 300天")
    
    # 获取股票列表
    print("\n加载股票列表...")
    stock_list = pd.read_parquet(DATA_DIR / 'stock_list.parquet')
    codes = stock_list['code'].tolist()
    print(f"股票数量: {len(codes)} 只")
    
    # 采集K线数据
    print("\n开始采集K线数据...")
    print("-" * 60)
    
    kline_dir = DATA_DIR / 'kline'
    result = fetch_kline_data_parallel(
        codes=codes,
        kline_dir=kline_dir,
        days=300,
        filter_delisted=True
    )
    
    print("-" * 60)
    print("\n采集完成!")
    print(f"成功: {result.get('success_count', 0)} 只")
    print(f"跳过: {result.get('skipped_count', 0)} 只")
    print(f"失败: {result.get('failed_count', 0)} 只")
    print(f"总行数: {result.get('total_rows', 0)} 条")
    
    # 验证结果
    print("\n验证数据...")
    import os
    kline_files = list(kline_dir.glob('*.parquet'))
    print(f"K线数据文件总数: {len(kline_files)} 个")
    
    if kline_files:
        # 抽样检查
        sample_file = kline_files[0]
        df = pd.read_parquet(sample_file)
        print(f"\n示例文件: {sample_file.name}")
        print(f"  数据行数: {len(df)}")
        print(f"  日期范围: {df['trade_date'].min()} 至 {df['trade_date'].max()}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
