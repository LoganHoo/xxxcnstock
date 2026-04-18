#!/usr/bin/env python3
"""
删除2026-04-17的所有K线数据
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
target_date = '2026-04-17'

deleted_count = 0
error_count = 0
lock = threading.Lock()

def process_file(parquet_file):
    global deleted_count, error_count
    try:
        df = pl.read_parquet(parquet_file)
        
        # 检查是否包含目标日期
        if df.filter(pl.col('trade_date') == target_date).height > 0:
            # 删除目标日期的数据
            df = df.filter(pl.col('trade_date') != target_date)
            
            if df.height > 0:
                # 保存剩余数据
                df.write_parquet(parquet_file)
                with lock:
                    deleted_count += 1
            else:
                # 如果文件为空，删除文件
                parquet_file.unlink()
                with lock:
                    deleted_count += 1
    except Exception as e:
        with lock:
            error_count += 1

# 获取所有parquet文件
parquet_files = list(kline_dir.glob('*.parquet'))
print(f"处理 {len(parquet_files)} 个文件...")

# 并行处理
with ThreadPoolExecutor(max_workers=8) as executor:
    list(executor.map(process_file, parquet_files))

print(f"\n完成:")
print(f"  删除/修改: {deleted_count} 个文件")
print(f"  错误: {error_count} 个文件")
