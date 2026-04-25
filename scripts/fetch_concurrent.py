#!/usr/bin/env python3
"""
并发获取K线数据 - 使用多进程加速
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
import baostock as bs
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import logging

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

TARGET_DATE = "2026-04-21"
BATCH_SIZE = 50
STANDARD_COLUMNS = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']


def format_code(code: str) -> str:
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_batch_worker(batch_codes):
    """工作进程：获取一批股票数据"""
    results = []
    
    lg = bs.login()
    if lg.error_code != '0':
        return results
    
    for code in batch_codes:
        try:
            bs_code = format_code(code)
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount",
                start_date=TARGET_DATE,
                end_date=TARGET_DATE,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if data_list:
                results.append((code, data_list[0]))
        except:
            pass
    
    bs.logout()
    return results


def save_to_parquet(code_data):
    """保存数据到parquet"""
    code, data = code_data
    try:
        df = pd.DataFrame([data], columns=['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount'])
        
        for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
        
        if kline_path.exists():
            existing_df = pl.read_parquet(kline_path)
            
            if TARGET_DATE in existing_df['trade_date'].to_list():
                return True
            
            for col in STANDARD_COLUMNS:
                if col not in existing_df.columns:
                    existing_df = existing_df.with_columns(pl.lit(None).alias(col))
            
            existing_df = existing_df.select(STANDARD_COLUMNS)
            new_df = pl.from_pandas(df).select(STANDARD_COLUMNS)
            
            combined = pl.concat([existing_df, new_df])
            combined = combined.unique(subset=['trade_date']).sort('trade_date')
            combined.write_parquet(kline_path)
        else:
            pl.from_pandas(df).write_parquet(kline_path)
        
        return True
    except:
        return False


def main():
    print(f"开始并发获取 {TARGET_DATE} 数据")
    
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
    stock_list = pl.read_parquet(stock_list_path)
    codes = stock_list['code'].to_list()
    print(f"股票列表: {len(codes)} 只")
    
    # 分批
    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    print(f"分成 {len(batches)} 批，使用 {cpu_count()} 进程并发处理")
    
    all_results = []
    
    # 使用进程池并发获取
    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        futures = {executor.submit(fetch_batch_worker, batch): i for i, batch in enumerate(batches)}
        
        completed = 0
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
                completed += 1
                if completed % 10 == 0:
                    print(f"进度: {completed}/{len(batches)} 批，已获取 {len(all_results)} 只股票")
            except Exception as e:
                print(f"批次 {batch_idx} 失败: {e}")
    
    print(f"\n获取完成: {len(all_results)} 只股票")
    
    # 保存数据
    print("开始保存数据...")
    success = 0
    for code_data in all_results:
        if save_to_parquet(code_data):
            success += 1
    
    print(f"保存完成: 成功 {success}/{len(all_results)}")


if __name__ == "__main__":
    main()
