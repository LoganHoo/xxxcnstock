#!/usr/bin/env python3
"""
获取最新K线数据（4月21日）- 优化版本
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
import baostock as bs
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import setup_logger

logger = setup_logger("fetch_latest")

TARGET_DATE = "2026-04-21"
BATCH_SIZE = 100  # 增大批次

STANDARD_COLUMNS = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']


def format_code(code: str) -> str:
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_all_data(codes: list) -> dict:
    """一次性获取所有股票数据"""
    results = {}
    
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"登录失败: {lg.error_msg}")
        return results
    
    logger.info(f"开始获取 {len(codes)} 只股票的 {TARGET_DATE} 数据...")
    
    for i, code in enumerate(codes):
        if i % 500 == 0:
            logger.info(f"进度: {i}/{len(codes)}")
        
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
                results[code] = data_list[0]
        except Exception as e:
            pass
    
    bs.logout()
    logger.info(f"获取完成，成功: {len(results)} 只")
    return results


def save_all_data(all_data: dict):
    """保存所有数据"""
    success = 0
    fail = 0
    
    for code, data in all_data.items():
        try:
            df = pd.DataFrame([data], columns=['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount'])
            
            for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
            
            if kline_path.exists():
                existing_df = pl.read_parquet(kline_path)
                
                if TARGET_DATE in existing_df['trade_date'].to_list():
                    success += 1
                    continue
                
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
            
            success += 1
        except Exception as e:
            fail += 1
    
    logger.info(f"保存完成: 成功{success}, 失败{fail}")


def main():
    logger.info(f"开始获取 {TARGET_DATE} 最新数据")
    
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
    if not stock_list_path.exists():
        logger.error("股票列表不存在")
        return
    
    stock_list = pl.read_parquet(stock_list_path)
    codes = stock_list['code'].to_list()
    logger.info(f"股票列表: {len(codes)} 只")
    
    # 获取数据
    all_data = fetch_all_data(codes)
    
    # 保存数据
    save_all_data(all_data)
    
    logger.info("全部完成!")


if __name__ == "__main__":
    main()
