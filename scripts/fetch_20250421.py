#!/usr/bin/env python3
"""
批量获取2026-04-21日K线数据（最新数据）
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

logger = setup_logger("fetch_20250421")

TARGET_DATE = "2026-04-21"
BATCH_SIZE = 50

# 标准列名
STANDARD_COLUMNS = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']


def format_code(code: str) -> str:
    """转换为baostock格式"""
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_batch(codes: list) -> list:
    """获取一批股票的数据"""
    results = []
    
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"登录失败: {lg.error_msg}")
        return results
    
    for code in codes:
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
                results.append({
                    'code': code,
                    'data': data_list[0]
                })
        except Exception as e:
            logger.warning(f"{code} 获取失败: {e}")
    
    bs.logout()
    return results


def save_to_parquet(code: str, data: list) -> bool:
    """保存数据到parquet文件"""
    try:
        df = pd.DataFrame([data], columns=['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount'])
        
        # 转换数据类型
        df['trade_date'] = df['trade_date'].astype(str)
        df['code'] = df['code'].astype(str)
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['preclose'] = pd.to_numeric(df['preclose'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
        
        if kline_path.exists():
            existing_df = pl.read_parquet(kline_path)
            existing_dates = existing_df['trade_date'].to_list()
            
            if TARGET_DATE in existing_dates:
                return True
            
            # 确保列一致
            for col in STANDARD_COLUMNS:
                if col not in existing_df.columns:
                    existing_df = existing_df.with_columns(pl.lit(None).alias(col))
            
            existing_df = existing_df.select(STANDARD_COLUMNS)
            
            new_df = pl.from_pandas(df)
            for col in STANDARD_COLUMNS:
                if col not in new_df.columns:
                    new_df = new_df.with_columns(pl.lit(None).alias(col))
            new_df = new_df.select(STANDARD_COLUMNS)
            
            combined = pl.concat([existing_df, new_df])
            combined = combined.unique(subset=['trade_date'])
            combined = combined.sort('trade_date')
            combined.write_parquet(kline_path)
        else:
            new_df = pl.from_pandas(df)
            new_df.write_parquet(kline_path)
        
        return True
    except Exception as e:
        logger.error(f"{code} 保存失败: {e}")
        return False


def main():
    """主函数"""
    logger.info(f"开始获取 {TARGET_DATE} 最新数据")
    
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
    if not stock_list_path.exists():
        logger.error("股票列表不存在")
        return
    
    stock_list = pl.read_parquet(stock_list_path)
    codes = stock_list['code'].to_list()
    logger.info(f"股票列表: {len(codes)} 只")
    
    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    logger.info(f"分成 {len(batches)} 批处理")
    
    success_count = 0
    fail_count = 0
    no_data_count = 0
    
    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"进度: {i}/{len(batches)} 批 | 成功:{success_count} 失败:{fail_count} 无数据:{no_data_count}")
        
        results = fetch_batch(batch)
        
        if not results:
            no_data_count += len(batch)
        
        for result in results:
            if save_to_parquet(result['code'], result['data']):
                success_count += 1
            else:
                fail_count += 1
    
    logger.info(f"\n完成!")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {fail_count}")
    logger.info(f"无数据: {no_data_count}")


if __name__ == "__main__":
    main()
