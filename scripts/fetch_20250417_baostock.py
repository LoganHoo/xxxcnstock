#!/usr/bin/env python3
"""
使用Baostock重新采集2026-04-17的K线数据
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import baostock as bs
import polars as pl
from pathlib import Path
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 登录Baostock
lg = bs.login()
logger.info(f"Baostock登录结果: {lg.error_code} - {lg.error_msg}")

# 读取股票列表
stock_list_file = Path('/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet')
if stock_list_file.exists():
    stock_df = pl.read_parquet(stock_list_file)
    stock_codes = stock_df['code'].to_list()
else:
    stock_codes = []

logger.info(f"股票数量: {len(stock_codes)}")

# 目标日期
target_date = '2026-04-17'

success_count = 0
fail_count = 0
skip_count = 0

kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
kline_dir.mkdir(parents=True, exist_ok=True)

# 批量处理，每100只记录一次进度
batch_size = 100
total = len(stock_codes)

for i, code in enumerate(stock_codes):
    try:
        if code.startswith('6'):
            bs_code = f"sh.{code}"
        else:
            bs_code = f"sz.{code}"
        
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
            start_date=target_date,
            end_date=target_date
        )
        
        if rs.error_code != '0':
            fail_count += 1
            continue
        
        if not rs.next():
            skip_count += 1
            continue
        
        row = rs.get_row_data()
        
        trade_date = row[0]
        code_short = row[1].split('.')[-1]
        open_price = float(row[2]) if row[2] else 0
        high = float(row[3]) if row[3] else 0
        low = float(row[4]) if row[4] else 0
        close = float(row[5]) if row[5] else 0
        preclose = float(row[6]) if row[6] else 0
        volume = int(float(row[7])) if row[7] else 0
        amount = float(row[8]) if row[8] else 0
        turnover = float(row[9]) if row[9] else 0
        pct_chg = float(row[10]) if row[10] else 0
        
        parquet_file = kline_dir / f"{code_short}.parquet"
        
        if parquet_file.exists():
            # 读取现有数据
            df = pl.read_parquet(parquet_file)
            
            # 创建新行数据 - 按照原有列顺序
            new_row = pl.DataFrame({
                'trade_date': [trade_date],
                'code': [code_short],
                'open': [open_price],
                'high': [high],
                'low': [low],
                'close': [close],
                'preclose': [preclose],
                'volume': [volume],
                'amount': [amount],
                'turnover': [turnover],
                'pct_chg': [pct_chg],
                'fetch_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            
            # 添加新数据并排序
            df = pl.concat([df, new_row])
            df = df.sort('trade_date')
            df.write_parquet(parquet_file)
        else:
            # 创建新文件
            df = pl.DataFrame({
                'trade_date': [trade_date],
                'code': [code_short],
                'open': [open_price],
                'high': [high],
                'low': [low],
                'close': [close],
                'preclose': [preclose],
                'volume': [volume],
                'amount': [amount],
                'turnover': [turnover],
                'pct_chg': [pct_chg],
                'fetch_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            df.write_parquet(parquet_file)
        
        success_count += 1
        
        # 每100只记录一次进度
        if (i + 1) % batch_size == 0:
            logger.info(f"进度: {i+1}/{total}, 成功: {success_count}, 失败: {fail_count}, 跳过: {skip_count}")
        
    except Exception as e:
        logger.error(f"处理 {code} 异常: {e}")
        fail_count += 1
    
    # 限速，避免请求过快
    time.sleep(0.03)

bs.logout()

logger.info(f"\n采集完成:")
logger.info(f"  总计: {total}只")
logger.info(f"  成功: {success_count}只")
logger.info(f"  失败: {fail_count}只")
logger.info(f"  跳过(无数据): {skip_count}只")
