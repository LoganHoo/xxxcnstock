#!/usr/bin/env python3
"""
补齐2026-04-20日K线数据 - 使用同步方式
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime
import baostock as bs

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import setup_logger

logger = setup_logger("fetch_20250420")

TARGET_DATE = "2026-04-20"


def format_code(code: str) -> str:
    """转换为baostock格式"""
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_single_stock_kline(code: str, name: str = "") -> bool:
    """获取单只股票指定日期的K线数据"""
    try:
        # 转换代码格式
        bs_code = format_code(code)
        
        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"登录失败: {lg.error_msg}")
            return False
        
        # 获取K线数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus",
            start_date=TARGET_DATE,
            end_date=TARGET_DATE,
            frequency="d",
            adjustflag="3"
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        bs.logout()
        
        if not data_list:
            logger.warning(f"{code} 无 {TARGET_DATE} 数据")
            return False
        
        # 转换为DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 转换数据类型
        df['date'] = df['date'].astype(str)
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['preclose'] = pd.to_numeric(df['preclose'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['turn'] = pd.to_numeric(df['turn'], errors='coerce')
        
        # 转换列名
        df = df.rename(columns={
            'date': 'trade_date',
            'turn': 'turnover'
        })
        
        # 读取现有K线文件
        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
        
        if kline_path.exists():
            existing_df = pl.read_parquet(kline_path)
            existing_dates = existing_df['trade_date'].to_list()
            
            # 检查是否已有该日期
            if TARGET_DATE in existing_dates:
                logger.info(f"{code} 已有 {TARGET_DATE} 数据，跳过")
                return True
            
            # 合并数据 - 转换为polars
            new_df = pl.from_pandas(df)
            combined = pl.concat([existing_df, new_df])
            # 去重
            combined = combined.unique(subset=['trade_date'])
            combined = combined.sort('trade_date')
            combined.write_parquet(kline_path)
        else:
            # 新建文件
            new_df = pl.from_pandas(df)
            new_df.write_parquet(kline_path)
        
        logger.info(f"{code} {name} {TARGET_DATE} 数据已保存")
        return True
        
    except Exception as e:
        logger.error(f"{code} 处理失败: {e}")
        return False


def main():
    """主函数"""
    logger.info(f"开始补齐 {TARGET_DATE} 数据")
    
    # 读取股票列表
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
    if not stock_list_path.exists():
        logger.error("股票列表不存在")
        return
    
    stock_list = pl.read_parquet(stock_list_path)
    logger.info(f"股票列表: {len(stock_list)} 只")
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for i, row in enumerate(stock_list.iter_rows(named=True)):
        code = row.get('code')
        name = row.get('name', '')
        
        if i % 100 == 0:
            logger.info(f"进度: {i}/{len(stock_list)} | 成功:{success_count} 失败:{fail_count} 跳过:{skip_count}")
        
        result = fetch_single_stock_kline(code, name)
        if result:
            success_count += 1
        else:
            fail_count += 1
    
    logger.info(f"\n完成!")
    logger.info(f"成功: {success_count}")
    logger.info(f"失败: {fail_count}")
    logger.info(f"跳过: {skip_count}")


if __name__ == "__main__":
    main()
