#!/usr/bin/env python3
"""
获取今天K线数据 - 单进程版本（稳定）
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
import baostock as bs
from datetime import datetime
import argparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

STANDARD_COLUMNS = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']


def format_code(code: str) -> str:
    """转换为baostock格式"""
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_and_save(code: str, target_date: str) -> bool:
    """获取并保存单只股票数据"""
    try:
        bs_code = format_code(code)
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,preclose,volume,amount",
            start_date=target_date,
            end_date=target_date,
            frequency="d",
            adjustflag="3"
        )

        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return False

        # 创建DataFrame
        df = pd.DataFrame(data_list, columns=['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount'])
        df = df.rename(columns={'date': 'trade_date'})

        # 转换数值列
        for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"

        if kline_path.exists():
            existing_df = pl.read_parquet(kline_path)

            if target_date in existing_df['trade_date'].to_list():
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
    except Exception as e:
        return False


def main():
    parser = argparse.ArgumentParser(description='获取今天K线数据')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'), help='目标日期 (YYYY-MM-DD)')
    args = parser.parse_args()

    target_date = args.date
    print(f"开始获取 {target_date} 数据")

    # 读取股票列表
    stock_list_path = PROJECT_ROOT / "data" / "stock_list.parquet"
    stock_list = pl.read_parquet(stock_list_path)
    codes = stock_list['code'].to_list()
    print(f"股票列表: {len(codes)} 只")

    # 登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return

    print("登录成功，开始采集...")

    # 采集数据
    success_count = 0
    fail_count = 0
    skip_count = 0

    for i, code in enumerate(codes):
        # 检查是否已存在
        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
        if kline_path.exists():
            try:
                existing_df = pl.read_parquet(kline_path, columns=['trade_date'])
                if target_date in existing_df['trade_date'].to_list():
                    skip_count += 1
                    continue
            except:
                pass

        # 获取数据
        if fetch_and_save(code, target_date):
            success_count += 1
        else:
            fail_count += 1

        # 进度显示
        if (i + 1) % 100 == 0:
            print(f"进度: {i+1}/{len(codes)} | 成功: {success_count} | 跳过: {skip_count} | 失败: {fail_count}")

    bs.logout()

    print(f"\n采集完成!")
    print(f"  成功: {success_count}")
    print(f"  跳过(已存在): {skip_count}")
    print(f"  失败: {fail_count}")


if __name__ == "__main__":
    main()
