#!/usr/bin/env python3
"""
获取今天K线数据 - 批量查询版本（高效）
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
BATCH_SIZE = 500  # 每批处理的股票数


def format_code(code: str) -> str:
    """转换为baostock格式"""
    if '.' in code:
        return code
    if code.startswith('6'):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


def fetch_batch(codes: list, target_date: str) -> list:
    """批量获取一批股票数据"""
    results = []

    for code in codes:
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

            if data_list:
                results.append((code, data_list[0]))
        except:
            pass

    return results


def save_to_parquet(code_data, target_date):
    """保存数据到parquet"""
    code, data = code_data
    try:
        # baostock 返回的字段名是 'date'，需要重命名为 'trade_date'
        df = pd.DataFrame([data], columns=['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount'])
        df = df.rename(columns={'date': 'trade_date'})

        # 转换数值列
        for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 确保code字段只包含股票代码，不包含市场前缀
        df['code'] = code

        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"

        if kline_path.exists():
            try:
                existing_df = pl.read_parquet(kline_path)

                # 检查是否已存在该日期数据
                existing_dates = existing_df['trade_date'].to_list()
                if target_date in existing_dates:
                    return True

                # 确保列一致
                for col in STANDARD_COLUMNS:
                    if col not in existing_df.columns:
                        existing_df = existing_df.with_columns(pl.lit(None).alias(col))

                existing_df = existing_df.select(STANDARD_COLUMNS)
                new_df = pl.from_pandas(df)[STANDARD_COLUMNS]

                combined = pl.concat([existing_df, new_df])
                combined = combined.unique(subset=['trade_date']).sort('trade_date')
                combined.write_parquet(kline_path)
            except Exception as e:
                # 如果读取或合并失败，直接覆盖
                pl.from_pandas(df)[STANDARD_COLUMNS].write_parquet(kline_path)
        else:
            pl.from_pandas(df)[STANDARD_COLUMNS].write_parquet(kline_path)

        return True
    except Exception as e:
        print(f"保存 {code} 失败: {e}")
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

    # 过滤掉已存在的
    codes_to_fetch = []
    for code in codes:
        kline_path = PROJECT_ROOT / "data" / "kline" / f"{code}.parquet"
        if kline_path.exists():
            try:
                existing_df = pl.read_parquet(kline_path, columns=['trade_date'])
                if target_date not in existing_df['trade_date'].to_list():
                    codes_to_fetch.append(code)
            except:
                codes_to_fetch.append(code)
        else:
            codes_to_fetch.append(code)

    print(f"需要采集: {len(codes_to_fetch)} 只")

    if not codes_to_fetch:
        print("所有股票数据已存在，无需采集")
        return

    # 分批
    batches = [codes_to_fetch[i:i+BATCH_SIZE] for i in range(0, len(codes_to_fetch), BATCH_SIZE)]
    print(f"分成 {len(batches)} 批处理")

    # 登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return

    print("登录成功，开始采集...")

    # 采集数据
    all_results = []
    for i, batch in enumerate(batches):
        results = fetch_batch(batch, target_date)
        all_results.extend(results)
        print(f"批次 {i+1}/{len(batches)}: 获取 {len(results)}/{len(batch)} 只 | 总计: {len(all_results)}")

    print(f"\n获取完成: {len(all_results)} 只股票")

    # 保存数据
    print("开始保存数据...")
    success = 0
    for code_data in all_results:
        if save_to_parquet(code_data, target_date):
            success += 1

    print(f"保存完成: 成功 {success}/{len(all_results)}")

    bs.logout()


if __name__ == "__main__":
    main()
