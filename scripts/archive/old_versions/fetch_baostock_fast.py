#!/usr/bin/env python3
"""
使用Baostock获取完整的基本面数据 - 多进程加速版本
每个进程独立登录Baostock，避免线程安全问题
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time
from multiprocessing import Pool, cpu_count
from functools import partial

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 进程配置
MAX_WORKERS = min(4, cpu_count())  # 进程数，避免过多连接
BATCH_SIZE = 50  # 每批处理的股票数


def convert_code(code):
    """转换代码格式为baostock格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    return f"sz.{code}"


def init_worker():
    """
    进程初始化函数 - 每个进程登录一次Baostock
    """
    import baostock as bs
    global bs_module
    bs_module = bs
    lg = bs.login()
    if lg.error_code != '0':
        print(f"进程登录失败: {lg.error_msg}")


def fetch_kline_batch(codes_batch, start_date, end_date, kline_dir):
    """
    批量获取K线数据 - 在独立进程中执行
    """
    import baostock as bs

    results = []
    for code in codes_batch:
        try:
            bs_code = convert_code(code)

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )

            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    data_list.append({
                        'trade_date': row[0],
                        'code': code,
                        'open': float(row[2]) if row[2] else None,
                        'high': float(row[3]) if row[3] else None,
                        'low': float(row[4]) if row[4] else None,
                        'close': float(row[5]) if row[5] else None,
                        'preclose': float(row[6]) if row[6] else None,
                        'volume': int(row[7]) if row[7] else None,
                        'amount': float(row[8]) if row[8] else None,
                        'turnover': float(row[9]) if row[9] else None,
                        'pct_chg': float(row[10]) if row[10] else None,
                    })

                if data_list:
                    df = pd.DataFrame(data_list)
                    output_file = Path(kline_dir) / f"{code}.parquet"
                    pl.from_pandas(df).write_parquet(output_file)
                    results.append((code, True, len(data_list)))
                else:
                    results.append((code, False, 0))
            else:
                results.append((code, False, 0))

            time.sleep(0.01)  # 短暂休息

        except Exception as e:
            results.append((code, False, 0))

    return results


def fetch_kline_data_parallel(codes, days=365*3):
    """
    并行获取K线历史行情数据
    """
    print("\n" + "=" * 80)
    print(f"并行获取K线历史行情数据 (最近{days}天, 进程数: {MAX_WORKERS})")
    print("=" * 80)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    kline_dir = PROJECT_ROOT / "data" / "kline"
    kline_dir.mkdir(exist_ok=True)

    # 将股票代码分批
    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]

    success_count = 0
    failed_count = 0
    total_rows = 0

    # 使用进程池
    with Pool(processes=MAX_WORKERS, initializer=init_worker) as pool:
        # 使用imap_unordered获取实时进度
        fetch_func = partial(fetch_kline_batch, start_date=start_date, end_date=end_date, kline_dir=str(kline_dir))

        for i, results in enumerate(pool.imap_unordered(fetch_func, batches)):
            for code, success, rows in results:
                if success:
                    success_count += 1
                    total_rows += rows
                else:
                    failed_count += 1

            processed = min((i + 1) * BATCH_SIZE, len(codes))
            print(f"  已处理 {processed}/{len(codes)} 只, 成功 {success_count} 只, 累计 {total_rows} 行")

    print(f"\nK线数据获取完成: {success_count}/{len(codes)} 只, 共 {total_rows} 行")
    if failed_count > 0:
        print(f"失败: {failed_count} 只")

    return success_count


def fetch_valuation_batch(codes_batch, start_date, end_date, valuation_dir):
    """
    批量获取估值数据 - 在独立进程中执行
    """
    import baostock as bs

    results = []
    for code in codes_batch:
        try:
            bs_code = convert_code(code)

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )

            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    row = rs.get_row_data()
                    pe = float(row[2]) if row[2] and row[2] != '' else None
                    pb = float(row[3]) if row[3] and row[3] != '' else None
                    ps = float(row[4]) if row[4] and row[4] != '' else None
                    pcf = float(row[5]) if row[5] and row[5] != '' else None
                    turnover = float(row[6]) if row[6] and row[6] != '' else None

                    if pe and 0 < pe < 1000:
                        data_list.append({
                            'trade_date': row[0],
                            'code': code,
                            'pe_ttm': pe,
                            'pb': pb if pb and 0 < pb < 100 else None,
                            'ps_ttm': ps if ps and 0 < ps < 1000 else None,
                            'pcf': pcf if pcf and -1000 < pcf < 1000 else None,
                            'turnover': turnover,
                        })

                if data_list:
                    df_new = pd.DataFrame(data_list)
                    output_file = Path(valuation_dir) / f"{code}.parquet"

                    if output_file.exists():
                        df_existing = pl.read_parquet(output_file).to_pandas()
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
                        df_combined = df_combined.sort_values('trade_date')
                        df_new = df_combined

                    pl.from_pandas(df_new).write_parquet(output_file)
                    results.append((code, True, len(data_list)))
                else:
                    results.append((code, False, 0))
            else:
                results.append((code, False, 0))

            time.sleep(0.01)

        except Exception as e:
            results.append((code, False, 0))

    return results


def fetch_valuation_data_parallel(codes, days=365*3):
    """
    并行获取估值数据历史
    """
    print("\n" + "=" * 80)
    print(f"并行获取估值数据历史 (最近{days}天, 进程数: {MAX_WORKERS})")
    print("=" * 80)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    valuation_dir = PROJECT_ROOT / "data" / "fundamental" / "valuation_daily"
    valuation_dir.mkdir(exist_ok=True, parents=True)

    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]

    success_count = 0
    failed_count = 0

    with Pool(processes=MAX_WORKERS, initializer=init_worker) as pool:
        fetch_func = partial(fetch_valuation_batch, start_date=start_date, end_date=end_date, valuation_dir=str(valuation_dir))

        for i, results in enumerate(pool.imap_unordered(fetch_func, batches)):
            for code, success, rows in results:
                if success:
                    success_count += 1
                else:
                    failed_count += 1

            processed = min((i + 1) * BATCH_SIZE, len(codes))
            print(f"  已处理 {processed}/{len(codes)} 只, 成功 {success_count} 只")

    print(f"\n估值数据获取完成: {success_count}/{len(codes)} 只")
    if failed_count > 0:
        print(f"失败: {failed_count} 只")

    return success_count


def get_stock_list_from_baostock():
    """
    从Baostock获取所有股票列表
    """
    import baostock as bs

    print("\n" + "=" * 80)
    print("从Baostock获取所有股票列表")
    print("=" * 80)

    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return []

    try:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            print(f"  尝试日期: {date}")

            rs = bs.query_all_stock(day=date)

            stock_list = []
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                code_with_prefix = row[0]
                code = code_with_prefix.split('.')[-1] if '.' in code_with_prefix else code_with_prefix
                stock_list.append({
                    'code': code,
                    'name': row[2] if len(row) > 2 else '',
                    'ipo_date': row[3] if len(row) > 3 else '',
                })

            if len(stock_list) > 0:
                print(f"获取到 {len(stock_list)} 只股票 (日期: {date})")
                break

        if len(stock_list) == 0:
            print("警告: 无法获取股票列表")
            return []

        output_dir = PROJECT_ROOT / "data"
        output_dir.mkdir(exist_ok=True)

        df = pd.DataFrame(stock_list)
        output_file = output_dir / "stock_list.parquet"
        pl.from_pandas(df).write_parquet(output_file)
        print(f"股票列表已保存: {output_file}")

        return [s['code'] for s in stock_list]
    finally:
        bs.logout()


def main():
    """主函数"""
    print("=" * 80)
    print("使用Baostock获取股票数据 - 多进程加速版本")
    print(f"进程配置: {MAX_WORKERS}进程, 每批{BATCH_SIZE}只")
    print("=" * 80)

    # 获取股票列表（单进程）
    codes = get_stock_list_from_baostock()
    if not codes:
        print("错误: 无法获取股票列表")
        return

    print(f"\n总共 {len(codes)} 只股票需要处理")

    # 并行获取K线数据
    fetch_kline_data_parallel(codes)

    # 并行获取估值数据
    fetch_valuation_data_parallel(codes)

    print("\n" + "=" * 80)
    print("所有数据采集完成!")
    print("=" * 80)


if __name__ == "__main__":
    main()
