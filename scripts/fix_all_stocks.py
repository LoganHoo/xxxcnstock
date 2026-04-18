#!/usr/bin/env python3
"""
修复所有股票的 2026-04-17 数据
使用 baostock 直接修复
"""
import sys
sys.path.insert(0, '.')

import polars as pl
import baostock as bs
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

KLINE_DIR = Path('data/kline')
TARGET_DATE = "2026-04-17"

# 线程本地存储
thread_local = threading.local()


def get_bs():
    """获取线程本地的 baostock 连接"""
    if not hasattr(thread_local, 'bs_logged_in'):
        lg = bs.login()
        if lg.error_code == '0':
            thread_local.bs_logged_in = True
        else:
            thread_local.bs_logged_in = False
    return thread_local.bs_logged_in


def fix_stock(code: str) -> tuple:
    """修复单只股票"""
    try:
        parquet_file = KLINE_DIR / f"{code}.parquet"
        if not parquet_file.exists():
            return code, 'no_file'
        
        # 确保已登录
        if not get_bs():
            return code, 'login_failed'
        
        # 读取现有数据获取列结构
        df = pl.read_parquet(parquet_file)
        existing_columns = df.columns
        
        # 转换代码格式
        code_bs = f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}"
        
        # 查询数据
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
            start_date=TARGET_DATE,
            end_date=TARGET_DATE,
            frequency="d"
        )
        
        if rs.error_code != '0':
            return code, f'query_error:{rs.error_msg}'
        
        if not rs.next():
            return code, 'no_data'
        
        row = rs.get_row_data()
        
        # 创建新数据行
        new_data = {
            'trade_date': row[0],
            'code': code,
            'open': float(row[2]) if row[2] else 0.0,
            'high': float(row[3]) if row[3] else 0.0,
            'low': float(row[4]) if row[4] else 0.0,
            'close': float(row[5]) if row[5] else 0.0,
            'preclose': float(row[6]) if row[6] else 0.0,
            'volume': int(float(row[7])) if row[7] else 0,
            'amount': float(row[8]) if row[8] else 0.0,
            'turnover': float(row[9]) if row[9] else 0.0,
            'pct_chg': float(row[10]) if row[10] else 0.0,
            'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        new_row = pl.DataFrame([new_data])
        new_row = new_row.select(existing_columns)
        
        # 检查是否已存在该日期
        existing_dates = df['trade_date'].cast(str).to_list()
        
        if TARGET_DATE in existing_dates:
            df_filtered = df.filter(pl.col('trade_date') != TARGET_DATE)
            df_merged = pl.concat([df_filtered, new_row])
        else:
            df_merged = pl.concat([df, new_row])
        
        df_merged = df_merged.sort('trade_date')
        df_merged.write_parquet(parquet_file)
        
        return code, 'success'
        
    except Exception as e:
        return code, f'error:{str(e)[:50]}'


def main():
    print("=" * 70)
    print(f"修复所有股票 {TARGET_DATE} 数据")
    print("=" * 70)
    
    # 获取所有股票代码
    all_files = list(KLINE_DIR.glob("*.parquet"))
    codes = [f.stem for f in all_files]
    
    print(f"\n共 {len(codes)} 只股票需要处理")
    print("使用 10 线程并发采集...\n")
    
    # 统计
    stats = {
        'success': 0,
        'no_data': 0,
        'error': 0,
        'no_file': 0,
        'login_failed': 0
    }
    
    # 并发处理
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fix_stock, code): code for code in codes}
        
        for i, future in enumerate(as_completed(futures), 1):
            code, status = future.result()
            
            if status == 'success':
                stats['success'] += 1
            elif status == 'no_data':
                stats['no_data'] += 1
            elif status == 'no_file':
                stats['no_file'] += 1
            elif status == 'login_failed':
                stats['login_failed'] += 1
            else:
                stats['error'] += 1
                if stats['error'] <= 5:  # 只显示前5个错误
                    print(f"  ❌ {code}: {status}")
            
            # 每100个显示进度
            if i % 100 == 0 or i == len(codes):
                print(f"  进度: {i}/{len(codes)} | "
                      f"✅{stats['success']} "
                      f"⚠️{stats['no_data']} "
                      f"❌{stats['error']}")
    
    print("\n" + "=" * 70)
    print("修复完成:")
    print(f"  ✅ 成功: {stats['success']}")
    print(f"  ⚠️  无数据: {stats['no_data']}")
    print(f"  ❌ 错误: {stats['error']}")
    print(f"  🗑️  无文件: {stats['no_file']}")
    print(f"  🔒 登录失败: {stats['login_failed']}")
    print("=" * 70)
    
    # 登出
    bs.logout()
    
    # 验证几只股票
    print("\n验证结果:")
    test_codes = ['002119', '002219', '000001', '600519']
    for code in test_codes:
        try:
            df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
            latest = df[-1].to_dicts()[0]
            if str(latest['trade_date']) == TARGET_DATE:
                print(f"  ✅ {code}: {TARGET_DATE} 收¥{latest['close']:.2f} 量{latest['volume']:,}")
            else:
                print(f"  ⚠️  {code}: 最新日期 {latest['trade_date']} (非 {TARGET_DATE})")
        except Exception as e:
            print(f"  ❌ {code}: 读取失败 - {e}")


if __name__ == "__main__":
    main()
