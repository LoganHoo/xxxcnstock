#!/usr/bin/env python3
"""
使用 baostock 直接修复数据
"""
import sys
sys.path.insert(0, '.')

import polars as pl
import baostock as bs
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

KLINE_DIR = Path('data/kline')
TARGET_DATE = "2026-04-17"


def fetch_and_update(code: str) -> tuple:
    """获取并更新单只股票数据"""
    try:
        parquet_file = KLINE_DIR / f"{code}.parquet"
        if not parquet_file.exists():
            return code, 'no_file'
        
        # 转换代码格式
        code_bs = f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}"
        
        # 查询数据
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,code,open,high,low,close,volume,amount",
            start_date=TARGET_DATE,
            end_date=TARGET_DATE,
            frequency="d"
        )
        
        if rs.error_code != '0' or not rs.next():
            return code, 'no_data'
        
        row = rs.get_row_data()
        
        # 读取现有数据
        df = pl.read_parquet(parquet_file)
        
        # 检查是否已存在该日期
        existing_dates = df['trade_date'].cast(str).to_list()
        
        new_row = pl.DataFrame({
            'code': [code],
            'trade_date': [row[0]],
            'open': [float(row[2])],
            'high': [float(row[3])],
            'low': [float(row[4])],
            'close': [float(row[5])],
            'volume': [int(row[6])],
            'amount': [float(row[7])],
            'fetch_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })
        
        if TARGET_DATE in existing_dates:
            # 替换现有数据
            df_filtered = df.filter(pl.col('trade_date') != TARGET_DATE)
            df_merged = pl.concat([df_filtered, new_row])
        else:
            # 添加新数据
            df_merged = pl.concat([df, new_row])
        
        df_merged = df_merged.sort('trade_date')
        df_merged.write_parquet(parquet_file)
        
        return code, 'success'
        
    except Exception as e:
        return code, f'error: {e}'


def main():
    print("=" * 70)
    print(f"使用 baostock 修复 {TARGET_DATE} 数据")
    print("=" * 70)
    
    # 登录
    print("\n登录 baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    print("✅ 登录成功")
    
    # 获取所有股票代码
    all_files = list(KLINE_DIR.glob("*.parquet"))
    codes = [f.stem for f in all_files]
    
    print(f"\n共 {len(codes)} 只股票需要处理")
    print("开始并发采集...")
    print()
    
    # 并发采集
    results = {'success': 0, 'no_data': 0, 'error': 0, 'no_file': 0}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_and_update, code): code for code in codes}
        
        for i, future in enumerate(as_completed(futures), 1):
            code, status = future.result()
            
            if status == 'success':
                results['success'] += 1
                print(f"  ✅ {code}")
            elif status == 'no_data':
                results['no_data'] += 1
            elif status == 'no_file':
                results['no_file'] += 1
            else:
                results['error'] += 1
                print(f"  ❌ {code}: {status}")
            
            if i % 100 == 0:
                print(f"\n  进度: {i}/{len(codes)} | "
                      f"✅{results['success']} "
                      f"⚠️{results['no_data']} "
                      f"❌{results['error']}")
    
    # 登出
    bs.logout()
    
    print("\n" + "=" * 70)
    print("修复完成:")
    print(f"  ✅ 成功: {results['success']}")
    print(f"  ⚠️  无数据: {results['no_data']}")
    print(f"  ❌ 错误: {results['error']}")
    print(f"  🗑️  无文件: {results['no_file']}")
    print("=" * 70)
    
    # 验证
    print("\n验证修复结果...")
    test_codes = ['002119', '002219', '000001', '600519']
    for code in test_codes:
        try:
            df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
            dates = df['trade_date'].cast(str).to_list()
            if TARGET_DATE in dates:
                row = df.filter(pl.col('trade_date') == TARGET_DATE).to_dicts()[0]
                print(f"  ✅ {code}: {TARGET_DATE} 收¥{row['close']:.2f} 量{row['volume']:,}")
            else:
                print(f"  ❌ {code}: 无 {TARGET_DATE} 数据")
        except Exception as e:
            print(f"  ❌ {code}: {e}")


if __name__ == "__main__":
    main()
