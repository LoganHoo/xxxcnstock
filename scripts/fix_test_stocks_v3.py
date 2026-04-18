#!/usr/bin/env python3
"""
修复测试股票数据 - 保持 baostock 登录状态
"""
import sys
sys.path.insert(0, '.')

import polars as pl
import baostock as bs
from pathlib import Path
from datetime import datetime

KLINE_DIR = Path('data/kline')
TARGET_DATE = "2026-04-17"


def fix_stock(code: str) -> bool:
    """修复单只股票 - 假设已登录"""
    try:
        parquet_file = KLINE_DIR / f"{code}.parquet"
        if not parquet_file.exists():
            print(f"  ❌ {code}: 文件不存在")
            return False
        
        # 读取现有数据获取列结构
        df = pl.read_parquet(parquet_file)
        existing_columns = df.columns
        
        # 转换代码格式
        code_bs = f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}"
        
        # 查询数据
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,code,open,high,low,close,preclose,volume,amount,turnover,pctChg",
            start_date=TARGET_DATE,
            end_date=TARGET_DATE,
            frequency="d"
        )
        
        if rs.error_code != '0':
            print(f"  ❌ {code}: 查询错误 {rs.error_msg}")
            return False
        
        if not rs.next():
            print(f"  ❌ {code}: baostock 无数据")
            return False
        
        row = rs.get_row_data()
        
        # 创建新数据行，匹配所有列
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
        
        # 确保列顺序一致
        new_row = new_row.select(existing_columns)
        
        # 检查是否已存在该日期
        existing_dates = df['trade_date'].cast(str).to_list()
        
        if TARGET_DATE in existing_dates:
            # 替换现有数据
            df_filtered = df.filter(pl.col('trade_date') != TARGET_DATE)
            df_merged = pl.concat([df_filtered, new_row])
            action = "替换"
        else:
            # 添加新数据
            df_merged = pl.concat([df, new_row])
            action = "添加"
        
        df_merged = df_merged.sort('trade_date')
        df_merged.write_parquet(parquet_file)
        
        print(f"  ✅ {code}: {action} {TARGET_DATE} 数据 "
              f"收¥{new_data['close']:.2f} 量{new_data['volume']:,}")
        return True
        
    except Exception as e:
        print(f"  ❌ {code}: 错误 - {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 70)
    print(f"修复测试股票 {TARGET_DATE} 数据")
    print("=" * 70)
    
    # 登录
    print("\n登录 baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    print("✅ 登录成功\n")
    
    try:
        # 测试股票
        test_codes = ['002119', '002219', '000001', '600519']
        
        success_count = 0
        for code in test_codes:
            if fix_stock(code):
                success_count += 1
        
        print(f"\n修复完成: {success_count}/{len(test_codes)}")
    finally:
        # 登出
        bs.logout()
        print("\n已登出 baostock")
    
    print("=" * 70)
    
    # 验证
    print("\n验证结果:")
    for code in test_codes:
        try:
            df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
            latest = df[-1].to_dicts()[0]
            print(f"  {code}: 最新日期 {latest['trade_date']}, 收盘 ¥{latest['close']:.2f}, 成交量 {latest['volume']:,}")
        except Exception as e:
            print(f"  {code}: 读取失败 - {e}")


if __name__ == "__main__":
    main()
