#!/usr/bin/env python3
"""
最新交易数据测试报告

生成数据新鲜度详细报告
"""
import sys
from pathlib import Path
import polars as pl
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

def main():
    print('=' * 60)
    print('🧪 最新交易数据测试报告')
    print('=' * 60)
    
    # 1. 统计K线文件
    kline_dir = Path('data/kline')
    parquet_files = list(kline_dir.glob('*.parquet'))
    print(f'\n📁 K线文件统计:')
    print(f'  总文件数: {len(parquet_files)}')
    
    # 2. 检查最新日期
    target_date = '2026-04-24'
    latest_count = 0
    outdated_count = 0
    error_count = 0
    
    print(f'\n📊 数据新鲜度检查（目标日期: {target_date}）:')
    
    for f in parquet_files:
        try:
            df = pl.read_parquet(f)
            if 'date' in df.columns:
                latest = str(df['date'].max())
            elif 'trade_date' in df.columns:
                latest = str(df['trade_date'].max())
            else:
                error_count += 1
                continue
                
            if latest == target_date:
                latest_count += 1
            else:
                outdated_count += 1
        except:
            error_count += 1
    
    print(f'  ✅ 最新: {latest_count} ({latest_count/len(parquet_files)*100:.1f}%)')
    print(f'  ⚠️  过期: {outdated_count}')
    print(f'  ❌ 错误: {error_count}')
    
    # 3. 显示样本
    print(f'\n📈 样本数据（最新3条）:')
    sample_codes = ['000001', '000002', '600000']
    for code in sample_codes:
        f = kline_dir / f'{code}.parquet'
        if f.exists():
            df = pl.read_parquet(f)
            print(f'\n  {code}:')
            print(df.tail(3).to_pandas().to_string())
    
    print('\n' + '=' * 60)
    print('✅ 测试完成')
    print('=' * 60)

if __name__ == '__main__':
    main()
