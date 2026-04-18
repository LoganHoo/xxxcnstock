#!/usr/bin/env python3
"""
历史数据清洗脚本
检查和清洗K线数据中的异常值
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta, date
import shutil

print("=" * 80)
print("历史数据清洗")
print("=" * 80)
print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

kline_dir = Path('data/kline')
backup_dir = Path('data/kline_backup')

# 创建备份目录
backup_dir.mkdir(parents=True, exist_ok=True)

# 数据质量检查规则
CHECKS = {
    'price_positive': lambda df: (df['close'] > 0) & (df['open'] > 0) & (df['high'] > 0) & (df['low'] > 0),
    'price_logic': lambda df: (df['high'] >= df['low']) & (df['high'] >= df['close']) & (df['low'] <= df['close']),
    'volume_positive': lambda df: df['volume'] >= 0,
    'price_range': lambda df: (df['close'] < 10000) & (df['close'] > 0.01),  # 股价在合理范围
}

# 统计信息
stats = {
    'total_files': 0,
    'clean_files': 0,
    'fixed_files': 0,
    'removed_files': 0,
    'total_rows_before': 0,
    'total_rows_after': 0,
}

print("【阶段1】扫描数据文件")
print("-" * 80)

parquet_files = list(kline_dir.glob('*.parquet'))
stats['total_files'] = len(parquet_files)
print(f"发现 {len(parquet_files)} 个数据文件")
print()

print("【阶段2】检查和清洗数据")
print("-" * 80)

problematic_files = []

for i, filepath in enumerate(parquet_files, 1):
    code = filepath.stem
    try:
        df = pl.read_parquet(filepath)
        stats['total_rows_before'] += len(df)
        
        original_len = len(df)
        
        # 检查数据质量
        issues = []
        
        # 检查必要列是否存在
        required_cols = ['code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            issues.append(f"缺少列: {missing_cols}")
        
        if not issues:
            # 应用数据质量检查
            for check_name, check_func in CHECKS.items():
                try:
                    mask = check_func(df)
                    invalid_count = (~mask).sum()
                    if invalid_count > 0:
                        issues.append(f"{check_name}: {invalid_count} 条异常")
                        df = df.filter(mask)
                except Exception as e:
                    issues.append(f"{check_name}检查失败: {e}")
            
            # 检查数据日期连续性（可选）
            if 'trade_date' in df.columns and len(df) > 0:
                df = df.sort('trade_date')
                # 确保日期列是日期类型
                if df['trade_date'].dtype == pl.Utf8:
                    df = df.with_columns(pl.col('trade_date').str.to_date().alias('trade_date'))
                dates = df['trade_date'].to_list()
                # 检查是否有未来日期
                today = datetime.now().date()
                future_dates = [d for d in dates if isinstance(d, (datetime, date)) and d > today]
                if future_dates:
                    issues.append(f"未来日期: {len(future_dates)} 条")
                    df = df.filter(pl.col('trade_date') <= today)
        
        after_len = len(df)
        
        if issues:
            print(f"[{i}/{len(parquet_files)}] {code}: 发现问题")
            for issue in issues:
                print(f"  - {issue}")
            
            # 备份原文件
            backup_path = backup_dir / filepath.name
            shutil.copy2(filepath, backup_path)
            
            if after_len > 0:
                # 保存清洗后的数据
                df.write_parquet(filepath)
                stats['fixed_files'] += 1
                print(f"  ✓ 已清洗: {original_len} -> {after_len} 条")
            else:
                # 数据全部异常，删除文件
                filepath.unlink()
                stats['removed_files'] += 1
                print(f"  ✗ 数据全部异常，已删除")
            
            problematic_files.append({
                'code': code,
                'issues': issues,
                'original_count': original_len,
                'after_count': after_len
            })
        else:
            stats['clean_files'] += 1
            if i % 500 == 0:
                print(f"[{i}/{len(parquet_files)}] 已检查 {i} 个文件...")
        
        stats['total_rows_after'] += after_len
        
    except Exception as e:
        print(f"[{i}/{len(parquet_files)}] {code}: 读取失败 - {e}")
        problematic_files.append({
            'code': code,
            'issues': [f"读取失败: {e}"],
            'original_count': 0,
            'after_count': 0
        })

print()
print("=" * 80)
print("清洗统计")
print("=" * 80)
print(f"总文件数:     {stats['total_files']}")
print(f"干净文件:     {stats['clean_files']}")
print(f"修复文件:     {stats['fixed_files']}")
print(f"删除文件:     {stats['removed_files']}")
print(f"问题文件:     {len(problematic_files)}")
print(f"总记录(前):   {stats['total_rows_before']:,}")
print(f"总记录(后):   {stats['total_rows_after']:,}")
print(f"删除记录:     {stats['total_rows_before'] - stats['total_rows_after']:,}")
print()

if problematic_files:
    print("=" * 80)
    print("问题文件详情 (前10个)")
    print("=" * 80)
    for pf in problematic_files[:10]:
        print(f"\n股票代码: {pf['code']}")
        print(f"  问题: {', '.join(pf['issues'])}")
        print(f"  记录数: {pf['original_count']} -> {pf['after_count']}")
    
    # 保存问题文件列表
    report_path = Path('data/clean_report.json')
    import json
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(problematic_files, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n完整报告已保存: {report_path}")

print()
print("=" * 80)
print("清洗完成")
print("=" * 80)
