#!/usr/bin/env python3
"""
修复损坏的数据 - 删除2026-04-17盘中采集的数据，重新采集收盘数据
"""
import sys
sys.path.insert(0, '.')

import polars as pl
from pathlib import Path
from datetime import datetime
import subprocess

KLINE_DIR = Path('data/kline')
CORRUPTED_DATE = "2026-04-17"


def remove_corrupted_date():
    """删除所有股票中2026-04-17的数据"""
    print("=" * 70)
    print(f"删除 {CORRUPTED_DATE} 的盘中采集数据")
    print("=" * 70)
    
    all_files = list(KLINE_DIR.glob("*.parquet"))
    fixed_count = 0
    error_count = 0
    
    for i, pf in enumerate(all_files, 1):
        code = pf.stem
        try:
            df = pl.read_parquet(pf)
            
            # 检查是否包含损坏的日期
            if CORRUPTED_DATE in df['trade_date'].cast(str).to_list():
                # 删除该日期的数据
                df_cleaned = df.filter(pl.col('trade_date') != CORRUPTED_DATE)
                
                if len(df_cleaned) > 0:
                    df_cleaned.write_parquet(pf)
                    fixed_count += 1
                    print(f"  ✅ {code}: 已删除 {CORRUPTED_DATE} 数据")
                else:
                    # 如果删除后没有数据了，删除整个文件
                    pf.unlink()
                    print(f"  🗑️  {code}: 文件已删除（仅剩损坏数据）")
                    fixed_count += 1
        except Exception as e:
            error_count += 1
            print(f"  ❌ {code}: 处理失败 - {e}")
        
        if i % 100 == 0:
            print(f"  进度: {i}/{len(all_files)}")
    
    print()
    print(f"修复完成: {fixed_count} 个文件已处理")
    if error_count > 0:
        print(f"错误: {error_count} 个文件处理失败")
    print("=" * 70)


def verify_removal():
    """验证删除是否成功"""
    print("\n验证删除结果...")
    print("-" * 70)
    
    test_codes = ['002119', '002219', '000001', '600519']
    all_clean = True
    
    for code in test_codes:
        try:
            df = pl.read_parquet(KLINE_DIR / f"{code}.parquet")
            dates = df['trade_date'].cast(str).to_list()
            
            if CORRUPTED_DATE in dates:
                print(f"  ❌ {code}: 仍包含 {CORRUPTED_DATE} 数据")
                all_clean = False
            else:
                latest_date = df['trade_date'].max()
                print(f"  ✅ {code}: 已清理，最新日期: {latest_date}")
        except Exception as e:
            print(f"  ⚠️  {code}: 文件不存在或读取失败")
    
    print("-" * 70)
    return all_clean


def re_collect_data():
    """重新采集数据"""
    print("\n" + "=" * 70)
    print(f"重新采集 {CORRUPTED_DATE} 收盘数据")
    print("=" * 70)
    print()
    print("注意: 当前时间检查...")
    
    now = datetime.now()
    print(f"  当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查是否在收盘后
    if now.hour < 15:
        print()
        print("⚠️ 警告: 当前是盘中时间，不建议现在采集！")
        print("  建议在今天 15:00 收盘后执行以下命令:")
        print(f"  python scripts/pipeline/data_collect.py --date {CORRUPTED_DATE}")
        return False
    else:
        print("  ✅ 当前时间已过收盘时间，可以采集")
        print()
        
        # 执行采集
        cmd = f"python scripts/pipeline/data_collect.py --date {CORRUPTED_DATE}"
        print(f"执行: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=False)
        return result.returncode == 0


def main():
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 20 + "数据修复工具" + " " * 36 + "║")
    print("╚" + "=" * 68 + "╝")
    print()
    
    # 第1步: 删除损坏数据
    remove_corrupted_date()
    
    # 第2步: 验证删除
    if verify_removal():
        print("\n✅ 损坏数据已清理完成")
    else:
        print("\n⚠️ 部分数据未清理干净")
    
    # 第3步: 重新采集
    print()
    print("是否现在重新采集数据？")
    print("  1. 如果当前已过15:00，可以立即采集")
    print("  2. 如果当前是盘中，建议15:00后再采集")
    print()
    
    # 自动判断
    now = datetime.now()
    if now.hour >= 15:
        print("检测到已过收盘时间，自动开始采集...")
        if re_collect_data():
            print("\n✅ 数据采集完成")
        else:
            print("\n❌ 数据采集失败")
    else:
        print(f"当前是盘中时间 ({now.hour}:{now.minute:02d})")
        print("请在今天15:00收盘后手动执行:")
        print(f"  python scripts/pipeline/data_collect.py --date {CORRUPTED_DATE}")


if __name__ == "__main__":
    main()
