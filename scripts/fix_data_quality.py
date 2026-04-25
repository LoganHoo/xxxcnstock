#!/usr/bin/env python3
"""
修复数据质量问题
"""
import sys
from pathlib import Path
import polars as pl
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

def fix_duplicate_stock_codes():
    """修复股票列表重复代码"""
    print('='*80)
    print('🔧 修复1: 股票列表重复代码')
    print('='*80)

    stock_list_path = Path('data/stock_list.parquet')
    if not stock_list_path.exists():
        print('❌ 股票列表文件不存在')
        return False

    stock_list = pl.read_parquet(stock_list_path)
    original_count = len(stock_list)
    print(f'原始股票数: {original_count}')

    # 去重，保留第一条
    stock_list_unique = stock_list.unique(subset=['code'], keep='first')
    new_count = len(stock_list_unique)
    removed_count = original_count - new_count

    print(f'去重后股票数: {new_count}')
    print(f'移除重复: {removed_count} 只')

    # 保存
    stock_list_unique.write_parquet(stock_list_path)
    print(f'✅ 已保存到: {stock_list_path}')
    return True

def fix_volume_nulls():
    """修复volume空值"""
    print()
    print('='*80)
    print('🔧 修复2: volume空值')
    print('='*80)

    scores_path = Path('data/enhanced_scores_full.parquet')
    if not scores_path.exists():
        print('❌ 评分文件不存在')
        return False

    scores = pl.read_parquet(scores_path)
    print(f'总股票数: {len(scores)}')

    # 检查空值
    null_count = scores['volume'].is_null().sum()
    print(f'volume空值: {null_count}')

    if null_count > 0:
        # 填充空值为0
        scores = scores.with_columns(
            pl.col('volume').fill_null(0)
        )

        # 验证
        new_null_count = scores['volume'].is_null().sum()
        print(f'修复后空值: {new_null_count}')

        # 保存
        scores.write_parquet(scores_path)
        print(f'✅ 已保存到: {scores_path}')
    else:
        print('✅ 无需修复')

    return True

def cleanup_extra_kline_files():
    """清理多余的K线数据文件"""
    print()
    print('='*80)
    print('🔧 修复3: 清理多余K线数据')
    print('='*80)

    stock_list_path = Path('data/stock_list.parquet')
    kline_dir = Path('data/kline')

    if not stock_list_path.exists() or not kline_dir.exists():
        print('❌ 必要文件不存在')
        return False

    # 读取股票列表
    stock_list = pl.read_parquet(stock_list_path)
    stock_codes = set(stock_list['code'].to_list())
    print(f'股票列表: {len(stock_codes)} 只')

    # 扫描K线文件
    parquet_files = list(kline_dir.glob('*.parquet'))
    print(f'K线文件: {len(parquet_files)} 个')

    # 找出多余的文件
    extra_files = []
    for f in parquet_files:
        code = f.stem
        if code not in stock_codes:
            extra_files.append(f)

    print(f'多余文件: {len(extra_files)} 个')

    if len(extra_files) > 0:
        print(f'示例: {[f.name for f in extra_files[:10]]}')

        # 移动而不是删除，以防万一
        backup_dir = Path('data/kline_backup')
        backup_dir.mkdir(exist_ok=True)

        moved_count = 0
        for f in extra_files:
            try:
                f.rename(backup_dir / f.name)
                moved_count += 1
            except Exception as e:
                print(f'移动失败 {f.name}: {e}')

        print(f'✅ 已移动 {moved_count} 个文件到: {backup_dir}')
    else:
        print('✅ 无需清理')

    return True

def check_missing_kline():
    """检查缺失K线数据的股票"""
    print()
    print('='*80)
    print('📋 检查: 缺失K线数据的股票')
    print('='*80)

    stock_list_path = Path('data/stock_list.parquet')
    kline_dir = Path('data/kline')

    if not stock_list_path.exists() or not kline_dir.exists():
        print('❌ 必要文件不存在')
        return []

    # 读取股票列表
    stock_list = pl.read_parquet(stock_list_path)
    stock_codes = set(stock_list['code'].to_list())

    # 扫描K线文件
    parquet_files = list(kline_dir.glob('*.parquet'))
    kline_codes = set([f.stem for f in parquet_files])

    # 找出缺失的
    missing_codes = stock_codes - kline_codes
    print(f'缺失K线数据: {len(missing_codes)} 只')

    if len(missing_codes) > 0:
        print(f'示例: {list(missing_codes)[:20]}')

    return list(missing_codes)

def main():
    print('='*80)
    print('🔧 数据质量修复工具')
    print('='*80)
    print(f'开始时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # 1. 修复重复代码
    fix_duplicate_stock_codes()

    # 2. 修复volume空值
    fix_volume_nulls()

    # 3. 清理多余K线文件
    cleanup_extra_kline_files()

    # 4. 检查缺失K线
    missing_codes = check_missing_kline()

    print()
    print('='*80)
    print('✅ 修复完成')
    print('='*80)

    if missing_codes:
        print(f'\n⚠️  还有 {len(missing_codes)} 只股票缺失K线数据')
        print('建议运行: python scripts/fetch_today_batch.py --date 2026-04-22')
        print('来补充这些数据')

if __name__ == '__main__':
    main()
