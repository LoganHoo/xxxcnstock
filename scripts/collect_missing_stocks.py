#!/usr/bin/env python3
"""
100% 数据采集 - 补充缺失股票
使用同步模式，更稳定可靠
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Set, List, Dict
import concurrent.futures

import pandas as pd
import polars as pl

from core.logger import setup_logger
from services.data_service.datasource.providers import BaostockProvider, TencentProvider

logger = setup_logger("collect_missing")


def get_missing_codes() -> List[str]:
    """获取缺失的股票代码"""
    project_root = Path('/Volumes/Xdata/workstation/xxxcnstock')

    # 加载有效股票列表
    valid_list_file = project_root / 'data' / 'valid_stock_list.json'
    with open(valid_list_file, 'r') as f:
        all_codes = set(json.load(f).get('codes', []))

    # 获取已有数据
    kline_dir = project_root / 'data' / 'kline'
    existing_files = list(kline_dir.glob('*.parquet'))
    existing_codes = {f.stem for f in existing_files}

    # 缺失的代码
    missing = sorted(list(all_codes - existing_codes))

    print(f"=" * 70)
    print(f"100% 数据采集 - 补充缺失")
    print(f"=" * 70)
    print(f"目标总数: {len(all_codes)}")
    print(f"已有数据: {len(existing_codes)}")
    print(f"缺失数量: {len(missing)}")
    print(f"=" * 70)

    return missing


def collect_single_stock(code: str, date: str) -> Dict:
    """
    采集单只股票
    优先baostock，失败则用tencent
    """
    start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')

    # 尝试baostock
    try:
        provider = BaostockProvider()
        df = provider.fetch_kline_sync(code, start_date, date)

        if df is not None and len(df) > 0:
            # 保存
            output_path = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline') / f"{code}.parquet"
            if isinstance(df, pd.DataFrame):
                df = pl.from_pandas(df)
            df.write_parquet(output_path)

            return {'code': code, 'success': True, 'source': 'baostock', 'records': len(df)}
    except Exception as e:
        pass

    # 尝试tencent
    try:
        provider = TencentProvider()
        df = provider.fetch_kline_sync(code, start_date, date)

        if df is not None and len(df) > 0:
            output_path = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline') / f"{code}.parquet"
            if isinstance(df, pd.DataFrame):
                df = pl.from_pandas(df)
            df.write_parquet(output_path)

            return {'code': code, 'success': True, 'source': 'tencent', 'records': len(df)}
    except Exception as e:
        pass

    return {'code': code, 'success': False, 'source': '', 'records': 0, 'error': 'all_failed'}


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--workers', type=int, default=5)
    parser.add_argument('--limit', type=int, default=0, help='限制采集数量，0表示全部')
    args = parser.parse_args()

    # 获取缺失代码
    missing_codes = get_missing_codes()

    if not missing_codes:
        print("\n✅ 所有股票数据已完整！100% 采集完成！")
        return

    if args.limit > 0:
        missing_codes = missing_codes[:args.limit]
        print(f"\n本次采集限制: {args.limit} 只")

    print(f"\n开始采集 {len(missing_codes)} 只股票...")
    print(f"并发数: {args.workers}")
    print("-" * 70)

    # 统计
    success_count = 0
    failed_count = 0
    by_source = {'baostock': 0, 'tencent': 0}

    # 使用线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(collect_single_stock, code, args.date): code
            for code in missing_codes
        }

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            code = result['code']

            if result['success']:
                success_count += 1
                by_source[result['source']] += 1
                status = f"✅ [{result['source']}]"
            else:
                failed_count += 1
                status = "❌"

            # 每10只显示进度
            if (i + 1) % 10 == 0 or i == len(missing_codes) - 1:
                progress = (i + 1) / len(missing_codes) * 100
                print(f"进度: {i+1}/{len(missing_codes)} ({progress:.1f}%) | "
                      f"成功: {success_count} | 失败: {failed_count}")

    # 最终结果
    print("\n" + "=" * 70)
    print("采集完成")
    print("=" * 70)
    print(f"总目标: {len(missing_codes)}")
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print(f"成功率: {success_count/len(missing_codes)*100:.2f}%")
    print(f"\n数据源分布:")
    for source, count in by_source.items():
        print(f"  {source}: {count}")

    # 验证完整性
    print("\n" + "=" * 70)
    print("验证完整性...")
    print("=" * 70)

    kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
    existing_files = list(kline_dir.glob('*.parquet'))
    final_count = len(existing_files)

    valid_list_file = Path('/Volumes/Xdata/workstation/xxxcnstock/data/valid_stock_list.json')
    with open(valid_list_file, 'r') as f:
        target_count = len(json.load(f).get('codes', []))

    completion_rate = final_count / target_count * 100

    print(f"目标: {target_count}")
    print(f"实际: {final_count}")
    print(f"完成率: {completion_rate:.2f}%")

    if completion_rate >= 100:
        print("\n🎉 100% 采集完成！")
    else:
        remaining = target_count - final_count
        print(f"\n⚠️ 还有 {remaining} 只缺失")


if __name__ == "__main__":
    main()
