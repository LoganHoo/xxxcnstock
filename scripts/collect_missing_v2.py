#!/usr/bin/env python3
"""
100% 数据采集 - 补充缺失股票 V2
使用单线程顺序采集，更稳定可靠
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
import time
import importlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Set, List, Dict

import pandas as pd
import polars as pl


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
    print(f"100% 数据采集 - 补充缺失 V2")
    print(f"=" * 70)
    print(f"目标总数: {len(all_codes)}")
    print(f"已有数据: {len(existing_codes)}")
    print(f"缺失数量: {len(missing)}")
    print(f"=" * 70)

    return missing


def collect_with_baostock(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用Baostock采集"""
    bs = importlib.import_module("baostock")

    lg = bs.login()
    if lg.error_code != '0':
        print(f"  Baostock登录失败: {lg.error_msg}")
        return None

    try:
        # 转换代码格式
        code_str = str(code).zfill(6)
        if code_str.startswith('6'):
            code_bs = f"sh.{code_str}"
        elif code_str.startswith('0') or code_str.startswith('3'):
            code_bs = f"sz.{code_str}"
        else:
            code_bs = code_str

        fields = "date,open,high,low,close,volume"
        rs = bs.query_history_k_data_plus(
            code_bs,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )

        if rs.error_code != '0':
            print(f"  Baostock查询失败: {rs.error_msg}")
            return None

        data = []
        while rs.next():
            row = rs.get_row_data()
            data.append({
                'code': code,
                'date': row[0],
                'open': float(row[1]) if row[1] else 0,
                'high': float(row[2]) if row[2] else 0,
                'low': float(row[3]) if row[3] else 0,
                'close': float(row[4]) if row[4] else 0,
                'volume': int(float(row[5])) if row[5] else 0
            })

        if data:
            return pd.DataFrame(data)
        return None

    finally:
        bs.logout()


def collect_with_tencent(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用腾讯采集"""
    import requests
    import re

    try:
        symbol = f"sh{code}" if code.startswith('6') else f"sz{code}"
        url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'

        params = {
            '_var': f'kline_dayqfq_{symbol}',
            'param': f'{symbol},day,,,500,qfq',
            'r': str(int(time.time() * 1000))
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/'
        }

        response = requests.get(url, params=params, headers=headers, timeout=30, proxies={})
        text = response.text
        match = re.match(rf'kline_dayqfq_\w+=(.*)', text)

        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][symbol].get('qfqday', [])
                records = []
                for k in klines:
                    records.append({
                        'code': code,
                        'date': k[0],
                        'open': float(k[1]),
                        'close': float(k[2]),
                        'high': float(k[3]),
                        'low': float(k[4]),
                        'volume': int(float(k[5]))
                    })
                if records:
                    return pd.DataFrame(records)
        return None
    except Exception as e:
        print(f"  Tencent错误: {e}")
        return None


def collect_single_stock(code: str, date: str) -> Dict:
    """采集单只股票"""
    start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')

    # 尝试Baostock
    try:
        df = collect_with_baostock(code, start_date, date)
        if df is not None and len(df) > 0:
            output_path = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline') / f"{code}.parquet"
            pl.from_pandas(df).write_parquet(output_path)
            return {'code': code, 'success': True, 'source': 'baostock', 'records': len(df)}
    except Exception as e:
        print(f"  Baostock异常: {e}")

    # 尝试Tencent
    try:
        df = collect_with_tencent(code, start_date, date)
        if df is not None and len(df) > 0:
            output_path = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline') / f"{code}.parquet"
            pl.from_pandas(df).write_parquet(output_path)
            return {'code': code, 'success': True, 'source': 'tencent', 'records': len(df)}
    except Exception as e:
        print(f"  Tencent异常: {e}")

    return {'code': code, 'success': False, 'source': '', 'records': 0}


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
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

    print(f"\n开始顺序采集 {len(missing_codes)} 只股票...")
    print("-" * 70)

    # 统计
    success_count = 0
    failed_count = 0
    by_source = {'baostock': 0, 'tencent': 0}

    for i, code in enumerate(missing_codes):
        print(f"\n[{i+1}/{len(missing_codes)}] 采集 {code}...")

        result = collect_single_stock(code, args.date)

        if result['success']:
            success_count += 1
            by_source[result['source']] += 1
            print(f"  ✅ 成功 [{result['source']}] - {result['records']} 条记录")
        else:
            failed_count += 1
            print(f"  ❌ 失败")

        # 每10只显示进度
        if (i + 1) % 10 == 0:
            progress = (i + 1) / len(missing_codes) * 100
            print(f"\n{'='*70}")
            print(f"进度: {i+1}/{len(missing_codes)} ({progress:.1f}%)")
            print(f"成功: {success_count} | 失败: {failed_count}")
            print(f"{'='*70}\n")

        # 短暂延迟，避免请求过快
        time.sleep(0.2)

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

    if completion_rate >= 99:
        print("\n🎉 基本达到100% 采集完成！")
    else:
        remaining = target_count - final_count
        print(f"\n⚠️ 还有 {remaining} 只缺失")


if __name__ == "__main__":
    main()
