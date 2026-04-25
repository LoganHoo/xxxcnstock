#!/usr/bin/env python3
"""
过滤纯股票列表（排除ETF基金）
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
from pathlib import Path
from datetime import datetime


def filter_stocks_only():
    """过滤出纯股票（排除ETF基金）"""
    project_root = Path('/Volumes/Xdata/workstation/xxxcnstock')

    # 加载有效股票列表
    valid_list_file = project_root / 'data' / 'valid_stock_list.json'
    with open(valid_list_file, 'r') as f:
        all_codes = json.load(f)['codes']

    # 过滤条件：只保留真正的股票
    # 上海主板: 6开头
    # 深圳主板: 0开头 (不包括000/001开头的指数)
    # 中小板: 002开头
    # 创业板: 3开头
    # 科创板: 688开头
    # 北交所: 8开头 或 4开头

    stock_codes = []
    excluded_codes = []

    for code in all_codes:
        # ETF基金排除
        if code.startswith(('15', '51', '52', '56', '58', '59')):
            excluded_codes.append(('ETF', code))
            continue

        # 可转债排除
        if code.startswith(('11', '12')):
            excluded_codes.append(('可转债', code))
            continue

        # 指数排除
        if code.startswith(('000', '001', '880', '999', '399')):
            excluded_codes.append(('指数', code))
            continue

        # B股排除
        if code.startswith(('2', '9')):
            excluded_codes.append(('B股', code))
            continue

        # LOF基金排除 (16/18开头)
        if code.startswith(('16', '18')):
            excluded_codes.append(('LOF', code))
            continue

        # 分级基金排除 (15开头已处理)

        # 保留股票
        stock_codes.append(code)

    # 统计
    print("=" * 70)
    print("股票列表过滤报告")
    print("=" * 70)
    print(f"原始总数: {len(all_codes)}")
    print(f"股票数量: {len(stock_codes)}")
    print(f"排除数量: {len(excluded_codes)}")
    print()

    # 按类型统计排除的
    exclude_by_type = {}
    for type_name, code in excluded_codes:
        exclude_by_type[type_name] = exclude_by_type.get(type_name, 0) + 1

    print("排除类型分布:")
    for type_name, count in sorted(exclude_by_type.items(), key=lambda x: -x[1]):
        print(f"  {type_name}: {count}")

    # 保存纯股票列表
    stock_list_file = project_root / 'data' / 'stock_only_list.json'
    with open(stock_list_file, 'w') as f:
        json.dump({
            'date': datetime.now().isoformat(),
            'total': len(stock_codes),
            'codes': sorted(stock_codes),
            'excluded': {
                'total': len(excluded_codes),
                'by_type': exclude_by_type
            }
        }, f, indent=2, ensure_ascii=False)

    print(f"\n✅ 纯股票列表已保存: {stock_list_file}")

    return stock_codes


if __name__ == "__main__":
    filter_stocks_only()
