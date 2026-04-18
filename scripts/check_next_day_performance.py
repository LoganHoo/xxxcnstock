#!/usr/bin/env python3
"""
检查选股结果的次日表现

使用方法:
    python scripts/check_next_day_performance.py --date 2026-04-10 --top-n 20
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import csv


def get_next_trading_day(date_str: str) -> str:
    """获取下一个交易日"""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date + timedelta(days=1)
    
    # 跳过周末
    while next_day.weekday() >= 5:  # 5=周六, 6=周日
        next_day += timedelta(days=1)
    
    return next_day.strftime("%Y-%m-%d")


def load_stock_data(code: str, date: str, data_dir: Path) -> Optional[pl.DataFrame]:
    """加载股票数据"""
    file_path = data_dir / f"{code}.parquet"
    if not file_path.exists():
        return None
    
    try:
        df = pl.read_parquet(file_path)
        df = df.filter(pl.col("trade_date") <= date)
        return df.sort("trade_date")
    except:
        return None


def get_close_price(df: pl.DataFrame, date: str) -> Optional[float]:
    """获取指定日期的收盘价"""
    if df is None or len(df) == 0:
        return None
    
    row = df.filter(pl.col("trade_date") == date)
    if len(row) == 0:
        return None
    
    return float(row["close"][0])


def get_price_change(df: pl.DataFrame, date: str) -> Optional[float]:
    """获取指定日期的涨跌幅"""
    if df is None or len(df) == 0:
        return None
    
    # 计算涨跌幅
    df = df.with_columns([
        pl.col("close").pct_change().alias("pct_change")
    ])
    
    row = df.filter(pl.col("trade_date") == date)
    if len(row) == 0:
        return None
    
    pct = row["pct_change"][0]
    return float(pct) if pct is not None else None


def check_performance(selected_codes: List[str], select_date: str, next_date: str, data_dir: Path) -> List[Dict]:
    """检查次日表现"""
    results = []
    
    for code in selected_codes:
        df = load_stock_data(code, next_date, data_dir)
        if df is None:
            continue
        
        # 选股日收盘价
        select_close = get_close_price(df, select_date)
        # 次日收盘价
        next_close = get_close_price(df, next_date)
        # 次日涨跌幅
        next_change = get_price_change(df, next_date)
        
        if select_close and next_close and next_change is not None:
            results.append({
                'code': code,
                'select_date': select_date,
                'select_close': select_close,
                'next_date': next_date,
                'next_close': next_close,
                'next_change_pct': next_change * 100,
                'profit': next_close - select_close
            })
    
    return results


def print_performance_report(results: List[Dict], select_date: str, next_date: str):
    """打印表现报告"""
    print("\n" + "=" * 100)
    print(f"📊 选股次日表现报告 | 选股日: {select_date} | 次日: {next_date}")
    print("=" * 100)
    
    if not results:
        print("❌ 无数据")
        return
    
    # 排序：按涨跌幅
    results.sort(key=lambda x: x['next_change_pct'], reverse=True)
    
    print(f"\n{'排名':<4} {'代码':<10} {'选股日收盘':>12} {'次日收盘':>12} {'次日涨跌':>12} {'盈亏':>12} {'状态':>8}")
    print("-" * 100)
    
    for i, r in enumerate(results, 1):
        status = "📈" if r['next_change_pct'] > 0 else ("📉" if r['next_change_pct'] < 0 else "➡️")
        print(f"{i:<4} {r['code']:<10} {r['select_close']:>12.2f} {r['next_close']:>12.2f} "
              f"{r['next_change_pct']:>11.2f}% {r['profit']:>12.2f} {status:>8}")
    
    print("=" * 100)
    
    # 统计
    changes = [r['next_change_pct'] for r in results]
    profits = [r['profit'] for r in results]
    
    win_count = sum(1 for c in changes if c > 0)
    lose_count = sum(1 for c in changes if c < 0)
    flat_count = sum(1 for c in changes if c == 0)
    
    print("\n📈 统计信息:")
    print(f"  选中股票数: {len(results)}")
    print(f"  上涨: {win_count}只 ({win_count/len(results)*100:.1f}%)")
    print(f"  下跌: {lose_count}只 ({lose_count/len(results)*100:.1f}%)")
    print(f"  平盘: {flat_count}只 ({flat_count/len(results)*100:.1f}%)")
    print(f"  平均涨跌幅: {np.mean(changes):.2f}%")
    print(f"  涨跌幅中位数: {np.median(changes):.2f}%")
    print(f"  最大涨幅: {max(changes):.2f}%")
    print(f"  最大跌幅: {min(changes):.2f}%")
    print(f"  标准差: {np.std(changes):.2f}%")
    print(f"  平均盈亏: {np.mean(profits):.2f}元")
    
    # 涨停/跌停统计
    limit_up = sum(1 for c in changes if c >= 9.5)
    limit_down = sum(1 for c in changes if c <= -9.5)
    
    if limit_up > 0:
        print(f"\n  🚀 涨停: {limit_up}只")
    if limit_down > 0:
        print(f"  💥 跌停: {limit_down}只")
    
    # 评分
    win_rate = win_count / len(results) * 100 if results else 0
    avg_return = np.mean(changes)
    
    print(f"\n📊 综合评分:")
    print(f"  胜率: {win_rate:.1f}%")
    print(f"  平均收益: {avg_return:.2f}%")
    
    if win_rate >= 60 and avg_return > 0:
        print(f"  评价: ✅ 优秀")
    elif win_rate >= 50 and avg_return > 0:
        print(f"  评价: 🟡 良好")
    elif win_rate >= 40:
        print(f"  评价: ⚠️ 一般")
    else:
        print(f"  评价: ❌ 较差")


def main():
    parser = argparse.ArgumentParser(description='检查选股次日表现')
    parser.add_argument('--date', type=str, required=True, help='选股日期 (YYYY-MM-DD)')
    parser.add_argument('--next-date', type=str, help='次日日期 (YYYY-MM-DD)，默认自动计算')
    parser.add_argument('--data-dir', type=str, default='data/kline', help='数据目录')
    
    args = parser.parse_args()
    
    # 确定次日日期
    if args.next_date:
        next_date = args.next_date
    else:
        next_date = get_next_trading_day(args.date)
    
    print(f"\n📅 选股日期: {args.date}")
    print(f"📅 次日日期: {next_date}")
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"❌ 数据目录不存在: {data_dir}")
        return
    
    # 读取选股结果文件
    result_file = f"all_filters_factors_v3_{args.date}.csv"
    if not Path(result_file).exists():
        print(f"❌ 选股结果文件不存在: {result_file}")
        print(f"请先运行: python scripts/test_all_filters_factors_v3.py --date {args.date}")
        return
    
    # 读取选中的股票代码
    selected_codes = []
    with open(result_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            selected_codes.append(row['code'])
    
    print(f"📋 选中股票数: {len(selected_codes)}")
    print(f"   {', '.join(selected_codes[:10])}{'...' if len(selected_codes) > 10 else ''}")
    
    # 检查次日表现
    results = check_performance(selected_codes, args.date, next_date, data_dir)
    
    # 打印报告
    print_performance_report(results, args.date, next_date)
    
    # 保存结果
    if results:
        output_file = f"performance_{args.date}_{next_date}.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['code', 'select_date', 'select_close', 'next_date', 'next_close', 'next_change_pct', 'profit'])
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ 结果已保存: {output_file}")


if __name__ == "__main__":
    main()
