#!/usr/bin/env python3
"""
选股复盘工具
- 比对不同日期的选股结果
- 分析选股表现
- 生成复盘报告
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime, date, timedelta
from tabulate import tabulate
from services.stock_selection_db_service import StockSelectionDBService


def print_header(title):
    """打印标题"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def cmd_list(args):
    """列出某日的选股结果"""
    print_header(f"选股列表 - {args.date}")

    service = StockSelectionDBService()

    # 获取波段选股
    trend_stocks = service.get_selections_by_date(args.date, 'trend')
    if trend_stocks:
        print(f"\n📈 波段趋势选股 ({len(trend_stocks)}只):")
        headers = ['排名', '代码', '名称', '评分', '收盘价', '次日收益', '日内最大']
        rows = []
        for s in trend_stocks[:args.limit]:
            rows.append([
                s['rank'],
                s['code'],
                s['name'][:8],
                f"{s['score']:.2f}" if s['score'] else '-',
                f"{s['close_price']:.2f}" if s['close_price'] else '-',
                f"{s['next_day_return']:.2f}%" if s['next_day_return'] else '-',
                f"{s['max_intraday_return']:.2f}%" if s['max_intraday_return'] else '-'
            ])
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print("\n📈 波段趋势选股: 无")

    # 获取短线选股
    short_stocks = service.get_selections_by_date(args.date, 'short_term')
    if short_stocks:
        print(f"\n🚀 短线打板选股 ({len(short_stocks)}只):")
        headers = ['排名', '代码', '名称', '评分', '收盘价', '次日收益', '日内最大']
        rows = []
        for s in short_stocks[:args.limit]:
            rows.append([
                s['rank'],
                s['code'],
                s['name'][:8],
                f"{s['score']:.2f}" if s['score'] else '-',
                f"{s['close_price']:.2f}" if s['close_price'] else '-',
                f"{s['next_day_return']:.2f}%" if s['next_day_return'] else '-',
                f"{s['max_intraday_return']:.2f}%" if s['max_intraday_return'] else '-'
            ])
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print("\n🚀 短线打板选股: 无")


def cmd_compare(args):
    """比对两个日期的选股"""
    print_header(f"选股比对: {args.date1} vs {args.date2}")

    service = StockSelectionDBService()

    # 比对波段选股
    print("\n📈 波段趋势选股比对:")
    result = service.compare_dates(args.date1, args.date2, 'trend')
    if result:
        print(f"  {args.date1}: {result['date1_count']} 只")
        print(f"  {args.date2}: {result['date2_count']} 只")
        print(f"  共同股票: {result['common_count']} 只 ({result['continuity_rate']:.1f}%)")

        if result['common_stocks']:
            print(f"\n  共同股票列表:")
            for code in result['common_stocks'][:10]:
                print(f"    - {code}")
            if len(result['common_stocks']) > 10:
                print(f"    ... 等共 {len(result['common_stocks'])} 只")

        if result['only_in_date1']:
            print(f"\n  仅在 {args.date1}:")
            print(f"    {', '.join(result['only_in_date1'][:10])}")
            if len(result['only_in_date1']) > 10:
                print(f"    ... 等共 {len(result['only_in_date1'])} 只")

        if result['only_in_date2']:
            print(f"\n  仅在 {args.date2}:")
            print(f"    {', '.join(result['only_in_date2'][:10])}")
            if len(result['only_in_date2']) > 10:
                print(f"    ... 等共 {len(result['only_in_date2'])} 只")

    # 比对短线选股
    print("\n🚀 短线打板选股比对:")
    result = service.compare_dates(args.date1, args.date2, 'short_term')
    if result:
        print(f"  {args.date1}: {result['date1_count']} 只")
        print(f"  {args.date2}: {result['date2_count']} 只")
        print(f"  共同股票: {result['common_count']} 只 ({result['continuity_rate']:.1f}%)")


def cmd_performance(args):
    """查看选股表现统计"""
    print_header(f"选股表现统计")

    service = StockSelectionDBService()

    # 获取波段表现
    print("\n📈 波段趋势选股表现:")
    summary = service.get_performance_summary(
        start_date=args.start,
        end_date=args.end,
        selection_type='trend',
        days=args.days
    )

    if summary.get('with_performance', 0) > 0:
        headers = ['指标', '数值']
        rows = [
            ['统计周期', summary['period']],
            ['选股总数', summary['total_selections']],
            ['有复盘数据', summary['with_performance']],
            ['盈利数量', summary['win_count']],
            ['亏损数量', summary['loss_count']],
            ['胜率', f"{summary['win_rate']:.1f}%"],
            ['平均收益', f"{summary['avg_return']:.2f}%"],
            ['最大收益', f"{summary['max_return']:.2f}%"],
            ['最小收益', f"{summary['min_return']:.2f}%"],
        ]
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print(f"  {summary.get('message', '暂无数据')}")

    # 获取短线表现
    print("\n🚀 短线打板选股表现:")
    summary = service.get_performance_summary(
        start_date=args.start,
        end_date=args.end,
        selection_type='short_term',
        days=args.days
    )

    if summary.get('with_performance', 0) > 0:
        headers = ['指标', '数值']
        rows = [
            ['统计周期', summary['period']],
            ['选股总数', summary['total_selections']],
            ['有复盘数据', summary['with_performance']],
            ['盈利数量', summary['win_count']],
            ['亏损数量', summary['loss_count']],
            ['胜率', f"{summary['win_rate']:.1f}%"],
            ['平均收益', f"{summary['avg_return']:.2f}%"],
            ['最大收益', f"{summary['max_return']:.2f}%"],
            ['最小收益', f"{summary['min_return']:.2f}%"],
        ]
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print(f"  {summary.get('message', '暂无数据')}")


def cmd_track(args):
    """追踪某只股票的选股记录"""
    print_header(f"股票追踪 - {args.code}")

    service = StockSelectionDBService()

    records = service.get_continuity_analysis(args.code, days=args.days)

    if records:
        headers = ['日期', '类型', '排名', '评分', '收盘价', '次日收益']
        rows = []
        for r in records:
            rows.append([
                r['report_date'],
                '波段' if r['selection_type'] == 'trend' else '短线',
                r['rank'],
                f"{r['score']:.2f}" if r['score'] else '-',
                f"{r['close_price']:.2f}" if r['close_price'] else '-',
                f"{r['next_day_return']:.2f}%" if r['next_day_return'] else '-'
            ])
        print(tabulate(rows, headers=headers, tablefmt='grid'))
        print(f"\n共 {len(records)} 次入选")
    else:
        print(f"\n最近 {args.days} 天内无选股记录")


def cmd_update_performance(args):
    """更新次日表现数据（从K线数据计算）"""
    print_header(f"更新次日表现 - {args.date}")

    import polars as pl
    from pathlib import Path

    service = StockSelectionDBService()

    # 获取当日的选股
    selections = service.get_selections_by_date(args.date)

    if not selections:
        print(f"❌ {args.date} 无选股记录")
        return

    # 计算次日日期
    report_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    next_date = report_date + timedelta(days=1)

    # 跳过周末
    while next_date.weekday() >= 5:  # 5=周六, 6=周日
        next_date += timedelta(days=1)

    print(f"📅 报告日期: {report_date}")
    print(f"📅 次日日期: {next_date}")

    # 读取次日K线数据
    kline_dir = Path('data/kline')

    performance_data = []
    updated_count = 0

    for sel in selections:
        code = sel['code']
        selection_type = sel['selection_type']

        # 读取股票K线数据
        parquet_file = kline_dir / f"{code}.parquet"
        if not parquet_file.exists():
            continue

        try:
            df = pl.read_parquet(parquet_file)

            # 确保日期格式正确
            if df['trade_date'].dtype == pl.Utf8:
                df = df.with_columns(pl.col('trade_date').str.to_date())

            # 查找次日数据
            next_day_data = df.filter(pl.col('trade_date') == next_date)

            if next_day_data.is_empty():
                continue

            row = next_day_data.row(0, named=True)

            # 计算收益
            prev_close = sel.get('close_price', 0)
            next_open = row.get('open', 0)
            next_close = row.get('close', 0)
            next_high = row.get('high', 0)
            next_low = row.get('low', 0)

            if prev_close and prev_close > 0:
                next_day_return = (next_close - prev_close) / prev_close * 100
                max_intraday_return = (next_high - prev_close) / prev_close * 100
                max_intraday_loss = (next_low - prev_close) / prev_close * 100
            else:
                next_day_return = None
                max_intraday_return = None
                max_intraday_loss = None

            performance_data.append({
                'code': code,
                'selection_type': selection_type,
                'next_day_open': next_open,
                'next_day_close': next_close,
                'next_day_high': next_high,
                'next_day_low': next_low,
                'next_day_return': next_day_return,
                'max_intraday_return': max_intraday_return,
                'max_intraday_loss': max_intraday_loss
            })

            updated_count += 1

        except Exception as e:
            print(f"  ⚠️ {code}: 处理失败 - {e}")

    # 更新数据库
    if performance_data:
        service.update_next_day_performance(args.date, performance_data)
        print(f"\n✅ 已更新 {updated_count} 只股票的次日表现")
    else:
        print(f"\n⚠️ 未找到次日数据，可能次日非交易日")


def cmd_multi_period(args):
    """更新多周期表现数据"""
    print_header(f"更新多周期表现 - {args.date}")

    from services.multi_period_performance_service import MultiPeriodPerformanceService

    service = MultiPeriodPerformanceService()
    service.update_selections_performance(args.date, args.type)


def cmd_multi_period_batch(args):
    """批量更新多周期表现"""
    print_header(f"批量更新多周期表现: {args.start} ~ {args.end}")

    from services.multi_period_performance_service import MultiPeriodPerformanceService

    service = MultiPeriodPerformanceService()
    service.batch_update_performance(args.start, args.end, args.type)


def cmd_analyze_periods(args):
    """分析多周期表现"""
    print_header(f"多周期表现分析 - {args.date}")

    from services.multi_period_performance_service import MultiPeriodPerformanceService
    import numpy as np

    service = MultiPeriodPerformanceService()

    # 获取选股列表
    db_service = StockSelectionDBService()
    selections = db_service.get_selections_by_date(args.date, args.type)

    if not selections:
        print(f"❌ {args.date} 无选股记录")
        return

    print(f"\n📊 选股数量: {len(selections)} 只")
    print(f"📊 选股类型: {'波段趋势' if args.type == 'trend' else '短线打板'}")

    # 收集各周期数据
    periods = [1, 4, 7, 11, 21]
    period_stats = {}

    for period in periods:
        period_key = f'day{period}_return'
        returns = [s.get(period_key) for s in selections if s.get(period_key) is not None]

        if returns:
            period_stats[period] = {
                'count': len(returns),
                'avg': np.mean(returns),
                'median': np.median(returns),
                'max': max(returns),
                'min': min(returns),
                'win_rate': len([r for r in returns if r > 0]) / len(returns) * 100,
                'std': np.std(returns)
            }

    if not period_stats:
        print("\n⚠️ 暂无多周期数据，请先运行 update-multi-period 命令")
        return

    # 打印统计表格
    print("\n📈 多周期表现统计:")
    headers = ['周期', '样本数', '平均收益', '中位数', '胜率', '最大收益', '最大亏损', '标准差']
    rows = []

    for period in periods:
        if period in period_stats:
            stats = period_stats[period]
            rows.append([
                f'{period}日',
                stats['count'],
                f"{stats['avg']:.2f}%",
                f"{stats['median']:.2f}%",
                f"{stats['win_rate']:.1f}%",
                f"{stats['max']:.2f}%",
                f"{stats['min']:.2f}%",
                f"{stats['std']:.2f}%"
            ])

    print(tabulate(rows, headers=headers, tablefmt='grid'))

    # 打印详细列表
    if args.detail:
        print("\n📋 个股多周期表现:")
        headers = ['代码', '名称', '1日', '4日', '7日', '11日', '21日']
        rows = []

        for s in selections[:args.limit]:
            rows.append([
                s['code'],
                s['name'][:8] if s['name'] else '-',
                f"{s.get('day1_return', 0):.2f}%" if s.get('day1_return') else '-',
                f"{s.get('day4_return', 0):.2f}%" if s.get('day4_return') else '-',
                f"{s.get('day7_return', 0):.2f}%" if s.get('day7_return') else '-',
                f"{s.get('day11_return', 0):.2f}%" if s.get('day11_return') else '-',
                f"{s.get('day21_return', 0):.2f}%" if s.get('day21_return') else '-'
            ])

        print(tabulate(rows, headers=headers, tablefmt='grid'))


def main():
    parser = argparse.ArgumentParser(description='选股复盘工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # list 命令
    list_parser = subparsers.add_parser('list', help='列出某日的选股结果')
    list_parser.add_argument('date', help='日期 (YYYY-MM-DD)')
    list_parser.add_argument('-l', '--limit', type=int, default=20, help='显示数量限制')
    list_parser.set_defaults(func=cmd_list)

    # compare 命令
    compare_parser = subparsers.add_parser('compare', help='比对两个日期的选股')
    compare_parser.add_argument('date1', help='第一个日期 (YYYY-MM-DD)')
    compare_parser.add_argument('date2', help='第二个日期 (YYYY-MM-DD)')
    compare_parser.set_defaults(func=cmd_compare)

    # performance 命令
    perf_parser = subparsers.add_parser('performance', help='查看选股表现统计')
    perf_parser.add_argument('-s', '--start', help='开始日期 (YYYY-MM-DD)')
    perf_parser.add_argument('-e', '--end', help='结束日期 (YYYY-MM-DD)')
    perf_parser.add_argument('-d', '--days', type=int, default=30, help='最近N天')
    perf_parser.set_defaults(func=cmd_performance)

    # track 命令
    track_parser = subparsers.add_parser('track', help='追踪某只股票的选股记录')
    track_parser.add_argument('code', help='股票代码')
    track_parser.add_argument('-d', '--days', type=int, default=30, help='最近N天')
    track_parser.set_defaults(func=cmd_track)

    # update-performance 命令
    update_parser = subparsers.add_parser('update-performance', help='更新次日表现数据')
    update_parser.add_argument('date', help='日期 (YYYY-MM-DD)')
    update_parser.set_defaults(func=cmd_update_performance)

    # update-multi-period 命令
    multi_parser = subparsers.add_parser('update-multi-period', help='更新多周期表现数据')
    multi_parser.add_argument('date', help='日期 (YYYY-MM-DD)')
    multi_parser.add_argument('--type', choices=['trend', 'short_term'], help='选股类型')
    multi_parser.set_defaults(func=cmd_multi_period)

    # update-multi-period-batch 命令
    multi_batch_parser = subparsers.add_parser('update-multi-period-batch', help='批量更新多周期表现')
    multi_batch_parser.add_argument('--start', required=True, help='开始日期 (YYYY-MM-DD)')
    multi_batch_parser.add_argument('--end', required=True, help='结束日期 (YYYY-MM-DD)')
    multi_batch_parser.add_argument('--type', choices=['trend', 'short_term'], help='选股类型')
    multi_batch_parser.set_defaults(func=cmd_multi_period_batch)

    # analyze-periods 命令
    analyze_parser = subparsers.add_parser('analyze-periods', help='分析多周期表现')
    analyze_parser.add_argument('date', help='日期 (YYYY-MM-DD)')
    analyze_parser.add_argument('--type', choices=['trend', 'short_term'], default='trend', help='选股类型')
    analyze_parser.add_argument('--detail', action='store_true', help='显示个股详情')
    analyze_parser.add_argument('-l', '--limit', type=int, default=20, help='显示数量限制')
    analyze_parser.set_defaults(func=cmd_analyze_periods)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == '__main__':
    main()
