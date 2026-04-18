#!/usr/bin/env python3
"""
每日选股复盘任务
- 更新昨日选股的次日表现
- 更新历史选股的多周期表现
- 生成复盘报告
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import argparse
from datetime import datetime, date, timedelta
from pathlib import Path

from services.stock_selection_db_service import StockSelectionDBService
from services.multi_period_performance_service import MultiPeriodPerformanceService
from services.email_sender import EmailSender


def get_yesterday_trading_day(today: date = None) -> date:
    """获取昨日交易日（跳过周末）"""
    if today is None:
        today = date.today()

    yesterday = today - timedelta(days=1)

    # 跳过周末
    while yesterday.weekday() >= 5:  # 5=周六, 6=周日
        yesterday -= timedelta(days=1)

    return yesterday


def get_recent_trading_days(days: int = 30, end_date: date = None) -> list:
    """获取最近N个交易日列表"""
    if end_date is None:
        end_date = date.today()

    trading_days = []
    current = end_date

    while len(trading_days) < days:
        if current.weekday() < 5:  # 周一到周五
            trading_days.append(current)
        current -= timedelta(days=1)

    return sorted(trading_days)


def update_yesterday_performance(yesterday: date = None):
    """更新昨日选股的次日表现"""
    if yesterday is None:
        yesterday = get_yesterday_trading_day()

    print(f"\n{'='*60}")
    print(f"📊 更新昨日选股次日表现: {yesterday}")
    print('='*60)

    service = StockSelectionDBService()

    # 获取昨日选股
    selections = service.get_selections_by_date(yesterday.strftime('%Y-%m-%d'))

    if not selections:
        print(f"⚠️ {yesterday} 无选股记录")
        return False

    print(f"📈 昨日选股数量: {len(selections)} 只")

    # 计算次日表现
    import polars as pl
    kline_dir = Path('data/kline')

    today = yesterday + timedelta(days=1)
    while today.weekday() >= 5:
        today += timedelta(days=1)

    performance_data = []
    updated_count = 0

    for sel in selections:
        code = sel['code']
        selection_type = sel['selection_type']

        parquet_file = kline_dir / f"{code}.parquet"
        if not parquet_file.exists():
            continue

        try:
            df = pl.read_parquet(parquet_file)

            if df['trade_date'].dtype == pl.Utf8:
                df = df.with_columns(pl.col('trade_date').str.to_date())

            next_day_data = df.filter(pl.col('trade_date') == today)

            if next_day_data.is_empty():
                continue

            row = next_day_data.row(0, named=True)

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

    if performance_data:
        service.update_next_day_performance(yesterday.strftime('%Y-%m-%d'), performance_data)
        print(f"✅ 已更新 {updated_count} 只股票的次日表现")
        return True
    else:
        print(f"⚠️ 未找到次日数据")
        return False


def update_multi_period_performance(target_date: date = None, days_back: int = 30):
    """更新多周期表现（针对有完整周期数据的历史选股）"""
    if target_date is None:
        target_date = date.today()

    print(f"\n{'='*60}")
    print(f"📊 更新多周期表现（最近{days_back}个交易日）")
    print('='*60)

    # 获取最近交易日
    trading_days = get_recent_trading_days(days_back, target_date)

    # 筛选有足够后续数据的日期（至少21个交易日前的数据）
    cutoff_date = target_date - timedelta(days=35)  # 约21个交易日 + 缓冲

    service = MultiPeriodPerformanceService()
    db_service = StockSelectionDBService()

    updated_dates = []

    for trade_date in trading_days:
        if trade_date > cutoff_date:
            # 日期太近，可能没有完整21日数据，跳过
            continue

        date_str = trade_date.strftime('%Y-%m-%d')

        # 检查该日期是否有选股
        selections = db_service.get_selections_by_date(date_str)
        if not selections:
            continue

        # 检查是否已有部分多周期数据
        has_data = any(s.get('day1_return') is not None for s in selections)

        if has_data:
            # 已有数据，跳过
            continue

        print(f"\n📅 处理: {date_str} ({len(selections)}只)")

        result = service.update_selections_performance(date_str)

        if result['updated'] > 0:
            updated_dates.append(date_str)
            print(f"  ✅ 更新成功: {result['updated']} 只")

    print(f"\n✅ 共更新 {len(updated_dates)} 个日期的多周期表现")
    return updated_dates


def generate_daily_review_report(target_date: date = None):
    """生成每日复盘报告"""
    if target_date is None:
        target_date = get_yesterday_trading_day()

    date_str = target_date.strftime('%Y-%m-%d')

    print(f"\n{'='*60}")
    print(f"📊 生成每日复盘报告: {date_str}")
    print('='*60)

    service = StockSelectionDBService()

    # 获取昨日选股
    trend_stocks = service.get_selections_by_date(date_str, 'trend')
    short_stocks = service.get_selections_by_date(date_str, 'short_term')

    if not trend_stocks and not short_stocks:
        print(f"⚠️ {date_str} 无选股记录")
        return None

    # 统计表现
    report = {
        'date': date_str,
        'trend': _analyze_selections(trend_stocks),
        'short_term': _analyze_selections(short_stocks)
    }

    # 打印报告
    print("\n📈 波段趋势选股复盘:")
    _print_analysis(report['trend'])

    print("\n🚀 短线打板选股复盘:")
    _print_analysis(report['short_term'])

    return report


def _analyze_selections(selections):
    """分析选股表现"""
    if not selections:
        return {'count': 0}

    # 次日表现统计
    next_day_returns = [s.get('next_day_return') for s in selections if s.get('next_day_return') is not None]

    # 多周期表现统计
    periods = [1, 4, 7, 11, 21]
    period_stats = {}

    for period in periods:
        key = f'day{period}_return'
        returns = [s.get(key) for s in selections if s.get(key) is not None]
        if returns:
            period_stats[period] = {
                'count': len(returns),
                'avg': sum(returns) / len(returns),
                'win_rate': len([r for r in returns if r > 0]) / len(returns) * 100,
                'max': max(returns),
                'min': min(returns)
            }

    return {
        'count': len(selections),
        'next_day': {
            'count': len(next_day_returns),
            'avg': sum(next_day_returns) / len(next_day_returns) if next_day_returns else 0,
            'win_rate': len([r for r in next_day_returns if r > 0]) / len(next_day_returns) * 100 if next_day_returns else 0
        },
        'periods': period_stats
    }


def _print_analysis(analysis):
    """打印分析结果"""
    if analysis['count'] == 0:
        print("  无数据")
        return

    print(f"  选股数量: {analysis['count']} 只")

    if analysis['next_day']['count'] > 0:
        nd = analysis['next_day']
        print(f"  次日表现: 平均收益 {nd['avg']:.2f}%, 胜率 {nd['win_rate']:.1f}%")

    if analysis['periods']:
        print("  多周期表现:")
        for period, stats in sorted(analysis['periods'].items()):
            print(f"    {period}日: 平均 {stats['avg']:.2f}%, 胜率 {stats['win_rate']:.1f}%, 最高 {stats['max']:.2f}%, 最低 {stats['min']:.2f}%")


def send_review_email(report: dict, recipients: list = None):
    """发送复盘邮件"""
    if not report:
        return False

    date_str = report['date']

    # 构建邮件内容
    subject = f"选股复盘报告 - {date_str}"

    body = f"""
<h2>📊 选股复盘报告 - {date_str}</h2>

<h3>📈 波段趋势选股</h3>
{_format_html_analysis(report['trend'])}

<h3>🚀 短线打板选股</h3>
{_format_html_analysis(report['short_term'])}

<hr>
<p><small>本邮件由系统自动发送</small></p>
"""

    try:
        sender = EmailSender()
        sender.send_html_email(subject, body, recipients)
        print(f"✅ 复盘邮件已发送")
        return True
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")
        return False


def _format_html_analysis(analysis):
    """格式化HTML分析结果"""
    if analysis['count'] == 0:
        return "<p>无数据</p>"

    html = f"<p>选股数量: {analysis['count']} 只</p>"

    if analysis['next_day']['count'] > 0:
        nd = analysis['next_day']
        html += f"<p>次日表现: 平均收益 {nd['avg']:.2f}%, 胜率 {nd['win_rate']:.1f}%</p>"

    if analysis['periods']:
        html += "<table border='1' cellpadding='5'><tr><th>周期</th><th>平均收益</th><th>胜率</th><th>最高</th><th>最低</th></tr>"
        for period, stats in sorted(analysis['periods'].items()):
            html += f"<tr><td>{period}日</td><td>{stats['avg']:.2f}%</td><td>{stats['win_rate']:.1f}%</td><td>{stats['max']:.2f}%</td><td>{stats['min']:.2f}%</td></tr>"
        html += "</table>"

    return html


def main():
    parser = argparse.ArgumentParser(description='每日选股复盘任务')
    parser.add_argument('--date', help='指定日期 (YYYY-MM-DD)，默认为昨日')
    parser.add_argument('--update-yesterday', action='store_true', help='更新昨日选股的次日表现')
    parser.add_argument('--update-multi-period', action='store_true', help='更新多周期表现')
    parser.add_argument('--days-back', type=int, default=30, help='多周期回看天数')
    parser.add_argument('--report', action='store_true', help='生成复盘报告')
    parser.add_argument('--email', action='store_true', help='发送复盘邮件')
    parser.add_argument('--all', action='store_true', help='执行完整复盘流程')

    args = parser.parse_args()

    # 确定目标日期
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = get_yesterday_trading_day()

    print(f"\n{'='*60}")
    print(f"🔄 每日选股复盘任务")
    print(f"📅 目标日期: {target_date}")
    print('='*60)

    results = {}

    # 执行选定的任务
    if args.all or args.update_yesterday:
        results['yesterday'] = update_yesterday_performance(target_date)

    if args.all or args.update_multi_period:
        results['multi_period'] = update_multi_period_performance(target_date, args.days_back)

    if args.all or args.report:
        results['report'] = generate_daily_review_report(target_date)

        if args.email and results['report']:
            send_review_email(results['report'])

    # 默认执行完整流程
    if not any([args.update_yesterday, args.update_multi_period, args.report, args.all]):
        print("\n执行完整复盘流程...")
        update_yesterday_performance(target_date)
        update_multi_period_performance(target_date, args.days_back)
        report = generate_daily_review_report(target_date)
        if report:
            send_review_email(report)

    print(f"\n{'='*60}")
    print("✅ 复盘任务完成")
    print('='*60)


if __name__ == '__main__':
    main()
