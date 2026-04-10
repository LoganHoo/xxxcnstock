#!/usr/bin/env python3
"""
资金行为学周报/月报生成脚本
================================================================================

【功能说明】
本脚本用于生成资金行为学策略的周报和月报，基于存储在MySQL中的每日报告数据。

【使用方式】
--------------------------------------------------------------------------------
# 生成周报 (上周)
python generate_weekly_monthly_report.py --type weekly

# 生成月报 (本月)
python generate_weekly_monthly_report.py --type monthly --year 2026 --month 4

# 生成指定月份的月报
python generate_weekly_monthly_report.py --type monthly --year 2026 --month 3

# 初始化数据库表
python generate_weekly_monthly_report.py --init-db

【报告输出】
--------------------------------------------------------------------------------
- MySQL: xcn_fund_behavior_weekly / xcn_fund_behavior_monthly
- HTML: data/reports/html/fund_behavior_weekly_YYYY-MM-DD.html
- HTML: data/reports/html/fund_behavior_monthly_YYYY-MM.html
--------------------------------------------------------------------------------
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from calendar import monthrange

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.fund_behavior_db_service import FundBehaviorDBService
from services.notify_service.templates.weekly_monthly_report_template import (
    generate_weekly_html,
    generate_monthly_html
)


def init_database():
    """初始化数据库表"""
    print("=" * 60)
    print("初始化数据库表")
    print("=" * 60)

    service = FundBehaviorDBService()
    service.init_tables()

    print("✅ 数据库表初始化完成")


def generate_weekly_report(service: FundBehaviorDBService, week_end: str = None):
    """生成周报"""
    if week_end is None:
        week_end = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"生成周报: 截止日期 {week_end}")
    print("=" * 60)

    success = service.calculate_and_save_weekly(week_end)

    if success:
        print("✅ 周报数据已保存到MySQL")

        from services.notify_service.templates.fund_behavior_report_template import generate_fund_behavior_html
        week_data = service.get_weekly_reports(weeks=1)
        if week_data:
            html = generate_weekly_html(week_data[0])
            html_dir = Path('data/reports/html')
            html_dir.mkdir(parents=True, exist_ok=True)
            html_path = html_dir / f"fund_behavior_weekly_{week_end}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"✅ HTML周报已保存: {html_path}")
    else:
        print("⚠️ 周报生成失败，请检查数据")


def generate_monthly_report(service: FundBehaviorDBService, year: int, month: int):
    """生成月报"""
    print("=" * 60)
    print(f"生成月报: {year}年{month:02d}月")
    print("=" * 60)

    success = service.calculate_and_save_monthly(year, month)

    if success:
        print("✅ 月报数据已保存到MySQL")

        monthly_reports = service.get_monthly_reports(months=1)
        if monthly_reports:
            m_data = monthly_reports[0]
            html = generate_monthly_html(m_data)
            html_dir = Path('data/reports/html')
            html_dir.mkdir(parents=True, exist_ok=True)
            html_path = html_dir / f"fund_behavior_monthly_{year}-{month:02d}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"✅ HTML月报已保存: {html_path}")
    else:
        print("⚠️ 月报生成失败，请检查数据")


def main():
    parser = argparse.ArgumentParser(
        description='资金行为学周报/月报生成脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python generate_weekly_monthly_report.py --init-db           # 初始化数据库
  python generate_weekly_monthly_report.py --type weekly       # 生成上周周报
  python generate_weekly_monthly_report.py --type monthly --year 2026 --month 3  # 生成2026年3月月报
        """
    )

    parser.add_argument('--type', type=str, choices=['weekly', 'monthly'],
                       help='报告类型: weekly(月报) / monthly(周报)')
    parser.add_argument('--year', type=int,
                       help='指定年份 (月报)')
    parser.add_argument('--month', type=int,
                       help='指定月份 (月报)')
    parser.add_argument('--week-end', type=str,
                       help='周报截止日期 (YYYY-MM-DD格式)')
    parser.add_argument('--init-db', action='store_true',
                       help='初始化数据库表')

    args = parser.parse_args()

    if args.init_db:
        init_database()
        return

    if args.type is None:
        parser.print_help()
        print("\n⚠️ 请指定报告类型: --type weekly 或 --type monthly")
        return

    service = FundBehaviorDBService()
    service.init_tables()

    if args.type == 'weekly':
        generate_weekly_report(service, args.week_end)
    elif args.type == 'monthly':
        if args.year is None or args.month is None:
            now = datetime.now()
            year = now.year
            month = now.month
        else:
            year = args.year
            month = args.month
        generate_monthly_report(service, year, month)


if __name__ == '__main__':
    main()