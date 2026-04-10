"""
早上报告发送脚本
发送昨日推荐报告和大盘分析
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import os

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.email_sender import EmailService
from services.notify_service.templates.report_templates import MorningReportTemplate


def get_yesterday_picks_report():
    """获取昨日推荐报告"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    report_file = project_root / 'reports' / f'daily_picks_{yesterday}.json'

    if not report_file.exists():
        return None

    with open(report_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_market_analysis_report():
    """获取大盘分析报告"""
    today = datetime.now().strftime('%Y%m%d')
    report_file = project_root / 'reports' / f'market_analysis_{today}.json'

    if not report_file.exists():
        return None

    with open(report_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_foreign_index_data():
    """获取外盘指数数据"""
    foreign_file = project_root / 'data' / 'foreign_index.json'

    if not foreign_file.exists():
        return None

    with open(foreign_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    print('=' * 50)
    print('早上报告发送')
    print(f'时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 50)
    print()

    picks_data = get_yesterday_picks_report()
    market_data = get_market_analysis_report()
    foreign_data = get_foreign_index_data()

    if not picks_data and not market_data and not foreign_data:
        print('没有可发送的报告')
        return

    content = MorningReportTemplate.generate(
        market_data=market_data,
        picks_data=picks_data,
        foreign_data=foreign_data
    )

    print(content)
    print()

    email_service = EmailService()

    recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

    if not recipients:
        print('未配置收件人')
        return

    subject = f"XCNStock 早报 - {datetime.now().strftime('%Y-%m-%d')}"

    print(f'发送邮件到: {recipients}')
    print(f'主题: {subject}')

    result = email_service.send(
        to_addrs=recipients,
        subject=subject,
        content=content
    )

    if result:
        print('邮件发送成功')
    else:
        print('邮件发送失败')


if __name__ == '__main__':
    main()