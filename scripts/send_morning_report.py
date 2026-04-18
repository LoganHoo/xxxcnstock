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
from services.notify_service.templates import get_template
from core.report_validator import check_report_quality, get_quality_checker
from core.paths import ReportPaths


def get_yesterday_picks_report():
    """获取昨日推荐报告"""
    file_path = ReportPaths.daily_picks(fallback_to_yesterday=True)
    if file_path and file_path.exists():
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def get_market_analysis_report():
    """获取大盘分析报告"""
    file_path = ReportPaths.market_analysis(fallback_to_yesterday=True)
    if file_path and file_path.exists():
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def get_foreign_index_data():
    """获取外盘指数数据"""
    file_path = ReportPaths.foreign_index()
    if file_path and file_path.exists():
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


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

    # 数据质量检查
    quality_check = check_report_quality(
        'morning_report_legacy',
        picks_data=picks_data,
        market_data=market_data,
        foreign_data=foreign_data
    )

    checker = get_quality_checker()
    quality_report = checker.generate_quality_report(quality_check)
    print("数据质量检查:")
    print(quality_report)

    # 如果有严重问题，阻止报告生成
    if quality_check['critical_issues']:
        print("数据存在严重问题，无法生成报告:")
        for issue in quality_check['critical_issues']:
            print(f"  - {issue}")
        return

    template = get_template('morning_report')
    content = template.generate(
        market_data=market_data,
        picks_data=picks_data,
        foreign_data=foreign_data
    )

    # 检查报告内容
    if not content or len(content.strip()) == 0:
        print("生成的报告内容为空")
        return

    if "数据暂不可用" in content or "N/A" in content:
        print("报告内容包含异常数据标记")

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