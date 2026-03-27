"""
测试定时任务和邮件发送功能
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import json

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from trading_calendar import check_market_status
from email_sender import EmailSender


def test_market_status():
    """测试市场状态检查"""
    print("\n" + "="*70)
    print("测试市场状态检查")
    print("="*70)
    
    status = check_market_status()
    
    print(f"当前时间: {status['current_time']} {status['weekday']}")
    print(f"是否交易日: {'是' if status['is_trading_day'] else '否'}")
    print(f"是否收盘后: {'是' if status['is_after_market_close'] else '否'}")
    print(f"应执行任务: {'是' if status['should_run_task'] else '否'}")
    print(f"原因: {status['reason']}")
    print(f"上一交易日: {status['last_trading_day']}")
    
    return status


def test_email_sending():
    """测试邮件发送"""
    print("\n" + "="*70)
    print("测试邮件发送功能")
    print("="*70)
    
    sender_email = os.getenv('SENDER_EMAIL', '')
    sender_password = os.getenv('SENDER_PASSWORD', '')
    notification_emails = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com').split(',')
    
    if not sender_email or sender_email == 'your_email@qq.com':
        print("⚠️  邮件发送功能未配置")
        print("请在 .env 文件中配置以下环境变量：")
        print("  - SENDER_EMAIL: 发件人邮箱")
        print("  - SENDER_PASSWORD: 发件人授权码")
        print("\n获取QQ邮箱授权码：")
        print("1. 登录QQ邮箱")
        print("2. 设置 -> 账户 -> POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV服务")
        print("3. 开启服务并生成授权码")
        return False
    
    print(f"发件人: {sender_email}")
    print(f"收件人: {', '.join(notification_emails)}")
    
    sender = EmailSender(
        smtp_server=os.getenv('SMTP_SERVER', 'smtp.qq.com'),
        smtp_port=int(os.getenv('SMTP_PORT', '587')),
        sender_email=sender_email,
        sender_password=sender_password,
        use_tls=os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
    )
    
    test_report = {
        'timestamp': datetime.now().isoformat(),
        'market_status': {
            'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'weekday': '周一',
            'is_trading_day': True,
            'is_after_market_close': True,
            'should_run_task': True,
            'reason': '测试邮件发送'
        },
        'validation': {
            'total': 5079,
            'valid': 5070,
            'invalid': 9,
            'warnings': 15
        },
        'completeness': {
            'expected_date': '2026-03-24',
            'total_stocks': 5079,
            'stocks_with_data': 5070,
            'stocks_missing_data': 9,
            'missing_stocks': ['000001', '000002', '000003']
        }
    }
    
    print("\n发送测试邮件...")
    success = sender.send_daily_report(
        to_emails=notification_emails,
        report_data=test_report
    )
    
    if success:
        print("✅ 测试邮件发送成功！")
        print(f"请检查收件箱: {', '.join(notification_emails)}")
    else:
        print("❌ 测试邮件发送失败")
    
    return success


def test_scheduled_task():
    """测试定时任务（不实际执行数据采集）"""
    print("\n" + "="*70)
    print("测试定时任务流程")
    print("="*70)
    
    print("\n1. 检查前置条件...")
    market_status = check_market_status()
    print(f"   结果: {market_status['reason']}")
    
    if not market_status['should_run_task']:
        print("\n⚠️  当前不满足执行条件，跳过后续测试")
        return
    
    print("\n2. 检查断点续传...")
    progress_file = PROJECT_ROOT / "data" / "kline" / ".fetch_progress.json"
    if progress_file.exists():
        print(f"   发现断点文件: {progress_file}")
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        print(f"   已处理: {len(progress.get('processed', []))} 只股票")
    else:
        print("   无断点文件")
    
    print("\n3. 测试邮件发送...")
    test_email_sending()
    
    print("\n" + "="*70)
    print("定时任务测试完成")
    print("="*70)


def main():
    """主函数"""
    print("\n" + "="*70)
    print("XCNStock 定时任务测试")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    test_market_status()
    
    test_email_sending()
    
    test_scheduled_task()
    
    print("\n" + "="*70)
    print("所有测试完成")
    print("="*70)


if __name__ == '__main__':
    main()
