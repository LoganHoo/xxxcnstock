#!/usr/bin/env python3
"""
工作流执行后邮件通知脚本
================================================================================
在 Kestra 工作流执行完成后发送邮件通知

用法:
    python scripts/pipeline/send_workflow_notification.py \
        --workflow xcnstock_data_pipeline \
        --execution-id <execution_id> \
        --status SUCCESS

环境变量:
    SMTP_HOST - SMTP服务器地址 (默认: smtp.qq.com)
    SMTP_PORT - SMTP端口 (默认: 465)
    SMTP_USER - 发件人邮箱
    SMTP_PASSWORD - 邮箱授权码
    NOTIFICATION_EMAILS - 收件人邮箱，多个用逗号分隔
"""
import sys
import os
import argparse
import json
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 导入邮件发送模块
sys.path.insert(0, str(project_root / 'services'))
from email_sender import send_email


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='发送工作流执行通知邮件')
    parser.add_argument('--workflow', required=True, help='工作流名称')
    parser.add_argument('--execution-id', required=True, help='执行ID')
    parser.add_argument('--status', required=True, choices=['SUCCESS', 'FAILED'], help='执行状态')
    parser.add_argument('--date', default=None, help='执行日期 (YYYY-MM-DD)')
    parser.add_argument('--log-file', default=None, help='日志文件路径')
    return parser.parse_args()


def build_email_content(workflow: str, execution_id: str, status: str, date: str, log_summary: str = '') -> tuple:
    """
    构建邮件内容
    
    Returns:
        (subject, text_content, html_content)
    """
    status_emoji = '✅' if status == 'SUCCESS' else '❌'
    status_text = '执行成功' if status == 'SUCCESS' else '执行失败'
    
    subject = f"[XCNStock] {workflow} {status_text} - {date}"
    
    # 纯文本内容
    text_content = f"""
{status_emoji} XCNStock 工作流执行通知

工作流: {workflow}
执行ID: {execution_id}
执行状态: {status_text}
执行时间: {date}

{log_summary}

---
本邮件由 XCNStock 量化交易系统自动发送
"""
    
    # HTML内容
    status_color = '#28a745' if status == 'SUCCESS' else '#dc3545'
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: {status_color}; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; }}
        .footer {{ text-align: center; color: #6c757d; font-size: 12px; margin-top: 20px; }}
        .info-row {{ margin: 10px 0; }}
        .label {{ font-weight: bold; color: #495057; }}
        .value {{ color: #212529; }}
        pre {{ background-color: #e9ecef; padding: 10px; border-radius: 3px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>{status_emoji} 工作流执行{status_text}</h2>
        </div>
        <div class="content">
            <div class="info-row">
                <span class="label">工作流:</span>
                <span class="value">{workflow}</span>
            </div>
            <div class="info-row">
                <span class="label">执行ID:</span>
                <span class="value">{execution_id}</span>
            </div>
            <div class="info-row">
                <span class="label">执行状态:</span>
                <span class="value" style="color: {status_color}; font-weight: bold;">{status_text}</span>
            </div>
            <div class="info-row">
                <span class="label">执行时间:</span>
                <span class="value">{date}</span>
            </div>
            {f'<div class="info-row"><span class="label">执行日志:</span><pre>{log_summary}</pre></div>' if log_summary else ''}
        </div>
        <div class="footer">
            <p>本邮件由 XCNStock 量化交易系统自动发送</p>
            <p>发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
"""
    
    return subject, text_content, html_content


def main():
    """主函数"""
    args = parse_args()
    
    # 获取日期
    date = args.date or datetime.now().strftime('%Y-%m-%d')
    
    # 读取日志摘要（如果提供）
    log_summary = ''
    if args.log_file and os.path.exists(args.log_file):
        try:
            with open(args.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # 取最后20行作为摘要
                log_summary = ''.join(lines[-20:])
        except Exception as e:
            log_summary = f'读取日志失败: {e}'
    
    # 构建邮件内容
    subject, text_content, html_content = build_email_content(
        args.workflow,
        args.execution_id,
        args.status,
        date,
        log_summary
    )
    
    # 获取收件人
    recipients_str = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')
    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    
    if not recipients:
        print("错误: 未配置收件人邮箱")
        return 1
    
    # 发送邮件
    print(f"发送邮件通知到: {', '.join(recipients)}")
    print(f"主题: {subject}")
    
    try:
        result = send_email(
            to_addrs=recipients,
            subject=subject,
            content=text_content,
            html_content=html_content
        )
        
        if result:
            print("✅ 邮件发送成功")
            return 0
        else:
            print("❌ 邮件发送失败")
            return 1
            
    except Exception as e:
        print(f"❌ 邮件发送异常: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
