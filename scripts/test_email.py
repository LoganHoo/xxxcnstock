#!/usr/bin/env python3
"""测试邮件发送功能"""
import os
import sys
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

load_dotenv()

smtp_host = os.getenv('SMTP_HOST', '192.168.1.168')
smtp_port = int(os.getenv('SMTP_PORT', '2000'))
sender_email = os.getenv('SENDER_EMAIL', 'xcnstock@local')
use_tls = os.getenv('SMTP_USE_TLS', 'false').lower() == 'true'
smtp_user = os.getenv('SMTP_USER', '')
smtp_password = os.getenv('SMTP_PASSWORD', '')
recipient = os.getenv('NOTIFICATION_EMAILS', '287363@qq.com')

print("="*70)
print("邮件发送测试")
print("="*70)
print(f"SMTP服务器: {smtp_host}:{smtp_port}")
print(f"发件人: {sender_email}")
print(f"收件人: {recipient}")
print(f"使用TLS: {use_tls}")
print(f"认证用户: {smtp_user if smtp_user else '(无)'}")
print("="*70)

try:
    msg = MIMEMultipart('alternative')
    msg['From'] = sender_email
    msg['To'] = recipient
    msg['Subject'] = 'XCNStock 邮件测试 - ' + '2026-03-25'
    
    text_content = """
这是来自 XCNStock 系统的测试邮件。

如果您收到这封邮件，说明邮件发送功能配置成功！

---
XCNStock 股票推荐系统
"""
    
    html_content = """
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; }
        .header { background-color: #4CAF50; color: white; padding: 10px; }
        .content { padding: 20px; }
        .footer { background-color: #f1f1f1; padding: 10px; text-align: center; }
    </style>
</head>
<body>
    <div class="header">
        <h2>XCNStock 邮件测试</h2>
    </div>
    <div class="content">
        <p>这是来自 <strong>XCNStock 系统</strong> 的测试邮件。</p>
        <p>如果您收到这封邮件，说明邮件发送功能配置成功！</p>
    </div>
    <div class="footer">
        <p>XCNStock 股票推荐系统</p>
    </div>
</body>
</html>
"""
    
    msg.attach(MIMEText(text_content, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))
    
    print("\n正在连接邮件服务器...")
    
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            print("启用 TLS 加密...")
            server.starttls()
        
        if smtp_user and smtp_password:
            print("正在进行身份认证...")
            server.login(smtp_user, smtp_password)
        
        print("发送邮件...")
        server.send_message(msg)
    
    print("\n✅ 邮件发送成功！")
    print(f"收件人: {recipient}")
    
except Exception as e:
    print(f"\n❌ 邮件发送失败: {e}")
    import traceback
    traceback.print_exc()
