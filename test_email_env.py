#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from services.email_sender import EmailSender
import os
from dotenv import load_dotenv
load_dotenv()

print('=== 邮件配置 (.env) ===')
print(f'EMAIL_USERNAME: {os.getenv("EMAIL_USERNAME")}')
print(f'EMAIL_PASSWORD: {"已设置" if os.getenv("EMAIL_PASSWORD") else "未设置"}')
print(f'EMAIL_SMTP_SERVER: {os.getenv("EMAIL_SMTP_SERVER")}')
print(f'EMAIL_SMTP_PORT: {os.getenv("EMAIL_SMTP_PORT")}')
print(f'NOTIFICATION_EMAILS: {os.getenv("NOTIFICATION_EMAILS")}')

sender = EmailSender(
    smtp_host=os.getenv('EMAIL_SMTP_SERVER'),
    smtp_port=int(os.getenv('EMAIL_SMTP_PORT')),
    smtp_user=os.getenv('EMAIL_USERNAME'),
    smtp_password=os.getenv('EMAIL_PASSWORD'),
    use_ssl=True
)

result = sender.send(
    to_addrs=[os.getenv('NOTIFICATION_EMAILS')],
    subject='测试邮件',
    content='这是一封测试邮件'
)
print(f'发送结果: {"成功" if result else "失败"}')
