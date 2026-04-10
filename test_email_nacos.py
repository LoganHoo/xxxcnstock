#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from services.email_sender import EmailSender
import yaml
import os
from dotenv import load_dotenv
load_dotenv()

import nacos

# 从 Nacos 加载配置
client = nacos.NacosClient(
    os.getenv('NACOS_SERVER_ADDR'),
    namespace=os.getenv('NACOS_NAMESPACE'),
    username=os.getenv('NACOS_USERNAME'),
    password=os.getenv('NACOS_PASSWORD')
)

content = client.get_config('xcomm.yaml', 'DEFAULT_GROUP')
cfg = yaml.safe_load(content)

email_cfg = cfg.get('email', {})
print('=== 邮件配置 (Nacos) ===')
print(f'recipients: {email_cfg.get("notification", {}).get("emails", [])}')
smtp_cfg = email_cfg.get('smtp', {})
print(f'smtp host: {smtp_cfg.get("server")}')
print(f'smtp port: {smtp_cfg.get("port")}')
print(f'smtp user: {smtp_cfg.get("username")}')
print(f'smtp password: {"已设置" if smtp_cfg.get("password") else "未设置"}')

sender = EmailSender(
    smtp_host=smtp_cfg.get('server'),
    smtp_port=smtp_cfg.get('port', 465),
    smtp_user=smtp_cfg.get('username'),
    smtp_password=smtp_cfg.get('password')
)
result = sender.send(
    to_addrs=email_cfg.get('notification', {}).get('emails', []),
    subject='测试邮件',
    content='这是一封测试邮件'
)
print(f'发送结果: {"成功" if result else "失败"}')
