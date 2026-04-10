#!/usr/bin/env python3
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from services.email_sender import EmailSender
import yaml
from pathlib import Path

config_path = Path('/Volumes/Xdata/workstation/xxxcnstock/config/xcn_comm.yaml')
with open(config_path, 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

email_cfg = cfg.get('recommendation', {}).get('email', {})
print('=== 邮件配置 ===')
print(f'recipients: {email_cfg.get("recipients", [])}')
smtp_cfg = email_cfg.get('smtp', {})
print(f'smtp host: {smtp_cfg.get("host")}')
print(f'smtp port: {smtp_cfg.get("port")}')
print(f'smtp user: {smtp_cfg.get("user")}')
print(f'smtp password: {"已设置" if smtp_cfg.get("password") else "未设置"}')

if smtp_cfg.get('user') and smtp_cfg.get('password'):
    sender = EmailSender(
        smtp_host=smtp_cfg.get('host'),
        smtp_port=smtp_cfg.get('port', 465),
        smtp_user=smtp_cfg.get('user'),
        smtp_password=smtp_cfg.get('password')
    )
    result = sender.send(
        to_addrs=email_cfg.get('recipients', []),
        subject='测试邮件',
        content='这是一封测试邮件'
    )
    print(f'发送结果: {"成功" if result else "失败"}')
else:
    print('SMTP配置不完整，无法发送')