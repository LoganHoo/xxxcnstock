#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from dotenv import load_dotenv
load_dotenv()

import nacos
import yaml

# 连接 Nacos
client = nacos.NacosClient(
    os.getenv('NACOS_SERVER_ADDR', '49.233.10.199:8188'),
    namespace=os.getenv('NACOS_NAMESPACE', ''),
    username=os.getenv('NACOS_USERNAME', 'nacos'),
    password=os.getenv('NACOS_PASSWORD', '')
)

# 从 Nacos 下载 xcomm.yaml
content = client.get_config('xcomm.yaml', 'DEFAULT_GROUP')

if content:
    cfg = yaml.safe_load(content)
    
    # 更新 SMTP 配置
    email_cfg = cfg.get('recommendation', {}).get('email', {})
    if not email_cfg:
        email_cfg = {}
    
    email_cfg['smtp'] = {
        'host': os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com'),
        'port': int(os.getenv('EMAIL_SMTP_PORT', 465)),
        'user': os.getenv('EMAIL_USERNAME', ''),
        'password': os.getenv('EMAIL_PASSWORD', '')
    }
    
    cfg['recommendation']['email'] = email_cfg
    
    # 保存回 Nacos
    new_content = yaml.dump(cfg, allow_unicode=True, default_flow_style=False)
    client.publish_config('xcomm.yaml', 'DEFAULT_GROUP', new_content)
    
    print("✅ xcomm.yaml 已从 Nacos 下载并更新 SMTP 配置")
    print(f"SMTP user: {os.getenv('EMAIL_USERNAME')}")
    print(f"SMTP password: {'已设置' if os.getenv('EMAIL_PASSWORD') else '未设置'}")
else:
    print("❌ Nacos 中不存在 xcomm.yaml")
