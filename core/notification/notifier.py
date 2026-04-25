#!/usr/bin/env python3
"""
通知服务

支持渠道:
- 钉钉
- 企业微信
- 飞书
- 邮件
- Webhook

使用方法:
    from core.notification.notifier import NotificationManager
    
    notifier = NotificationManager()
    notifier.send_dingtalk("告警标题", "告警内容")
"""
import json
import logging
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class NotificationChannel(Enum):
    """通知渠道"""
    DINGTALK = "dingtalk"
    WECHAT = "wechat"
    FEISHU = "feishu"
    EMAIL = "email"
    WEBHOOK = "webhook"


@dataclass
class NotificationMessage:
    """通知消息"""
    title: str
    content: str
    level: str = "info"  # info, warning, error, critical
    timestamp: datetime = None
    extra: Dict = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.extra is None:
            self.extra = {}


class BaseNotifier:
    """通知基类"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.enabled = config.get('enabled', True)
    
    def send(self, message: NotificationMessage) -> bool:
        """发送通知"""
        if not self.enabled:
            return False
        
        try:
            return self._send_impl(message)
        except Exception as e:
            logger.error(f"发送通知失败: {e}")
            return False
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """子类实现"""
        raise NotImplementedError


class DingTalkNotifier(BaseNotifier):
    """钉钉通知"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.webhook = config.get('webhook')
        self.secret = config.get('secret')
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """发送钉钉消息"""
        if not self.webhook:
            logger.error("钉钉Webhook未配置")
            return False
        
        # 构建消息
        color_map = {
            'info': '#1890ff',
            'warning': '#faad14',
            'error': '#f5222d',
            'critical': '#722ed1'
        }
        
        markdown_content = f"""#### {message.title}
**时间**: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
**级别**: {message.level.upper()}

{message.content}
"""
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": message.title,
                "text": markdown_content
            }
        }
        
        # 发送请求
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            self.webhook,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info(f"钉钉通知发送成功: {message.title}")
                return True
            else:
                logger.error(f"钉钉通知发送失败: {result}")
                return False
        else:
            logger.error(f"钉钉通知HTTP错误: {response.status_code}")
            return False


class WeChatNotifier(BaseNotifier):
    """企业微信通知"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.webhook = config.get('webhook')
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """发送企业微信消息"""
        if not self.webhook:
            logger.error("企业微信Webhook未配置")
            return False
        
        # 构建markdown消息
        content = f"""**{message.title}**
时间: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
级别: {message.level.upper()}

{message.content}
"""
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            self.webhook,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info(f"企业微信通知发送成功: {message.title}")
                return True
            else:
                logger.error(f"企业微信通知发送失败: {result}")
                return False
        else:
            logger.error(f"企业微信通知HTTP错误: {response.status_code}")
            return False


class FeishuNotifier(BaseNotifier):
    """飞书通知"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.webhook = config.get('webhook')
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """发送飞书消息"""
        if not self.webhook:
            logger.error("飞书Webhook未配置")
            return False
        
        # 构建卡片消息
        color_map = {
            'info': 'blue',
            'warning': 'orange',
            'error': 'red',
            'critical': 'purple'
        }
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": message.title
                    },
                    "template": color_map.get(message.level, 'blue')
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**时间**: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n**级别**: {message.level.upper()}\n\n{message.content}"
                        }
                    }
                ]
            }
        }
        
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            self.webhook,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"飞书通知发送成功: {message.title}")
            return True
        else:
            logger.error(f"飞书通知HTTP错误: {response.status_code}")
            return False


class EmailNotifier(BaseNotifier):
    """邮件通知"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.smtp_host = config.get('smtp_host')
        self.smtp_port = config.get('smtp_port', 587)
        self.username = config.get('username')
        self.password = config.get('password')
        self.from_addr = config.get('from_addr')
        self.to_addrs = config.get('to_addrs', [])
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """发送邮件"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = self.from_addr
            msg['To'] = ', '.join(self.to_addrs)
            msg['Subject'] = f"[{message.level.upper()}] {message.title}"
            
            body = f"""
时间: {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
级别: {message.level.upper()}

{message.content}
"""
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"邮件发送成功: {message.title}")
            return True
            
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False


class WebhookNotifier(BaseNotifier):
    """通用Webhook通知"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.webhook = config.get('webhook')
        self.headers = config.get('headers', {})
        self.method = config.get('method', 'POST')
    
    def _send_impl(self, message: NotificationMessage) -> bool:
        """发送Webhook请求"""
        if not self.webhook:
            logger.error("Webhook未配置")
            return False
        
        payload = {
            'title': message.title,
            'content': message.content,
            'level': message.level,
            'timestamp': message.timestamp.isoformat(),
            'extra': message.extra
        }
        
        try:
            if self.method.upper() == 'POST':
                response = requests.post(
                    self.webhook,
                    headers=self.headers,
                    json=payload,
                    timeout=10
                )
            else:
                response = requests.get(
                    self.webhook,
                    headers=self.headers,
                    params=payload,
                    timeout=10
                )
            
            if response.status_code == 200:
                logger.info(f"Webhook通知发送成功: {message.title}")
                return True
            else:
                logger.error(f"Webhook通知HTTP错误: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Webhook通知发送失败: {e}")
            return False


class NotificationManager:
    """通知管理器"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.notifiers: Dict[NotificationChannel, BaseNotifier] = {}
        self._init_notifiers()
    
    def _init_notifiers(self):
        """初始化通知器"""
        # 钉钉
        dingtalk_config = self.config.get('dingtalk', {})
        if dingtalk_config.get('enabled') and dingtalk_config.get('webhook'):
            self.notifiers[NotificationChannel.DINGTALK] = DingTalkNotifier(dingtalk_config)
        
        # 企业微信
        wechat_config = self.config.get('wechat', {})
        if wechat_config.get('enabled') and wechat_config.get('webhook'):
            self.notifiers[NotificationChannel.WECHAT] = WeChatNotifier(wechat_config)
        
        # 飞书
        feishu_config = self.config.get('feishu', {})
        if feishu_config.get('enabled') and feishu_config.get('webhook'):
            self.notifiers[NotificationChannel.FEISHU] = FeishuNotifier(feishu_config)
        
        # 邮件
        email_config = self.config.get('email', {})
        if email_config.get('enabled') and email_config.get('smtp_host'):
            self.notifiers[NotificationChannel.EMAIL] = EmailNotifier(email_config)
        
        # Webhook
        webhook_config = self.config.get('webhook', {})
        if webhook_config.get('enabled') and webhook_config.get('webhook'):
            self.notifiers[NotificationChannel.WEBHOOK] = WebhookNotifier(webhook_config)
    
    def send(self, title: str, content: str, level: str = "info",
             channels: List[NotificationChannel] = None, extra: Dict = None) -> Dict[NotificationChannel, bool]:
        """发送通知到指定渠道"""
        message = NotificationMessage(
            title=title,
            content=content,
            level=level,
            extra=extra
        )
        
        results = {}
        target_channels = channels or list(self.notifiers.keys())
        
        for channel in target_channels:
            notifier = self.notifiers.get(channel)
            if notifier:
                results[channel] = notifier.send(message)
            else:
                results[channel] = False
                logger.warning(f"通知渠道未配置: {channel.value}")
        
        return results
    
    def send_dingtalk(self, title: str, content: str, level: str = "info") -> bool:
        """发送钉钉通知"""
        results = self.send(title, content, level, [NotificationChannel.DINGTALK])
        return results.get(NotificationChannel.DINGTALK, False)
    
    def send_wechat(self, title: str, content: str, level: str = "info") -> bool:
        """发送企业微信通知"""
        results = self.send(title, content, level, [NotificationChannel.WECHAT])
        return results.get(NotificationChannel.WECHAT, False)
    
    def send_feishu(self, title: str, content: str, level: str = "info") -> bool:
        """发送飞书通知"""
        results = self.send(title, content, level, [NotificationChannel.FEISHU])
        return results.get(NotificationChannel.FEISHU, False)
    
    def send_email(self, title: str, content: str, level: str = "info") -> bool:
        """发送邮件通知"""
        results = self.send(title, content, level, [NotificationChannel.EMAIL])
        return results.get(NotificationChannel.EMAIL, False)
    
    def send_all(self, title: str, content: str, level: str = "info") -> Dict[NotificationChannel, bool]:
        """发送到所有渠道"""
        return self.send(title, content, level)
    
    def send_alert(self, title: str, content: str, level: str = "warning"):
        """发送告警"""
        return self.send(title, content, level)
    
    def send_trade_notification(self, trade_info: Dict):
        """发送交易通知"""
        title = f"交易执行: {trade_info.get('code')}"
        content = f"""
操作: {trade_info.get('side', 'UNKNOWN')}
股票: {trade_info.get('code')} - {trade_info.get('name', '')}
价格: ¥{trade_info.get('price', 0):.2f}
数量: {trade_info.get('quantity', 0)}
金额: ¥{trade_info.get('amount', 0):,.2f}
"""
        return self.send(title, content, "info")
    
    def send_risk_alert(self, risk_info: Dict):
        """发送风险告警"""
        title = f"风险告警: {risk_info.get('type', 'Unknown')}"
        content = f"""
风险类型: {risk_info.get('type')}
风险等级: {risk_info.get('level', 'warning')}
详情: {risk_info.get('message')}

建议操作: {risk_info.get('recommendation', '请检查系统状态')}
"""
        return self.send(title, content, risk_info.get('level', 'warning'))


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 配置（实际使用时从配置文件读取）
    config = {
        'dingtalk': {
            'enabled': True,
            'webhook': 'https://oapi.dingtalk.com/robot/send?access_token=your_token'
        }
    }
    
    notifier = NotificationManager(config)
    
    # 发送测试消息
    # notifier.send("测试通知", "这是一条测试消息", "info")
    
    print("通知服务初始化完成")
