import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import logging

from core.config import get_settings
from core.logger import setup_logger
from services.notify_service.channels.base import BaseNotifier

logger = setup_logger("email_notifier")


class EmailNotifier(BaseNotifier):
    """邮件通知渠道"""
    
    def __init__(self):
        settings = get_settings()
        self.host = settings.SMTP_HOST
        self.port = settings.SMTP_PORT
        self.user = settings.SMTP_USER
        self.password = settings.SMTP_PASSWORD
        self.recipients = settings.EMAIL_RECIPIENTS.split(",") if settings.EMAIL_RECIPIENTS else []
    
    async def send(self, title: str, content: str, **kwargs) -> bool:
        """发送邮件通知"""
        if not self.is_configured():
            logger.warning("邮件通知未配置")
            return False
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[XCNStock] {title}"
            msg['From'] = self.user
            msg['To'] = ", ".join(self.recipients)
            
            # 纯文本内容
            text_part = MIMEText(content, 'plain', 'utf-8')
            msg.attach(text_part)
            
            # 发送邮件
            with smtplib.SMTP_SSL(self.host, self.port) as server:
                server.login(self.user, self.password)
                server.sendmail(self.user, self.recipients, msg.as_string())
            
            logger.info(f"邮件通知发送成功: {title}")
            return True
                
        except Exception as e:
            logger.error(f"邮件通知发送异常: {e}")
            return False
    
    def is_configured(self) -> bool:
        return bool(self.host and self.user and self.password and self.recipients)
