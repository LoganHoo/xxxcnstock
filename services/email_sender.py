"""
邮件发送工具模块
支持两种发送方式：
1. SMTP 直连 (QQ邮箱等)
2. HTTP API (本地邮件服务)
"""
import os
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional
from pathlib import Path
from core.logger import get_logger

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


logger = get_logger(__name__)


class EmailSender:
    """SMTP 邮件发送器"""
    
    def __init__(
        self,
        smtp_host: str = None,
        smtp_port: int = None,
        smtp_user: str = None,
        smtp_password: str = None,
        use_ssl: bool = None
    ):
        self.smtp_host = smtp_host or os.getenv('SMTP_HOST', 'smtp.qq.com')
        self.smtp_port = int(smtp_port or os.getenv('SMTP_PORT', '465'))
        self.smtp_user = smtp_user or os.getenv('SMTP_USER', '')
        self.smtp_password = smtp_password or os.getenv('SMTP_PASSWORD', '')
        self.use_ssl = use_ssl if use_ssl is not None else os.getenv('SMTP_USE_TLS', 'false').lower() == 'true'
        self.sender_email = os.getenv('SENDER_EMAIL', self.smtp_user)
    
    def send(
        self,
        to_addrs: List[str],
        subject: str,
        content: str,
        html_content: str = None,
        attachments: List[str] = None
    ) -> bool:
        """
        发送邮件
        
        Args:
            to_addrs: 收件人列表
            subject: 邮件主题
            content: 纯文本内容
            html_content: HTML内容 (可选)
            attachments: 附件文件路径列表 (可选)
        
        Returns:
            bool: 发送是否成功
        """
        if not self.smtp_user or not self.smtp_password:
            logger.error("SMTP 用户名或密码未配置")
            return False
        
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(to_addrs)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            
            if html_content:
                msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            if attachments:
                for file_path in attachments:
                    self._attach_file(msg, file_path)
            
            logger.info(f"连接邮件服务器: {self.smtp_host}:{self.smtp_port}")
            
            try:
                if self.smtp_port == 465:
                    # SSL模式 (端口465)
                    with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=30) as server:
                        server.login(self.smtp_user, self.smtp_password)
                        server.send_message(msg)
                elif self.smtp_port == 587:
                    # STARTTLS模式 (端口587)
                    with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                        server.starttls()
                        logger.info("已启用STARTTLS加密")
                        server.login(self.smtp_user, self.smtp_password)
                        server.send_message(msg)
                else:
                    # 其他端口，尝试STARTTLS，失败则使用普通模式
                    with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                        try:
                            server.starttls()
                            logger.info("已启用STARTTLS加密")
                        except Exception as tls_error:
                            logger.warning(f"STARTTLS失败，使用非加密连接: {tls_error}")
                        server.login(self.smtp_user, self.smtp_password)
                        server.send_message(msg)
            except Exception as ssl_error:
                # SSL失败时，尝试降级到非SSL模式
                if self.smtp_port == 465:
                    logger.warning(f"SSL连接失败，尝试使用STARTTLS: {ssl_error}")
                    with smtplib.SMTP(self.smtp_host, 587, timeout=30) as server:
                        server.starttls()
                        server.login(self.smtp_user, self.smtp_password)
                        server.send_message(msg)
                else:
                    raise
            
            logger.info(f"邮件发送成功: {', '.join(to_addrs)}")
            return True
            
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False
    
    def _attach_file(self, msg: MIMEMultipart, file_path: str):
        """添加附件"""
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"附件文件不存在: {file_path}")
            return
        
        with open(path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                'attachment',
                filename=path.name
            )
            msg.attach(part)


class EmailAPISender:
    """HTTP API 邮件发送器"""
    
    def __init__(self, api_url: str = None):
        self.api_url = api_url or os.getenv('EMAIL_API_URL', 'http://192.168.1.168:2000/api/email/send')
        self.timeout = 30
    
    def send(
        self,
        to_addrs: List[str],
        subject: str,
        content: str,
        html_content: str = None,
        attachments: List[str] = None
    ) -> bool:
        """
        通过 HTTP API 发送邮件
        
        API 格式:
        {
            "to": "收件人邮箱",
            "subject": "邮件主题",
            "content": "邮件内容",
            "is_html": false,
            "attachments": []
        }
        """
        try:
            payload = {
                'to': ','.join(to_addrs),
                'subject': subject,
                'content': html_content if html_content else content,
                'is_html': html_content is not None,
                'attachments': attachments or []
            }
            
            logger.info(f"调用邮件 API: {self.api_url}")
            
            response = requests.post(
                self.api_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success' or result.get('status') == 'ok':
                    logger.info(f"邮件发送成功: {', '.join(to_addrs)}")
                    return True
                else:
                    logger.error(f"邮件发送失败: {result.get('message', 'Unknown error')}")
                    return False
            else:
                logger.error(f"邮件发送失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"邮件 API 调用失败: {e}")
            return False


class EmailService:
    """邮件服务 - HTTP API 优先，SMTP 备用"""

    def __init__(self, api_url: str = None, use_api: bool = None):
        self.api_url = api_url or os.getenv('EMAIL_API_URL', 'http://192.168.1.168:2000/api/email/send')
        self.use_api = use_api if use_api is not None else os.getenv('EMAIL_USE_API', 'true').lower() == 'true'
        self.api_sender = EmailAPISender(self.api_url)
        self.smtp_sender = EmailSender(
            smtp_host=os.getenv('EMAIL_SMTP_SERVER', 'smtp.qq.com'),
            smtp_port=int(os.getenv('EMAIL_SMTP_PORT', 465)),
            smtp_user=os.getenv('EMAIL_USERNAME', ''),
            smtp_password=os.getenv('EMAIL_PASSWORD', ''),
            use_ssl=True
        )
    
    def send(
        self,
        to_addrs: List[str],
        subject: str,
        content: str,
        html_content: str = None,
        attachments: List[str] = None
    ) -> bool:
        """
        发送邮件 - 优先 API，失败则 SMTP 备用
        """
        if self.use_api:
            logger.info(f"尝试 HTTP API 发送: {self.api_url}")
            result = self.api_sender.send(to_addrs, subject, content, html_content, attachments)
            if result:
                return True
            logger.warning("HTTP API 发送失败，切换到 SMTP 备用...")
        
        logger.info("使用 SMTP 发送邮件")
        return self.smtp_sender.send(to_addrs, subject, content, html_content, attachments)


def send_email(
    to_addrs: List[str],
    subject: str,
    content: str,
    html_content: str = None,
    attachments: List[str] = None,
    use_api: bool = None
) -> bool:
    """
    快捷发送邮件函数
    
    Args:
        to_addrs: 收件人列表
        subject: 邮件主题
        content: 纯文本内容
        html_content: HTML内容 (可选)
        attachments: 附件文件路径列表 (可选)
        use_api: 是否使用 API 方式 (可选)
    
    Returns:
        bool: 发送是否成功
    """
    service = EmailService(use_api=use_api)
    return service.send(to_addrs, subject, content, html_content, attachments)


def send_report_email(
    subject: str,
    content: str,
    html_content: str = None,
    recipients: List[str] = None
) -> bool:
    """
    发送报告邮件 (使用环境变量中的收件人)
    
    Args:
        subject: 邮件主题
        content: 纯文本内容
        html_content: HTML内容 (可选)
        recipients: 收件人列表 (可选，默认使用环境变量)
    
    Returns:
        bool: 发送是否成功
    """
    if recipients is None:
        recipients_str = os.getenv('NOTIFICATION_EMAILS', '')
        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    
    if not recipients:
        logger.error("未配置收件人")
        return False
    
    return send_email(recipients, subject, content, html_content)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    result = send_email(
        to_addrs=['287363@qq.com'],
        subject='XCNStock 测试邮件',
        content='这是一封测试邮件，来自 XCNStock 邮件模块。',
        html_content='<h1>测试邮件</h1><p>这是一封测试邮件，来自 XCNStock 邮件模块。</p>'
    )
    
    print(f"发送结果: {result}")
