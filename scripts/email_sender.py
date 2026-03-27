"""
邮件发送工具
用于发送每日报告邮件
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict
import json
import logging

logger = logging.getLogger(__name__)


class EmailSender:
    """邮件发送器"""
    
    def __init__(
        self,
        smtp_server: str = "smtp.qq.com",
        smtp_port: int = 587,
        sender_email: str = "",
        sender_password: str = "",
        use_tls: bool = True
    ):
        """
        初始化邮件发送器
        
        Args:
            smtp_server: SMTP服务器地址
            smtp_port: SMTP端口
            sender_email: 发件人邮箱
            sender_password: 发件人密码/授权码
            use_tls: 是否使用TLS
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.use_tls = use_tls
    
    def send_email(
        self,
        to_emails: List[str],
        subject: str,
        content: str,
        html_content: Optional[str] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        发送邮件
        
        Args:
            to_emails: 收件人邮箱列表
            subject: 邮件主题
            content: 邮件内容（纯文本）
            html_content: HTML格式内容（可选）
            attachments: 附件文件路径列表（可选）
            
        Returns:
            bool: 是否发送成功
        """
        try:
            message = MIMEMultipart('alternative')
            message['From'] = self.sender_email
            message['To'] = ', '.join(to_emails)
            message['Subject'] = subject
            
            message.attach(MIMEText(content, 'plain', 'utf-8'))
            
            if html_content:
                message.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            if attachments:
                for attachment_path in attachments:
                    file_path = Path(attachment_path)
                    if file_path.exists():
                        with open(file_path, 'rb') as f:
                            attachment = MIMEApplication(f.read())
                            attachment.add_header(
                                'Content-Disposition',
                                'attachment',
                                filename=file_path.name
                            )
                            message.attach(attachment)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                server.login(self.sender_email, self.sender_password)
                server.send_message(message)
            
            logger.info(f"✅ 邮件发送成功: {', '.join(to_emails)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")
            return False
    
    def send_daily_report(
        self,
        to_emails: List[str],
        report_data: Dict,
        report_file: Optional[str] = None
    ) -> bool:
        """
        发送每日报告邮件
        
        Args:
            to_emails: 收件人邮箱列表
            report_data: 报告数据
            report_file: 报告文件路径（可选）
            
        Returns:
            bool: 是否发送成功
        """
        timestamp = report_data.get('timestamp', datetime.now().isoformat())
        market_status = report_data.get('market_status', {})
        validation = report_data.get('validation', {})
        completeness = report_data.get('completeness', {})
        
        subject = f"XCNStock 每日数据采集报告 - {timestamp[:10]}"
        
        text_content = f"""
XCNStock 每日数据采集报告
{'='*50}

时间: {timestamp}
市场状态: {market_status.get('weekday', '')} {'交易日' if market_status.get('is_trading_day') else '非交易日'}

数据验证:
  - 总股票数: {validation.get('total', 0)}
  - 有效数据: {validation.get('valid', 0)}
  - 无效数据: {validation.get('invalid', 0)}
  - 警告数量: {validation.get('warnings', 0)}

数据完整性:
  - 期望日期: {completeness.get('expected_date', '')}
  - 总股票数: {completeness.get('total_stocks', 0)}
  - 包含数据: {completeness.get('stocks_with_data', 0)}
  - 缺失数据: {completeness.get('stocks_missing_data', 0)}
  
{'='*50}

XCNStock A股量化分析系统
"""
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }}
        .section {{ background: #f9f9f9; padding: 15px; border-radius: 8px; margin-bottom: 15px; border-left: 4px solid #667eea; }}
        .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #667eea; }}
        .metric-label {{ font-size: 14px; color: #666; }}
        .status-good {{ color: #10b981; }}
        .status-warning {{ color: #f59e0b; }}
        .status-error {{ color: #ef4444; }}
        .footer {{ text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #e5e7eb; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 XCNStock 每日数据采集报告</h1>
            <p>时间: {timestamp}</p>
        </div>
        
        <div class="section">
            <h2>📅 市场状态</h2>
            <p><strong>日期:</strong> {market_status.get('current_time', '')} {market_status.get('weekday', '')}</p>
            <p><strong>交易日:</strong> <span class="{'status-good' if market_status.get('is_trading_day') else 'status-warning'}">{'是' if market_status.get('is_trading_day') else '否'}</span></p>
            <p><strong>收盘后:</strong> <span class="{'status-good' if market_status.get('is_after_market_close') else 'status-warning'}">{'是' if market_status.get('is_after_market_close') else '否'}</span></p>
        </div>
        
        <div class="section">
            <h2>✅ 数据验证</h2>
            <div class="metric">
                <div class="metric-value">{validation.get('total', 0)}</div>
                <div class="metric-label">总股票数</div>
            </div>
            <div class="metric">
                <div class="metric-value status-good">{validation.get('valid', 0)}</div>
                <div class="metric-label">有效数据</div>
            </div>
            <div class="metric">
                <div class="metric-value status-error">{validation.get('invalid', 0)}</div>
                <div class="metric-label">无效数据</div>
            </div>
            <div class="metric">
                <div class="metric-value status-warning">{validation.get('warnings', 0)}</div>
                <div class="metric-label">警告数量</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📈 数据完整性</h2>
            <p><strong>期望日期:</strong> {completeness.get('expected_date', '')}</p>
            <div class="metric">
                <div class="metric-value">{completeness.get('total_stocks', 0)}</div>
                <div class="metric-label">总股票数</div>
            </div>
            <div class="metric">
                <div class="metric-value status-good">{completeness.get('stocks_with_data', 0)}</div>
                <div class="metric-label">包含数据</div>
            </div>
            <div class="metric">
                <div class="metric-value status-error">{completeness.get('stocks_missing_data', 0)}</div>
                <div class="metric-label">缺失数据</div>
            </div>
        </div>
        
        <div class="footer">
            <p>XCNStock A股量化分析系统 | 本项目仅做流水线的SOP，不做实盘交易</p>
        </div>
    </div>
</body>
</html>
"""
        
        attachments = [report_file] if report_file else None
        
        return self.send_email(
            to_emails=to_emails,
            subject=subject,
            content=text_content,
            html_content=html_content,
            attachments=attachments
        )


def send_report_email(
    report_file: str,
    to_emails: List[str],
    smtp_config: Dict
) -> bool:
    """
    发送报告邮件的便捷函数
    
    Args:
        report_file: 报告文件路径
        to_emails: 收件人邮箱列表
        smtp_config: SMTP配置
        
    Returns:
        bool: 是否发送成功
    """
    report_path = Path(report_file)
    
    if not report_path.exists():
        logger.error(f"❌ 报告文件不存在: {report_file}")
        return False
    
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
    except Exception as e:
        logger.error(f"❌ 读取报告文件失败: {e}")
        return False
    
    sender = EmailSender(
        smtp_server=smtp_config.get('smtp_server', 'smtp.qq.com'),
        smtp_port=smtp_config.get('smtp_port', 587),
        sender_email=smtp_config.get('sender_email', ''),
        sender_password=smtp_config.get('sender_password', ''),
        use_tls=smtp_config.get('use_tls', True)
    )
    
    return sender.send_daily_report(
        to_emails=to_emails,
        report_data=report_data,
        report_file=report_file
    )


if __name__ == '__main__':
    import os
    
    sender_email = os.getenv('SENDER_EMAIL', '')
    sender_password = os.getenv('SENDER_PASSWORD', '')
    
    if not sender_email or not sender_password:
        print("请设置环境变量 SENDER_EMAIL 和 SENDER_PASSWORD")
        sys.exit(1)
    
    sender = EmailSender(
        smtp_server='smtp.qq.com',
        smtp_port=587,
        sender_email=sender_email,
        sender_password=sender_password
    )
    
    test_report = {
        'timestamp': datetime.now().isoformat(),
        'market_status': {
            'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'weekday': '周一',
            'is_trading_day': True,
            'is_after_market_close': True
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
            'stocks_missing_data': 9
        }
    }
    
    success = sender.send_daily_report(
        to_emails=['287363@qq.com'],
        report_data=test_report
    )
    
    if success:
        print("✅ 测试邮件发送成功")
    else:
        print("❌ 测试邮件发送失败")
