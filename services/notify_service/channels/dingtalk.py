import requests
import hmac
import hashlib
import base64
import time
from urllib.parse import quote_plus
from typing import Dict
import logging

from core.config import get_settings
from core.logger import setup_logger
from services.notify_service.channels.base import BaseNotifier

logger = setup_logger("dingtalk_notifier")


class DingTalkNotifier(BaseNotifier):
    """钉钉机器人通知渠道"""
    
    def __init__(self):
        settings = get_settings()
        self.webhook = settings.DINGTALK_WEBHOOK
        self.secret = settings.DINGTALK_SECRET
    
    async def send(self, title: str, content: str, **kwargs) -> bool:
        """发送钉钉通知"""
        if not self.is_configured():
            logger.warning("钉钉通知未配置")
            return False
        
        try:
            url = self._build_url()
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": title,
                    "text": content
                }
            }
            
            response = requests.post(url, json=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    logger.info(f"钉钉通知发送成功: {title}")
                    return True
                else:
                    logger.error(f"钉钉通知发送失败: {result}")
                    return False
            else:
                logger.error(f"钉钉通知请求失败: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"钉钉通知发送异常: {e}")
            return False
    
    def _build_url(self) -> str:
        """构建带签名的URL"""
        if not self.secret:
            return self.webhook
        
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        
        hmac_code = hmac.new(
            self.secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        
        sign = quote_plus(base64.b64encode(hmac_code))
        
        return f"{self.webhook}&timestamp={timestamp}&sign={sign}"
    
    def is_configured(self) -> bool:
        return bool(self.webhook)
