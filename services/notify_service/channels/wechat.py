import requests
from typing import Dict
import logging

from core.config import get_settings
from core.logger import setup_logger
from services.notify_service.channels.base import BaseNotifier

logger = setup_logger("wechat_notifier")


class WechatNotifier(BaseNotifier):
    """微信通知渠道 (Server酱)"""
    
    def __init__(self):
        settings = get_settings()
        self.send_key = settings.WECHAT_SEND_KEY
    
    async def send(self, title: str, content: str, **kwargs) -> bool:
        """发送微信通知"""
        if not self.is_configured():
            logger.warning("微信通知未配置")
            return False
        
        try:
            url = f"https://sctapi.ftqq.com/{self.send_key}.send"
            data = {
                "title": title,
                "desp": content
            }
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 0:
                    logger.info(f"微信通知发送成功: {title}")
                    return True
                else:
                    logger.error(f"微信通知发送失败: {result}")
                    return False
            else:
                logger.error(f"微信通知请求失败: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"微信通知发送异常: {e}")
            return False
    
    def is_configured(self) -> bool:
        return bool(self.send_key)
