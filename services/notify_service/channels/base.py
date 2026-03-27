from abc import ABC, abstractmethod
from typing import Dict


class BaseNotifier(ABC):
    """通知渠道基类"""
    
    @abstractmethod
    async def send(self, title: str, content: str, **kwargs) -> bool:
        """发送通知"""
        pass
    
    @abstractmethod
    def is_configured(self) -> bool:
        """检查是否已配置"""
        pass
