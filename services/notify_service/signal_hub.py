from typing import List, Dict
from datetime import datetime
import asyncio
import logging

from core.models import NotificationMessage, SignalLevel
from core.logger import setup_logger
from services.notify_service.channels.wechat import WechatNotifier
from services.notify_service.channels.dingtalk import DingTalkNotifier
from services.notify_service.channels.email import EmailNotifier

logger = setup_logger("signal_hub", log_file="signals/notify.log")


class SignalHub:
    """信号中心 - 聚合多渠道通知"""
    
    def __init__(self):
        self.wechat = WechatNotifier()
        self.dingtalk = DingTalkNotifier()
        self.email = EmailNotifier()
        
        # 通知频率控制
        self._last_notify_time: Dict[str, datetime] = {}
        self.min_interval = 300  # 同一股票最小通知间隔5分钟
    
    async def send_signal(self, message: NotificationMessage) -> Dict[str, bool]:
        """
        发送信号通知
        
        Returns:
            各渠道发送结果
        """
        results = {}
        
        # 根据信号等级选择渠道
        channels = self._get_channels_for_level(message.level)
        
        for channel in channels:
            if channel == "wechat" and "wechat" in message.channels:
                results["wechat"] = await self.wechat.send(
                    message.title,
                    message.content
                )
            elif channel == "dingtalk" and "dingtalk" in message.channels:
                results["dingtalk"] = await self.dingtalk.send(
                    message.title,
                    message.content
                )
            elif channel == "email" and "email" in message.channels:
                results["email"] = await self.email.send(
                    message.title,
                    message.content
                )
        
        logger.info(f"信号发送完成: {message.title} - {results}")
        return results
    
    def _get_channels_for_level(self, level: SignalLevel) -> List[str]:
        """根据信号等级获取通知渠道"""
        if level == SignalLevel.S:
            return ["wechat", "dingtalk", "email"]
        elif level == SignalLevel.A:
            return ["wechat", "dingtalk", "email"]
        elif level == SignalLevel.B:
            return ["wechat", "dingtalk"]
        else:
            return ["wechat"]
    
    async def send_limit_up_signal(self, signal_data: Dict) -> bool:
        """发送涨停信号"""
        level = SignalLevel(signal_data.get("signal_level", "C"))
        
        content = self._format_limit_up_content(signal_data)
        
        message = NotificationMessage(
            title=f"【涨停信号-{level.value}级】{signal_data.get('name', '')}",
            content=content,
            level=level,
            channels=["wechat", "dingtalk", "email"] if level in [SignalLevel.S, SignalLevel.A] else ["wechat"]
        )
        
        results = await self.send_signal(message)
        return any(results.values())
    
    def _format_limit_up_content(self, data: Dict) -> str:
        """格式化涨停信号内容"""
        lines = [
            f"股票: {data.get('name', '')} ({data.get('code', '')})",
            f"涨幅: {data.get('change_pct', 0)}%",
            f"涨停时间: {data.get('limit_time', '')}",
            f"封单: {data.get('seal_amount', 0):.0f}万",
            f"连板: {data.get('continuous_limit', 1)}板",
            f"",
            f"涨停原因:",
        ]
        
        for reason in data.get("reasons", []):
            lines.append(f"  • {reason}")
        
        lines.extend([
            f"",
            f"次日预判: {data.get('next_day_predict', '')}",
            f"操作建议: {data.get('suggestion', '')}",
            f"",
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(lines)
    
    async def send_selection_signal(self, signal_data: Dict) -> bool:
        """发送选股信号"""
        score = signal_data.get("score", {})
        level = SignalLevel(signal_data.get("signal_level", "C"))
        
        content = self._format_selection_content(signal_data)
        
        message = NotificationMessage(
            title=f"【选股信号-{level.value}级】{signal_data.get('name', '')}",
            content=content,
            level=level,
            channels=["wechat", "dingtalk"]
        )
        
        results = await self.send_signal(message)
        return any(results.values())
    
    def _format_selection_content(self, data: Dict) -> str:
        """格式化选股信号内容"""
        score = data.get("score", {})
        
        lines = [
            f"股票: {data.get('name', '')} ({data.get('code', '')})",
            f"综合评分: {score.get('total_score', 0):.1f}分",
            f"",
            f"四维评分:",
            f"  • 基本面: {score.get('fundamental_score', 0):.1f}分",
            f"  • 量价: {score.get('volume_price_score', 0):.1f}分",
            f"  • 资金: {score.get('fund_flow_score', 0):.1f}分",
            f"  • 情绪: {score.get('sentiment_score', 0):.1f}分",
            f"",
            f"当前价: {data.get('current_price', 0)}",
            f"涨跌: {data.get('change_pct', 0)}%",
            f"",
            f"筛选理由:",
        ]
        
        for reason in data.get("reasons", []):
            lines.append(f"  • {reason}")
        
        lines.extend([
            f"",
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(lines)
