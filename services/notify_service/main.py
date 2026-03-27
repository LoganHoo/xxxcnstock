from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from core.models import NotificationMessage, SignalLevel
from services.notify_service.signal_hub import SignalHub

settings = get_settings()
logger = setup_logger("notify_service", log_file="system/notify_service.log")

# 全局实例
signal_hub = SignalHub()

app = FastAPI(
    title="XCNStock Notify Service",
    description="A股通知服务 - 多渠道推送",
    version="0.1.0"
)


class NotifyRequest(BaseModel):
    """通知请求"""
    title: str
    content: str
    level: str = "B"
    channels: List[str] = ["wechat", "dingtalk"]


class LimitUpNotifyRequest(BaseModel):
    """涨停通知请求"""
    code: str
    name: str
    change_pct: float
    limit_time: str
    seal_amount: float
    continuous_limit: int = 1
    signal_level: str = "B"
    reasons: List[str] = []
    next_day_predict: str = ""
    suggestion: str = ""


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "notify_service"}


@app.post("/api/v1/notify/send")
async def send_notification(request: NotifyRequest):
    """发送通知"""
    message = NotificationMessage(
        title=request.title,
        content=request.content,
        level=SignalLevel(request.level),
        channels=request.channels
    )
    
    results = await signal_hub.send_signal(message)
    
    return {
        "success": any(results.values()),
        "results": results
    }


@app.post("/api/v1/notify/limit-up")
async def send_limit_up_notification(request: LimitUpNotifyRequest):
    """发送涨停通知"""
    signal_data = request.model_dump()
    
    success = await signal_hub.send_limit_up_signal(signal_data)
    
    return {"success": success}


@app.get("/api/v1/notify/channels")
async def get_channels():
    """获取通知渠道状态"""
    return {
        "wechat": signal_hub.wechat.is_configured(),
        "dingtalk": signal_hub.dingtalk.is_configured(),
        "email": signal_hub.email.is_configured()
    }


@app.get("/api/v1/notify/history")
async def get_history(limit: int = 50):
    """获取通知历史"""
    # 简化版本：返回空列表
    return {
        "count": 0,
        "data": []
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.NOTIFY_SERVICE_PORT,
        reload=True
    )
