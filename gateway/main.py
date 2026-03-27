from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
import uvicorn
from typing import Optional
import logging

from core.config import get_settings
from core.logger import setup_logger

settings = get_settings()
logger = setup_logger("gateway", log_file="system/gateway.log")

# 服务地址映射
SERVICES = {
    "data": f"http://127.0.0.1:{settings.DATA_SERVICE_PORT}",
    "stock": f"http://127.0.0.1:{settings.STOCK_SERVICE_PORT}",
    "limit": f"http://127.0.0.1:{settings.LIMIT_SERVICE_PORT}",
    "notify": f"http://127.0.0.1:{settings.NOTIFY_SERVICE_PORT}",
}

app = FastAPI(
    title="XCNStock API Gateway",
    description="A股专业交易系统 - 统一API网关",
    version="0.1.0"
)


# 全局HTTP客户端
http_client = httpx.AsyncClient(timeout=30.0)


@app.on_event("startup")
async def startup():
    logger.info("API网关启动中...")


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    logger.info("API网关已关闭")


@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "service": "gateway",
        "services": {
            "data": SERVICES["data"],
            "stock": SERVICES["stock"],
            "limit": SERVICES["limit"],
            "notify": SERVICES["notify"]
        }
    }


# ==================== 数据服务路由 ====================

@app.get("/api/v1/quote/realtime")
async def get_realtime_quotes():
    """获取实时行情"""
    try:
        response = await http_client.get(f"{SERVICES['data']}/api/v1/quote/realtime")
        return response.json()
    except Exception as e:
        logger.error(f"获取实时行情失败: {e}")
        return JSONResponse(status_code=503, content={"error": "数据服务不可用"})


@app.get("/api/v1/quote/kline/{code}")
async def get_kline(code: str):
    """获取K线数据"""
    try:
        response = await http_client.get(f"{SERVICES['data']}/api/v1/quote/kline/{code}")
        return response.json()
    except Exception as e:
        logger.error(f"获取K线数据失败: {e}")
        return JSONResponse(status_code=503, content={"error": "数据服务不可用"})


@app.get("/api/v1/limitup/today")
async def get_today_limit_up():
    """获取今日涨停池"""
    try:
        response = await http_client.get(f"{SERVICES['data']}/api/v1/limitup/today")
        return response.json()
    except Exception as e:
        logger.error(f"获取涨停池失败: {e}")
        return JSONResponse(status_code=503, content={"error": "数据服务不可用"})


# ==================== 选股服务路由 ====================

@app.get("/api/v1/stock/rank")
async def get_stock_rank(top: int = 20):
    """获取选股排行榜"""
    try:
        response = await http_client.get(f"{SERVICES['stock']}/api/v1/stock/rank", params={"top": top})
        return response.json()
    except Exception as e:
        logger.error(f"获取选股排行榜失败: {e}")
        return JSONResponse(status_code=503, content={"error": "选股服务不可用"})


@app.get("/api/v1/stock/score/{code}")
async def get_stock_score(code: str):
    """获取个股评分"""
    try:
        response = await http_client.get(f"{SERVICES['stock']}/api/v1/stock/score/{code}")
        return response.json()
    except Exception as e:
        logger.error(f"获取个股评分失败: {e}")
        return JSONResponse(status_code=503, content={"error": "选股服务不可用"})


# ==================== 打板服务路由 ====================

@app.get("/api/v1/limit/pre-limit")
async def get_pre_limit_stocks():
    """获取涨停预判股票池"""
    try:
        response = await http_client.get(f"{SERVICES['limit']}/api/v1/limit/pre-limit")
        return response.json()
    except Exception as e:
        logger.error(f"获取涨停预判失败: {e}")
        return JSONResponse(status_code=503, content={"error": "打板服务不可用"})


@app.get("/api/v1/limit/{code}/analysis")
async def get_limit_analysis(code: str):
    """获取个股涨停分析"""
    try:
        response = await http_client.get(f"{SERVICES['limit']}/api/v1/limit/{code}/analysis")
        return response.json()
    except Exception as e:
        logger.error(f"获取涨停分析失败: {e}")
        return JSONResponse(status_code=503, content={"error": "打板服务不可用"})


@app.get("/api/v1/limit/{code}/predict")
async def get_next_day_predict(code: str):
    """获取次日预判"""
    try:
        response = await http_client.get(f"{SERVICES['limit']}/api/v1/limit/{code}/predict")
        return response.json()
    except Exception as e:
        logger.error(f"获取次日预判失败: {e}")
        return JSONResponse(status_code=503, content={"error": "打板服务不可用"})


# ==================== 通知服务路由 ====================

@app.get("/api/v1/notify/channels")
async def get_notify_channels():
    """获取通知渠道状态"""
    try:
        response = await http_client.get(f"{SERVICES['notify']}/api/v1/notify/channels")
        return response.json()
    except Exception as e:
        logger.error(f"获取通知渠道状态失败: {e}")
        return JSONResponse(status_code=503, content={"error": "通知服务不可用"})


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.GATEWAY_PORT,
        reload=True
    )
