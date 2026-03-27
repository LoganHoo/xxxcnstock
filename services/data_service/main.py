from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from services.data_service.scheduler import DataScheduler
from services.data_service.fetchers.quote import QuoteFetcher
from services.data_service.fetchers.limitup import LimitUpFetcher

settings = get_settings()
logger = setup_logger("data_service", log_file="system/data_service.log")

# 全局实例
data_scheduler = DataScheduler()
quote_fetcher = QuoteFetcher()
limitup_fetcher = LimitUpFetcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    logger.info("数据服务启动中...")
    data_scheduler.start()
    yield
    logger.info("数据服务关闭中...")
    data_scheduler.stop()


app = FastAPI(
    title="XCNStock Data Service",
    description="A股数据服务 - 行情获取与存储",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "data_service"}


@app.get("/api/v1/quote/realtime")
async def get_realtime_quotes():
    """获取实时行情"""
    quotes = await quote_fetcher.fetch_realtime_quotes()
    return {"count": len(quotes), "data": [q.model_dump() for q in quotes[:100]]}


@app.get("/api/v1/quote/kline/{code}")
async def get_kline(code: str):
    """获取K线数据"""
    df = await quote_fetcher.fetch_kline(code)
    if df is not None:
        return {"code": code, "count": len(df), "data": df.to_dict(orient="records")[-100:]}
    return {"code": code, "count": 0, "data": []}


@app.get("/api/v1/limitup/today")
async def get_today_limit_up():
    """获取今日涨停池"""
    stocks = await limitup_fetcher.fetch_limit_up_pool()
    return {
        "count": len(stocks),
        "data": [{
            "code": s.code,
            "name": s.name,
            "change_pct": s.change_pct,
            "limit_time": s.limit_time,
            "seal_amount": s.seal_amount,
            "continuous_limit": s.continuous_limit
        } for s in stocks]
    }


@app.get("/api/v1/scheduler/jobs")
async def get_scheduler_jobs():
    """获取调度任务状态"""
    jobs = data_scheduler.scheduler.get_jobs()
    return {
        "jobs": [{
            "id": job.id,
            "next_run": str(job.next_run_time)
        } for job in jobs]
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.DATA_SERVICE_PORT,
        reload=True
    )
