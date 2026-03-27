from fastapi import FastAPI
from typing import List
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from services.limit_service.engine import LimitUpEngine
from services.limit_service.analyzers.pre_limit import PreLimitPredictor

settings = get_settings()
logger = setup_logger("limit_service", log_file="system/limit_service.log")

# 全局实例
limit_engine = LimitUpEngine()
pre_limit_predictor = PreLimitPredictor()

app = FastAPI(
    title="XCNStock Limit Service",
    description="A股打板服务 - 涨停分析与预判",
    version="0.1.0"
)


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "limit_service"}


@app.get("/api/v1/limit/pre-limit")
async def get_pre_limit_stocks():
    """获取涨停预判股票池"""
    # 简化版本：返回示例数据
    # 实际应从实时行情中筛选
    return {
        "count": 0,
        "data": []
    }


@app.get("/api/v1/limit/today")
async def get_today_limit_up():
    """获取今日涨停板分析"""
    # 模拟数据
    mock_stocks = [
        {
            "code": "000001",
            "name": "平安银行",
            "change_pct": 10.0,
            "limit_time": "09:35:00",
            "seal_amount": 50000,
            "seal_ratio": 3.5,
            "continuous_limit": 1,
            "open_count": 0,
            "sector": "银行"
        }
    ]
    
    signals = await limit_engine.analyze_limit_pool(mock_stocks)
    
    return {
        "count": len(signals),
        "data": [s.model_dump() for s in signals]
    }


@app.get("/api/v1/limit/{code}/analysis")
async def get_limit_analysis(code: str):
    """获取个股涨停分析"""
    # 模拟数据
    mock_data = {
        "code": code,
        "name": f"股票{code}",
        "change_pct": 10.0,
        "limit_time": "10:30:00",
        "seal_amount": 30000,
        "seal_ratio": 2.0,
        "continuous_limit": 1,
        "open_count": 0,
        "sector": "科技"
    }
    
    signal = await limit_engine.analyze_limit_stock(mock_data)
    return signal.model_dump()


@app.get("/api/v1/limit/{code}/predict")
async def get_next_day_predict(code: str):
    """获取次日预判"""
    mock_data = {
        "code": code,
        "change_pct": 8.5,
        "volume_ratio": 2.5,
        "turnover_rate": 8.0,
        "sector_change": 3.0,
        "sector_limit_count": 5
    }
    
    result = pre_limit_predictor.predict(mock_data)
    return result


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.LIMIT_SERVICE_PORT,
        reload=True
    )
