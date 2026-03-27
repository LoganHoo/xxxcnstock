from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from core.models import StockSelectionSignal
from services.stock_service.engine import StockSelectionEngine

settings = get_settings()
logger = setup_logger("stock_service", log_file="system/stock_service.log")

# 全局引擎实例
engine = StockSelectionEngine()

app = FastAPI(
    title="XCNStock Stock Service",
    description="A股选股服务 - 多维度筛选与评分",
    version="0.1.0"
)


class ScreenRequest(BaseModel):
    """选股请求"""
    codes: List[str]
    min_score: float = 60.0


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "stock_service"}


@app.post("/api/v1/stock/screen")
async def screen_stocks(request: ScreenRequest):
    """执行选股筛选"""
    # 简化版本：模拟数据
    # 实际应从 Data Service 获取数据
    mock_stock_list = [
        {
            "code": code,
            "name": f"股票{code}",
            "fundamental": {
                "pe": 25,
                "pb": 3,
                "roe": 20,
                "revenue_growth": 30,
                "profit_growth": 35,
                "debt_ratio": 40,
                "price": 10.0,
                "change_pct": 2.5
            },
            "kline": None,
            "fund_flow": {"main_net_inflow": 1000000, "north_bound_days": 3, "big_order_ratio": 0.1},
            "sentiment": {"sector_rank": 3, "turnover_rate": 5, "market_up": True}
        }
        for code in request.codes[:10]  # 限制数量
    ]
    
    results = await engine.screen_stocks(mock_stock_list, request.min_score)
    
    return {
        "count": len(results),
        "data": [r.model_dump() for r in results]
    }


@app.get("/api/v1/stock/score/{code}")
async def get_stock_score(code: str):
    """获取个股评分"""
    # 模拟数据
    signal = await engine.analyze_stock(
        code=code,
        name=f"股票{code}",
        fundamental_data={
            "pe": 25,
            "pb": 3,
            "roe": 20,
            "revenue_growth": 30,
            "profit_growth": 35,
            "debt_ratio": 40,
            "price": 10.0,
            "change_pct": 2.5
        },
        kline_data=None,
        fund_flow_data={"main_net_inflow": 1000000},
        sentiment_data={"turnover_rate": 5}
    )
    
    return signal.model_dump()


@app.get("/api/v1/stock/rank")
async def get_stock_rank(top: int = 20):
    """获取选股排行榜"""
    # 模拟数据
    return {
        "count": 0,
        "data": []
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.STOCK_SERVICE_PORT,
        reload=True
    )
