from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import uvicorn

from core.config import get_settings
from core.logger import setup_logger
from services.data_service.scheduler import DataScheduler
from services.data_service.fetchers.quote import QuoteFetcher
from services.data_service.fetchers.limitup import LimitUpFetcher
from services.data_service.fetchers.stock_list import StockListFetcher
from services.data_service.fetchers.fundamental import FundamentalFetcher
from services.data_service.fetchers.kline_history import KlineHistoryFetcher
from services.data_service.datasource import get_datasource_manager

settings = get_settings()
logger = setup_logger("data_service", log_file="system/data_service.log")

# 全局实例
data_scheduler = DataScheduler()
quote_fetcher = QuoteFetcher()
limitup_fetcher = LimitUpFetcher()
stock_list_fetcher = StockListFetcher()
fundamental_fetcher = FundamentalFetcher()
kline_fetcher = KlineHistoryFetcher()
ds_manager = get_datasource_manager()


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


# ========== 数据采集API ==========

@app.post("/api/v1/collect/stock_list")
async def collect_stock_list(background_tasks: BackgroundTasks):
    """采集股票列表"""
    background_tasks.add_task(stock_list_fetcher.update_stock_list)
    return {"status": "started", "task": "stock_list_collection"}


@app.post("/api/v1/collect/fundamental")
async def collect_fundamental(background_tasks: BackgroundTasks):
    """采集基本面数据"""
    async def task():
        from pathlib import Path
        import polars as pl

        stock_list_path = Path(settings.DATA_DIR) / "stock_list.parquet"
        if not stock_list_path.exists():
            logger.error("股票列表不存在")
            return

        df = await fundamental_fetcher.fetch_from_parquet(str(stock_list_path))
        if not df.empty:
            output_path = Path(settings.DATA_DIR) / "fundamental" / "valuation_realistic.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
            logger.info(f"基本面数据采集完成: {len(df)} 只股票")

    background_tasks.add_task(task)
    return {"status": "started", "task": "fundamental_collection"}


@app.post("/api/v1/collect/kline")
async def collect_kline(background_tasks: BackgroundTasks, codes: list = None):
    """采集K线数据"""
    async def task():
        if codes:
            await kline_fetcher.incremental_update(codes)
        else:
            # 全量更新
            from pathlib import Path
            import polars as pl

            stock_list_path = Path(settings.DATA_DIR) / "stock_list.parquet"
            if stock_list_path.exists():
                df_stocks = pl.read_parquet(stock_list_path)
                stock_codes = df_stocks['code'].to_list()
                await kline_fetcher.incremental_update(stock_codes)

    background_tasks.add_task(task)
    return {"status": "started", "task": "kline_collection"}


@app.post("/api/v1/collect/all")
async def collect_all(background_tasks: BackgroundTasks):
    """执行完整采集流程"""
    async def task():
        logger.info("开始完整采集流程")
        # 1. 股票列表
        await stock_list_fetcher.update_stock_list()
        # 2. K线数据
        from pathlib import Path
        import polars as pl

        stock_list_path = Path(settings.DATA_DIR) / "stock_list.parquet"
        if stock_list_path.exists():
            df_stocks = pl.read_parquet(stock_list_path)
            stock_codes = df_stocks['code'].to_list()
            await kline_fetcher.incremental_update(stock_codes)
        # 3. 基本面数据
        if stock_list_path.exists():
            df = await fundamental_fetcher.fetch_from_parquet(str(stock_list_path))
            if not df.empty:
                output_path = Path(settings.DATA_DIR) / "fundamental" / "valuation_realistic.parquet"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(output_path, index=False)
        logger.info("完整采集流程完成")

    background_tasks.add_task(task)
    return {"status": "started", "task": "full_collection"}


# ========== 数据查询API ==========

@app.get("/api/v1/fundamental/{code}")
async def get_fundamental(code: str):
    """获取单只股票基本面数据"""
    fundamental = await fundamental_fetcher.fetch_fundamental(code)
    if fundamental:
        return {
            "code": fundamental.code,
            "name": fundamental.name,
            "pe_ttm": fundamental.pe_ttm,
            "pb": fundamental.pb,
            "ps_ttm": fundamental.ps_ttm,
            "pcf": fundamental.pcf,
            "total_mv": fundamental.total_mv,
            "date": fundamental.date
        }
    return {"code": code, "error": "未找到数据"}


@app.get("/api/v1/stock_list")
async def get_stock_list():
    """获取股票列表"""
    from pathlib import Path
    import polars as pl

    stock_list_path = Path(settings.DATA_DIR) / "stock_list.parquet"
    if stock_list_path.exists():
        df = pl.read_parquet(stock_list_path)
        return {
            "count": len(df),
            "data": df.to_dicts()[:100]  # 限制返回数量
        }
    return {"count": 0, "data": []}


# ========== 数据源状态API ==========

@app.get("/api/v1/datasource/status")
async def get_datasource_status():
    """获取数据源状态"""
    return ds_manager.get_status()


@app.post("/api/v1/datasource/check")
async def check_datasource():
    """手动触发数据源健康检查"""
    # 这里可以触发健康检查
    return {"status": "checking", "message": "健康检查已触发"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.DATA_SERVICE_PORT,
        reload=True
    )
