from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import pandas as pd
import logging

from core.config import get_settings
from core.logger import setup_logger
from services.data_service.fetchers.quote import QuoteFetcher
from services.data_service.fetchers.limitup import LimitUpFetcher
from services.data_service.storage.parquet_manager import ParquetManager

logger = setup_logger("scheduler", log_file="system/scheduler.log")


class DataScheduler:
    """数据调度器"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.settings = get_settings()
        self.quote_fetcher = QuoteFetcher()
        self.limitup_fetcher = LimitUpFetcher()
        self.storage = ParquetManager()
    
    async def job_realtime_quotes(self):
        """定时任务：获取实时行情"""
        logger.info("执行定时任务：获取实时行情")
        try:
            quotes = await self.quote_fetcher.fetch_realtime_quotes()
            if quotes:
                df = pd.DataFrame([q.model_dump() for q in quotes])
                self.storage.save(df, f"realtime/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        except Exception as e:
            logger.error(f"实时行情任务失败: {e}")
    
    async def job_limit_up_pool(self):
        """定时任务：获取涨停池"""
        logger.info("执行定时任务：获取涨停池")
        try:
            stocks = await self.limitup_fetcher.fetch_limit_up_pool()
            if stocks:
                df = pd.DataFrame([{
                    "code": s.code,
                    "name": s.name,
                    "change_pct": s.change_pct,
                    "limit_time": s.limit_time,
                    "seal_amount": s.seal_amount,
                    "open_count": s.open_count,
                    "continuous_limit": s.continuous_limit,
                    "sector": s.sector
                } for s in stocks])
                self.storage.save_daily_data(df, "limitup")
        except Exception as e:
            logger.error(f"涨停池任务失败: {e}")
    
    async def job_daily_kline(self):
        """定时任务：获取日K线"""
        logger.info("执行定时任务：获取日K线")
        try:
            limit_stocks = await self.limitup_fetcher.fetch_limit_up_pool()
            for stock in limit_stocks[:50]:  # 限制数量
                df = await self.quote_fetcher.fetch_kline(stock.code)
                if df is not None:
                    self.storage.save(df, f"kline/daily/{stock.code}.parquet")
        except Exception as e:
            logger.error(f"日K线任务失败: {e}")
    
    def setup_jobs(self):
        """配置定时任务"""
        # 盘中实时行情（每分钟）
        self.scheduler.add_job(
            self.job_realtime_quotes,
            CronTrigger(
                day_of_week="mon-fri",
                hour="9-11,13-15",
                minute="*/1"
            ),
            id="realtime_quotes",
            replace_existing=True
        )
        
        # 涨停池监控（盘中每分钟）
        self.scheduler.add_job(
            self.job_limit_up_pool,
            CronTrigger(
                day_of_week="mon-fri",
                hour="9-11,13-15",
                minute="*/1"
            ),
            id="limit_up_pool",
            replace_existing=True
        )
        
        # 日K线（每日16:00）
        self.scheduler.add_job(
            self.job_daily_kline,
            CronTrigger(
                day_of_week="mon-fri",
                hour=16,
                minute=0
            ),
            id="daily_kline",
            replace_existing=True
        )
        
        logger.info("定时任务配置完成")
    
    def start(self):
        """启动调度器"""
        if self.settings.SCHEDULE_ENABLED:
            self.setup_jobs()
            self.scheduler.start()
            logger.info("数据调度器已启动")
        else:
            logger.info("调度器未启用")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        logger.info("数据调度器已停止")


class DailyScheduler:
    """每日任务调度器"""
    
    def __init__(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        self.scheduler = BackgroundScheduler()
        self._setup_jobs()
    
    def _setup_jobs(self):
        """配置定时任务"""
        # 延迟导入避免循环依赖
        from scripts.daily_tasks.task_data_collect import DataCollectTask
        from scripts.daily_tasks.task_data_audit import DataAuditTask
        from scripts.daily_tasks.task_daily_review import DailyReviewTask
        from scripts.daily_tasks.task_stock_pick import StockPickTask
        from scripts.daily_tasks.task_morning_push import MorningPushTask
        from scripts.daily_tasks.task_open_process import OpenProcessTask
        
        # 15:30 数据采集
        self.scheduler.add_job(
            DataCollectTask().execute,
            CronTrigger(hour=15, minute=30),
            id="data_collect",
            name="数据采集"
        )
        
        # 16:00 数据验证
        self.scheduler.add_job(
            DataAuditTask().execute,
            CronTrigger(hour=16, minute=0),
            id="data_audit",
            name="数据验证"
        )
        
        # 16:30 当日复盘
        self.scheduler.add_job(
            DailyReviewTask().execute,
            CronTrigger(hour=16, minute=30),
            id="daily_review",
            name="当日复盘"
        )
        
        # 17:00 次日选股
        self.scheduler.add_job(
            StockPickTask().execute,
            CronTrigger(hour=17, minute=0),
            id="stock_pick",
            name="次日选股"
        )
        
        # 次日 08:30 报告推送
        self.scheduler.add_job(
            MorningPushTask().execute,
            CronTrigger(hour=8, minute=30),
            id="morning_push",
            name="早间推送"
        )
        
        # 09:30 开盘处理(一字涨停)
        self.scheduler.add_job(
            OpenProcessTask().execute,
            CronTrigger(hour=9, minute=30),
            id="open_process",
            name="开盘处理"
        )
        
        logger.info("每日任务配置完成")
    
    def start(self):
        """启动调度器"""
        self.scheduler.start()
        print(f"[{datetime.now()}] 每日任务调度器已启动")
        logger.info("每日任务调度器已启动")
    
    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        print(f"[{datetime.now()}] 调度器已停止")
        logger.info("每日任务调度器已停止")
