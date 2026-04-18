#!/usr/bin/env python3
"""
APScheduler 调度器启动脚本

启动 APScheduler 服务，执行定时数据采集任务

使用方法:
    python scripts/run_scheduler.py
    
后台运行:
    nohup python scripts/run_scheduler.py > logs/scheduler.log 2>&1 &
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import signal
import asyncio
from datetime import datetime
from services.data_service.scheduler import DataScheduler, DailyScheduler
from core.logger import setup_logger

logger = setup_logger("run_scheduler", log_file="system/run_scheduler.log")

# 全局调度器实例
data_scheduler = None
daily_scheduler = None


def signal_handler(signum, frame):
    """信号处理"""
    print(f"\n[{datetime.now()}] 收到信号 {signum}，正在停止调度器...")
    logger.info(f"收到信号 {signum}，正在停止调度器...")
    
    if data_scheduler:
        data_scheduler.stop()
    if daily_scheduler:
        daily_scheduler.stop()
    
    print(f"[{datetime.now()}] 调度器已停止")
    logger.info("调度器已停止")
    sys.exit(0)


async def run_data_scheduler():
    """运行数据调度器 (AsyncIOScheduler)"""
    global data_scheduler
    
    print(f"[{datetime.now()}] 正在启动数据调度器...")
    logger.info("正在启动数据调度器...")
    
    data_scheduler = DataScheduler()
    data_scheduler.start()
    
    print(f"[{datetime.now()}] 数据调度器已启动")
    print(f"  - 实时行情: 每分钟 (9:30-11:30, 13:00-15:00)")
    print(f"  - 涨停池监控: 每分钟 (9:30-11:30, 13:00-15:00)")
    print(f"  - 日K线采集: 每日16:00")
    logger.info("数据调度器已启动")
    
    # 保持运行
    try:
        while True:
            await asyncio.sleep(60)
            # 打印心跳
            jobs = data_scheduler.scheduler.get_jobs()
            logger.debug(f"调度器运行中，任务数: {len(jobs)}")
    except asyncio.CancelledError:
        pass


def run_daily_scheduler():
    """运行每日任务调度器 (BackgroundScheduler)"""
    global daily_scheduler
    
    print(f"[{datetime.now()}] 正在启动每日任务调度器...")
    logger.info("正在启动每日任务调度器...")
    
    daily_scheduler = DailyScheduler()
    daily_scheduler.start()
    
    print(f"[{datetime.now()}] 每日任务调度器已启动")
    print(f"  - 数据采集: 15:30")
    print(f"  - 数据验证: 16:00")
    print(f"  - 当日复盘: 16:30")
    print(f"  - 次日选股: 17:00")
    print(f"  - 早间推送: 08:30")
    print(f"  - 开盘处理: 09:30")
    logger.info("每日任务调度器已启动")


def main():
    """主函数"""
    print("=" * 80)
    print("🚀 APScheduler 调度器启动")
    print("=" * 80)
    print(f"启动时间: {datetime.now()}")
    print()
    
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 启动每日任务调度器 (在后台线程运行)
    run_daily_scheduler()
    
    # 启动数据调度器 (在主线程运行 asyncio)
    try:
        asyncio.run(run_data_scheduler())
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
    except Exception as e:
        logger.error(f"调度器异常: {e}")
        print(f"[{datetime.now()}] 调度器异常: {e}")
        raise


if __name__ == "__main__":
    main()
