#!/usr/bin/env python3
"""
通过微服务执行数据采集
替代原有的直接采集脚本，统一通过data_service进行采集
"""
import sys
import argparse
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.data_service.client import DataServiceClient, check_service
from core.logger import setup_logger

logger = setup_logger("collect_via_service", log_file="system/collect_service.log")


def main():
    parser = argparse.ArgumentParser(description='通过微服务执行数据采集')
    parser.add_argument(
        '--task',
        type=str,
        choices=['all', 'list', 'kline', 'fundamental', 'check'],
        default='all',
        help='采集任务类型'
    )
    parser.add_argument(
        '--wait',
        action='store_true',
        help='等待任务完成（后台任务不等待）'
    )
    
    args = parser.parse_args()
    
    # 检查服务是否运行
    logger.info("检查数据服务状态...")
    if not check_service():
        logger.error("❌ 数据服务未运行")
        print("\n请先启动数据服务:")
        print("  python services/data_service/main.py")
        print("\n或者使用后台启动:")
        print("  nohup python services/data_service/main.py > system/data_service.log 2>&1 &")
        sys.exit(1)
    
    logger.info("✅ 数据服务运行正常")
    
    client = DataServiceClient()
    
    # 执行任务
    if args.task == 'all':
        logger.info("🚀 执行完整采集流程...")
        success = client.collect_all()
        if success:
            print("✅ 完整采集流程已启动")
            print("📊 包含: 股票列表 + K线数据 + 基本面数据")
        
    elif args.task == 'list':
        logger.info("📋 采集股票列表...")
        success = client.collect_stock_list()
        if success:
            print("✅ 股票列表采集已启动")
        
    elif args.task == 'kline':
        logger.info("📈 采集K线数据...")
        success = client.collect_kline()
        if success:
            print("✅ K线数据采集已启动")
        
    elif args.task == 'fundamental':
        logger.info("📊 采集基本面数据...")
        success = client.collect_fundamental()
        if success:
            print("✅ 基本面数据采集已启动")
        
    elif args.task == 'check':
        logger.info("🔍 检查数据服务状态...")
        
        # 健康检查
        if client.health_check():
            print("✅ 数据服务健康")
        
        # 获取调度任务
        jobs = client.get_scheduler_jobs()
        if jobs:
            print(f"\n📅 定时任务 ({len(jobs.get('jobs', []))}个):")
            for job in jobs.get('jobs', []):
                print(f"  - {job['id']}: 下次运行 {job['next_run']}")
        
        # 获取股票列表统计
        stock_list = client.get_stock_list()
        if stock_list:
            print(f"\n📋 股票列表: {stock_list.get('count', 0)} 只")
        
        # 获取实时行情统计
        quotes = client.get_realtime_quotes()
        if quotes:
            print(f"📈 实时行情: {quotes.get('count', 0)} 条")
        
        # 获取涨停池
        limit_up = client.get_limit_up()
        if limit_up:
            print(f"🚀 涨停池: {limit_up.get('count', 0)} 只")
        
        print("\n✅ 检查完成")
        return
    
    if args.wait and args.task != 'check':
        print("\n⏳ 等待任务完成...")
        print("(按 Ctrl+C 取消等待，任务将在后台继续执行)")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n✅ 任务在后台继续执行")
    else:
        print("\n✅ 任务已在后台启动")
        print("📁 查看日志: tail -f system/data_service.log")


if __name__ == "__main__":
    main()
