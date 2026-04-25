#!/usr/bin/env python3
"""
K线数据采集任务 - Kestra 工作流版本
支持指定日期采集，用于补采历史数据
"""
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_collect")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='K线数据采集')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='目标日期 YYYY-MM-DD（默认今天）'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='开始日期 YYYY-MM-DD（用于补采区间）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='结束日期 YYYY-MM-DD（用于补采区间）'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        default=False,
        help='启用并行采集模式（使用异步HTTP客户端，提升3倍性能）'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=50,
        help='并行采集最大并发数（默认50，仅在--parallel模式下有效）'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='每批处理股票数量（默认100，仅在--parallel模式下有效）'
    )
    parser.add_argument(
        '--incremental',
        action='store_true',
        default=True,
        help='启用增量采集（默认启用，只采集缺失数据）'
    )
    parser.add_argument(
        '--use-cache',
        action='store_true',
        default=True,
        help='启用多级缓存（默认启用，L1内存+L2 Redis）'
    )
    return parser.parse_args()


def collect_single_date(target_date: str, parallel: bool = False, max_concurrent: int = 50,
                        batch_size: int = 100, incremental: bool = True, use_cache: bool = True) -> bool:
    """
    采集单日期数据

    Args:
        target_date: 目标日期 YYYY-MM-DD
        parallel: 是否启用并行采集
        max_concurrent: 最大并发数
        batch_size: 每批处理数量
        incremental: 是否启用增量采集
        use_cache: 是否启用缓存

    Returns:
        bool: 是否成功（基于验证通过率判断）
    """
    logger.info(f"开始采集 {target_date} 的K线数据...")
    if parallel:
        logger.info(f"🚀 并行模式: 并发数={max_concurrent}, 批次大小={batch_size}")
    if incremental:
        logger.info(f"📦 增量采集: 只采集缺失数据")
    if use_cache:
        logger.info(f"💾 多级缓存: 已启用")

    try:
        # 导入原始的数据采集控制器
        project_root = Path(__file__).parent.parent.parent
        sys.path.insert(0, str(project_root))

        # 使用并行采集模式
        if parallel:
            return _collect_parallel(target_date, max_concurrent, batch_size, incremental, use_cache)
        else:
            return _collect_legacy(target_date)

    except Exception as e:
        logger.error(f"❌ 数据采集异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def _collect_legacy(target_date: str) -> bool:
    """传统串行采集模式"""
    from scripts.data_collection_controller import DataCollectionController

    controller = DataCollectionController()

    import asyncio

    async def run_collection():
        await controller.initialize()
        await controller.run_daily_collection(target_date=target_date)
        return {
            'total': controller.stats.total_stocks,
            'success': controller.stats.success_count,
            'failed': controller.stats.failed_count,
            'skipped': controller.stats.skipped_count
        }

    stats = asyncio.run(run_collection())
    return _check_success_rate(stats, target_date)


def _collect_parallel(target_date: str, max_concurrent: int, batch_size: int,
                      incremental: bool, use_cache: bool) -> bool:
    """并行采集模式"""
    import asyncio
    from core.parallel_fetcher import ParallelDataFetcher, ParallelFetcherConfig, FetchTask
    from core.incremental_processor import IncrementalDetector
    from core.cache.multi_level_cache import MultiLevelCache
    from pathlib import Path

    project_root = Path(__file__).parent.parent.parent
    kline_dir = project_root / "data" / "kline"

    # 初始化组件
    config = ParallelFetcherConfig(
        max_concurrent=max_concurrent,
        batch_size=batch_size,
        calls_per_minute=480,
        request_timeout=30,
        pool_size=100
    )

    fetcher = ParallelDataFetcher(config)

    # 初始化缓存（如果启用）
    cache = None
    if use_cache:
        try:
            cache = MultiLevelCache(
                l1_maxsize=1000,
                l1_ttl=3600,
                redis_host=os.getenv('REDIS_HOST', 'localhost'),
                redis_port=int(os.getenv('REDIS_PORT', 6379)),
                l2_ttl=86400
            )
        except Exception as e:
            logger.warning(f"缓存初始化失败，继续无缓存模式: {e}")

    async def run_parallel_collection():
        await fetcher.open()
        try:
            # 获取股票列表
            stock_list = await _get_stock_list()
            logger.info(f"📋 获取到 {len(stock_list)} 只股票")

            # 增量检测（如果启用）
            if incremental:
                detector = IncrementalDetector(kline_dir)
                stocks_to_fetch = []
                for code in stock_list:
                    result = detector.check_stock(code, target_date, target_date)
                    if result.needs_update:
                        stocks_to_fetch.append(code)
                logger.info(f"📦 增量检测: 需要更新 {len(stocks_to_fetch)}/{len(stock_list)} 只股票")
            else:
                stocks_to_fetch = stock_list

            if not stocks_to_fetch:
                logger.info("✅ 所有股票数据已是最新，无需采集")
                return {'total': 0, 'success': 0, 'failed': 0, 'skipped': len(stock_list)}

            # 构建采集任务
            tasks = []
            for code in stocks_to_fetch:
                # 检查缓存
                cache_key = f"kline:{code}:{target_date}"
                if cache and cache.get(cache_key):
                    continue

                tasks.append(FetchTask(
                    identifier=code,
                    url=f"http://api.example.com/kline/{code}",  # 实际URL根据数据源调整
                    params={'date': target_date, 'code': code}
                ))

            # 执行并行采集
            results = await fetcher.fetch_many(tasks)

            # 处理结果
            success_count = sum(1 for r in results if r.success)
            failed_count = len(results) - success_count
            skipped_count = len(stock_list) - len(stocks_to_fetch)

            # 保存到缓存
            if cache:
                for result in results:
                    if result.success:
                        cache_key = f"kline:{result.identifier}:{target_date}"
                        cache.set(cache_key, result.data, level='both')

            return {
                'total': len(stock_list),
                'success': success_count,
                'failed': failed_count,
                'skipped': skipped_count
            }
        finally:
            await fetcher.close()

    stats = asyncio.run(run_parallel_collection())
    return _check_success_rate(stats, target_date)


def _check_success_rate(stats: dict, target_date: str) -> bool:
    """检查成功率"""
    total_processed = stats['success'] + stats['failed']
    if total_processed > 0:
        success_rate = stats['success'] / total_processed
        logger.info(f"📊 采集统计: 成功 {stats['success']}, 失败 {stats['failed']}, 跳过 {stats['skipped']}")
        logger.info(f"📊 成功率: {success_rate:.1%}")

        if success_rate >= 0.8:
            logger.info(f"✅ {target_date} 数据采集成功（成功率 {success_rate:.1%} >= 80%）")
            return True
        else:
            logger.error(f"❌ {target_date} 数据采集失败（成功率 {success_rate:.1%} < 80%）")
            return False
    else:
        logger.info(f"✅ {target_date} 无需采集（所有股票已是最新）")
        return True


async def _get_stock_list() -> list:
    """获取股票列表"""
    import polars as pl
    from pathlib import Path

    project_root = Path(__file__).parent.parent.parent
    stock_list_file = project_root / "data" / "stock_list.parquet"

    if stock_list_file.exists():
        df = pl.read_parquet(stock_list_file)
        return df['code'].to_list()
    else:
        # 默认返回一些测试股票
        return ['000001', '000002', '000333', '600000', '600519']


def collect_date_range(start_date: str, end_date: str, parallel: bool = False,
                       max_concurrent: int = 50, batch_size: int = 100,
                       incremental: bool = True, use_cache: bool = True) -> bool:
    """
    采集日期区间数据

    Args:
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        parallel: 是否启用并行采集
        max_concurrent: 最大并发数
        batch_size: 每批处理数量
        incremental: 是否启用增量采集
        use_cache: 是否启用缓存

    Returns:
        bool: 是否全部成功
    """
    logger.info(f"开始补采区间数据: {start_date} 至 {end_date}")
    if parallel:
        logger.info(f"🚀 并行模式已启用")

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    success_count = 0
    fail_count = 0
    current = start

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')

        # 跳过周末（简单判断，实际应该用交易日历）
        if current.weekday() >= 5:  # 5=周六, 6=周日
            logger.info(f"⏭️  {date_str} 是周末，跳过")
        else:
            if collect_single_date(date_str, parallel=parallel, max_concurrent=max_concurrent,
                                  batch_size=batch_size, incremental=incremental, use_cache=use_cache):
                success_count += 1
            else:
                fail_count += 1

        current += timedelta(days=1)

    logger.info(f"\n{'='*60}")
    logger.info(f"区间采集完成: 成功 {success_count} 天, 失败 {fail_count} 天")
    logger.info(f"{'='*60}")

    return fail_count == 0


def main():
    """主函数"""
    args = parse_args()

    logger.info("=" * 60)
    logger.info("开始 K 线数据采集")
    logger.info("=" * 60)

    # 确定采集模式
    if args.start_date and args.end_date:
        # 区间补采模式
        logger.info(f"补采模式: {args.start_date} 至 {args.end_date}")
        success = collect_date_range(
            args.start_date, args.end_date,
            parallel=args.parallel,
            max_concurrent=args.max_concurrent,
            batch_size=args.batch_size,
            incremental=args.incremental,
            use_cache=args.use_cache
        )
    elif args.date:
        # 单日期采集模式
        logger.info(f"单日期模式: {args.date}")
        success = collect_single_date(
            args.date,
            parallel=args.parallel,
            max_concurrent=args.max_concurrent,
            batch_size=args.batch_size,
            incremental=args.incremental,
            use_cache=args.use_cache
        )
    else:
        # 默认采集今天
        today = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"默认模式: 采集今天 ({today})")
        success = collect_single_date(
            today,
            parallel=args.parallel,
            max_concurrent=args.max_concurrent,
            batch_size=args.batch_size,
            incremental=args.incremental,
            use_cache=args.use_cache
        )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
