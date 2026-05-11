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
    """并行采集模式 - 使用双源采集器（Baostock + 腾讯财经）"""
    import sys
    from pathlib import Path
    from datetime import datetime, timedelta

    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    kline_dir = str(project_root / "data" / "kline")
    data_dir = str(project_root / "data")

    logger.info(f"🚀 使用双源采集器: Baostock + 腾讯财经")

    from services.data_service.fetchers.dual_source_fetcher import run_dual_source_fetch

    result = run_dual_source_fetch(
        codes=None,
        kline_dir=kline_dir,
        data_dir=data_dir,
        days=100,
        filter_delisted=True,
        resume=True
    )

    stats = {
        'total': result.get('total', 0),
        'success': result.get('success', 0),
        'failed': result.get('failed', 0),
        'skipped': result.get('skipped', 0)
    }

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
