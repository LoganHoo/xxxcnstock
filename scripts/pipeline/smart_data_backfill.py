#!/usr/bin/env python3
"""
智能数据补采脚本

功能：
1. 识别真正需要补采的活跃股票（排除退市/停牌）
2. 自动补采缺失数据
3. 过滤已退市/停牌股票
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("smart_backfill")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
from core.delisting_guard import get_delisting_guard
from services.data_service.fetchers.async_kline_fetcher import AsyncKlineFetcher, AsyncConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='智能数据补采')
    parser.add_argument(
        '--target-date',
        type=str,
        default='2026-04-24',
        help='目标日期 YYYY-MM-DD（默认2026-04-24）'
    )
    parser.add_argument(
        '--max-age-days',
        type=int,
        default=7,
        help='最大数据年龄（天），超过此值视为需要更新（默认7天）'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅分析，不执行补采'
    )
    parser.add_argument(
        '--max-stocks',
        type=int,
        default=None,
        help='最大补采股票数（用于测试）'
    )
    return parser.parse_args()


def analyze_data_freshness(target_date: str, max_age_days: int) -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    分析数据新鲜度，识别需要补采的股票

    Returns:
        (活跃需更新, 已退市, 长期停牌, 数据最新)
    """
    logger.info("=" * 60)
    logger.info("🔍 分析数据新鲜度")
    logger.info("=" * 60)

    kline_dir = Path("data/kline")
    parquet_files = list(kline_dir.glob("*.parquet"))

    target = datetime.strptime(target_date, '%Y-%m-%d')
    cutoff_date = target - timedelta(days=max_age_days)

    delisting_guard = get_delisting_guard()

    active_needs_update = []  # 活跃但需更新
    delisted_stocks = []      # 已退市
    suspended_stocks = []     # 长期停牌
    up_to_date = []           # 数据最新

    for f in parquet_files:
        code = f.stem

        try:
            df = pl.read_parquet(f)

            # 获取最新日期
            if 'date' in df.columns:
                latest_str = str(df['date'].max())
            elif 'trade_date' in df.columns:
                latest_str = str(df['trade_date'].max())
            else:
                continue

            latest = datetime.strptime(latest_str, '%Y-%m-%d')

            # 检查是否已退市
            if delisting_guard.is_delisted_by_code(code):
                delisted_stocks.append((code, latest_str))
                continue

            # 检查数据新鲜度
            if latest_str == target_date:
                up_to_date.append(code)
            elif latest >= cutoff_date:
                # 数据较新（在max_age_days内），可能是停牌
                suspended_stocks.append((code, latest_str))
            else:
                # 数据较旧，需要更新
                active_needs_update.append((code, latest_str))

        except Exception as e:
            logger.warning(f"读取 {code} 失败: {e}")

    logger.info(f"\n📊 分析结果:")
    logger.info(f"  ✅ 数据最新: {len(up_to_date)} 只")
    logger.info(f"  ⚠️  活跃需更新: {len(active_needs_update)} 只")
    logger.info(f"  🚫 已退市: {len(delisted_stocks)} 只")
    logger.info(f"  ⏸️  长期停牌: {len(suspended_stocks)} 只")

    # 显示需要更新的股票示例
    if active_needs_update:
        logger.info(f"\n📋 活跃需更新的股票示例（前10）:")
        for code, date in sorted(active_needs_update, key=lambda x: x[1])[:10]:
            logger.info(f"  {code}: {date}")

    return (
        [code for code, _ in active_needs_update],
        [code for code, _ in delisted_stocks],
        [code for code, _ in suspended_stocks],
        up_to_date
    )


async def backfill_stocks(codes: List[str], target_date: str, max_stocks: int = None) -> Dict:
    """
    补采指定股票的数据

    Args:
        codes: 股票代码列表
        target_date: 目标日期
        max_stocks: 最大补采数量

    Returns:
        补采统计
    """
    if not codes:
        logger.info("✅ 没有需要补采的股票")
        return {'success': 0, 'failed': 0, 'total': 0}

    # 限制数量（用于测试）
    if max_stocks and len(codes) > max_stocks:
        logger.info(f"⚠️  限制补采数量: {max_stocks}/{len(codes)}")
        codes = codes[:max_stocks]

    logger.info("=" * 60)
    logger.info(f"🔄 开始补采 {len(codes)} 只股票")
    logger.info("=" * 60)

    kline_dir = Path("data/kline")

    # 创建异步获取器
    config = AsyncConfig(
        max_concurrent=3,
        semaphore_value=3,
        batch_size=30,
        batch_pause=3.0,
        request_delay=0.5,
        min_kline_rows=50
    )
    fetcher = AsyncKlineFetcher(config)

    # 执行补采
    results = await fetcher.fetch_all(
        codes=codes,
        kline_dir=kline_dir,
        days=365 * 3,
        filter_delisted=True
    )

    return results


def generate_report(
    target_date: str,
    active_needs_update: List[str],
    delisted_stocks: List[str],
    suspended_stocks: List[str],
    up_to_date: List[str],
    backfill_results: Dict = None
):
    """生成补采报告"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 智能补采报告")
    logger.info("=" * 60)

    logger.info(f"\n目标日期: {target_date}")
    logger.info(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    logger.info(f"\n📈 数据状态分布:")
    total = len(up_to_date) + len(active_needs_update) + len(delisted_stocks) + len(suspended_stocks)
    logger.info(f"  ✅ 数据最新: {len(up_to_date)} 只 ({len(up_to_date)/total*100:.1f}%)")
    logger.info(f"  🔄 已补采: {len(active_needs_update)} 只 ({len(active_needs_update)/total*100:.1f}%)")
    logger.info(f"  🚫 已退市: {len(delisted_stocks)} 只 ({len(delisted_stocks)/total*100:.1f}%)")
    logger.info(f"  ⏸️  长期停牌: {len(suspended_stocks)} 只 ({len(suspended_stocks)/total*100:.1f}%)")

    if backfill_results:
        logger.info(f"\n📊 补采结果:")
        logger.info(f"  ✅ 成功: {backfill_results['success']}")
        logger.info(f"  ⏭️  跳过: {backfill_results['skipped']}")
        logger.info(f"  ❌ 失败: {backfill_results['failed']}")
        logger.info(f"  📈 总行数: {backfill_results['total_rows']}")

    # 保存报告
    report = {
        'target_date': target_date,
        'analysis_time': datetime.now().isoformat(),
        'summary': {
            'total_stocks': total,
            'up_to_date': len(up_to_date),
            'backfilled': len(active_needs_update),
            'delisted': len(delisted_stocks),
            'suspended': len(suspended_stocks)
        },
        'backfill_results': {
            'success': backfill_results.get('success', 0),
            'skipped': backfill_results.get('skipped', 0),
            'failed': backfill_results.get('failed', 0),
            'total_rows': backfill_results.get('total_rows', 0)
        } if backfill_results else None,
        'delisted_stocks': delisted_stocks[:100],  # 只保存前100
        'suspended_stocks': suspended_stocks[:100]
    }

    report_path = Path("data/backfill_report.json")
    import json
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"\n📄 报告已保存: {report_path}")


async def main():
    """主函数"""
    args = parse_args()

    logger.info("=" * 60)
    logger.info("🚀 智能数据补采")
    logger.info("=" * 60)

    # 1. 分析数据新鲜度
    active_needs_update, delisted_stocks, suspended_stocks, up_to_date = analyze_data_freshness(
        args.target_date,
        args.max_age_days
    )

    # 2. 执行补采（如果不是dry-run）
    backfill_results = None
    if not args.dry_run and active_needs_update:
        backfill_results = await backfill_stocks(
            active_needs_update,
            args.target_date,
            args.max_stocks
        )
    elif args.dry_run:
        logger.info("\n🏃 干运行模式，不执行补采")

    # 3. 生成报告
    generate_report(
        args.target_date,
        active_needs_update,
        delisted_stocks,
        suspended_stocks,
        up_to_date,
        backfill_results
    )

    return 0


if __name__ == "__main__":
    asyncio.run(main())
