#!/usr/bin/env python3
"""
每日数据更新脚本

功能：
1. 每日更新股票列表（获取最新上市/退市信息）
2. 基于股票列表进行个股数据采集
3. 识别并补采缺失数据
4. 过滤已退市/停牌股票

使用方式:
    python scripts/pipeline/daily_data_update.py
    python scripts/pipeline/daily_data_update.py --date 2026-04-24
    python scripts/pipeline/daily_data_update.py --backfill-only
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import asyncio
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("daily_update")

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import pandas as pd
from core.delisting_guard import get_delisting_guard
from core.market_guardian import enforce_market_closed
from services.data_service.fetchers.stock_list_fetcher import fetch_stock_list, save_stock_list_to_parquet
from services.data_service.fetchers.async_kline_fetcher import AsyncKlineFetcher, AsyncConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='每日数据更新')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='目标日期 YYYY-MM-DD（默认今天）'
    )
    parser.add_argument(
        '--backfill-only',
        action='store_true',
        help='仅执行补采，不更新股票列表'
    )
    parser.add_argument(
        '--skip-collection',
        action='store_true',
        help='跳过数据采集，仅更新列表'
    )
    parser.add_argument(
        '--max-stocks',
        type=int,
        default=None,
        help='限制处理股票数量（用于测试）'
    )
    return parser.parse_args()


async def update_stock_list() -> Tuple[bool, int]:
    """
    更新股票列表

    Returns:
        (是否成功, 股票数量)
    """
    logger.info("=" * 60)
    logger.info("📋 更新股票列表")
    logger.info("=" * 60)

    try:
        # 直接使用异步方式获取股票列表
        from services.data_service.fetchers.unified_fetcher import get_unified_fetcher

        fetcher = await get_unified_fetcher()
        df = await fetcher.fetch_stock_list()

        if df.empty:
            logger.error("❌ 获取股票列表失败: 数据为空")
            return False, 0

        # 转换为标准格式
        stock_list = []
        for _, row in df.iterrows():
            stock_list.append({
                'code': str(row.get('code', '')),
                'name': str(row.get('name', '')),
                'industry': str(row.get('industry', '')),
                'tradeStatus': str(row.get('tradeStatus', '1')),
                'exchange': str(row.get('exchange', '')),
            })

        # 保存到parquet
        output_file = Path("data/stock_list.parquet")
        output_file.parent.mkdir(exist_ok=True)

        df_pl = pl.from_pandas(pd.DataFrame(stock_list))
        df_pl.write_parquet(output_file)

        logger.info(f"✅ 股票列表更新成功: {len(stock_list)} 只")
        return True, len(stock_list)

    except Exception as e:
        logger.error(f"❌ 更新股票列表异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0


def get_stock_list_from_file() -> List[str]:
    """从文件获取股票代码列表"""
    try:
        stock_list_path = Path("data/stock_list.parquet")
        if not stock_list_path.exists():
            logger.error(f"❌ 股票列表文件不存在: {stock_list_path}")
            return []

        df = pl.read_parquet(stock_list_path)
        codes = df['code'].to_list()
        logger.info(f"📊 从文件读取股票列表: {len(codes)} 只")
        return codes
    except Exception as e:
        logger.error(f"❌ 读取股票列表失败: {e}")
        return []


def analyze_collection_status(target_date: str) -> Tuple[List[str], List[str], List[str]]:
    """
    分析采集状态，识别需要更新的股票

    Returns:
        (需要采集, 已最新, 已退市/停牌)
    """
    logger.info("=" * 60)
    logger.info("🔍 分析采集状态")
    logger.info("=" * 60)

    # 获取股票列表
    codes = get_stock_list_from_file()
    if not codes:
        return [], [], []

    # 限制数量（用于测试）
    if args.max_stocks and len(codes) > args.max_stocks:
        codes = codes[:args.max_stocks]
        logger.info(f"⚠️ 限制处理数量: {len(codes)} 只")

    kline_dir = Path("data/kline")
    kline_dir.mkdir(exist_ok=True)

    delisting_guard = get_delisting_guard()

    needs_update = []   # 需要采集
    up_to_date = []     # 已最新
    delisted = []       # 已退市/停牌

    for code in codes:
        # 检查是否已退市
        if delisting_guard.is_delisted_by_code(code):
            delisted.append(code)
            continue

        # 检查K线数据文件
        kline_file = kline_dir / f"{code}.parquet"

        if not kline_file.exists():
            # 新上市股票，需要采集
            needs_update.append(code)
            continue

        try:
            df = pl.read_parquet(kline_file)

            # 获取最新日期
            if 'date' in df.columns:
                latest_str = str(df['date'].max())
            elif 'trade_date' in df.columns:
                latest_str = str(df['trade_date'].max())
            else:
                needs_update.append(code)
                continue

            # 检查是否最新
            if latest_str == target_date:
                up_to_date.append(code)
            else:
                # 检查数据年龄
                latest = datetime.strptime(latest_str, '%Y-%m-%d')
                target = datetime.strptime(target_date, '%Y-%m-%d')
                days_diff = (target - latest).days

                if days_diff > 7:
                    # 超过7天未更新，可能是停牌
                    delisted.append(code)
                else:
                    needs_update.append(code)

        except Exception as e:
            logger.warning(f"读取 {code} 数据失败: {e}")
            needs_update.append(code)

    logger.info(f"\n📊 分析结果:")
    logger.info(f"  ✅ 已最新: {len(up_to_date)} 只")
    logger.info(f"  🔄 需要采集: {len(needs_update)} 只")
    logger.info(f"  🚫 已退市/停牌: {len(delisted)} 只")

    return needs_update, up_to_date, delisted


async def collect_stocks(codes: List[str], target_date: str) -> Dict:
    """
    采集指定股票的数据

    Args:
        codes: 股票代码列表
        target_date: 目标日期

    Returns:
        采集统计
    """
    if not codes:
        logger.info("✅ 没有需要采集的股票")
        return {'success': 0, 'failed': 0, 'skipped': 0, 'total_rows': 0}

    logger.info("=" * 60)
    logger.info(f"🔄 开始采集 {len(codes)} 只股票")
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

    # 执行采集
    results = await fetcher.fetch_all(
        codes=codes,
        kline_dir=kline_dir,
        days=365 * 3,
        filter_delisted=True
    )

    return results


def generate_report(
    target_date: str,
    stock_count: int,
    needs_update: List[str],
    up_to_date: List[str],
    delisted: List[str],
    collection_results: Dict
):
    """生成更新报告"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 每日数据更新报告")
    logger.info("=" * 60)

    logger.info(f"\n目标日期: {target_date}")
    logger.info(f"更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    logger.info(f"\n📈 股票列表:")
    logger.info(f"  总股票数: {stock_count}")

    logger.info(f"\n📊 数据采集:")
    total = len(up_to_date) + len(needs_update) + len(delisted)
    logger.info(f"  ✅ 已最新: {len(up_to_date)} 只 ({len(up_to_date)/total*100:.1f}%)")
    logger.info(f"  🔄 已采集: {len(needs_update)} 只 ({len(needs_update)/total*100:.1f}%)")
    logger.info(f"  🚫 已退市/停牌: {len(delisted)} 只 ({len(delisted)/total*100:.1f}%)")

    if collection_results:
        logger.info(f"\n📊 采集详情:")
        logger.info(f"  ✅ 成功: {collection_results['success']}")
        logger.info(f"  ⏭️  跳过: {collection_results['skipped']}")
        logger.info(f"  ❌ 失败: {collection_results['failed']}")
        logger.info(f"  📈 总行数: {collection_results['total_rows']}")

    # 保存报告
    report = {
        'target_date': target_date,
        'update_time': datetime.now().isoformat(),
        'stock_list': {
            'total': stock_count
        },
        'collection': {
            'up_to_date': len(up_to_date),
            'collected': len(needs_update),
            'delisted': len(delisted),
            'success': collection_results.get('success', 0),
            'failed': collection_results.get('failed', 0),
            'total_rows': collection_results.get('total_rows', 0)
        }
    }

    report_path = Path("data/daily_update_report.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"\n📄 报告已保存: {report_path}")


async def main():
    """主函数"""
    global args
    args = parse_args()

    # 确定目标日期
    if args.date:
        target_date = args.date
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')

    logger.info("=" * 60)
    logger.info("🚀 每日数据更新")
    logger.info("=" * 60)
    logger.info(f"目标日期: {target_date}")

    stock_count = 0
    collection_results = None

    # 1. 更新股票列表
    if not args.backfill_only:
        success, stock_count = await update_stock_list()
        if not success:
            logger.error("❌ 股票列表更新失败，使用现有列表")
            stock_count = len(get_stock_list_from_file())
    else:
        logger.info("🏃 跳过股票列表更新（--backfill-only）")
        stock_count = len(get_stock_list_from_file())

    # 2. 分析采集状态
    needs_update, up_to_date, delisted = analyze_collection_status(target_date)

    # 3. 执行数据采集
    if not args.skip_collection and needs_update:
        collection_results = await collect_stocks(needs_update, target_date)
    elif args.skip_collection:
        logger.info("🏃 跳过数据采集（--skip-collection）")

    # 4. 生成报告
    generate_report(
        target_date,
        stock_count,
        needs_update,
        up_to_date,
        delisted,
        collection_results or {}
    )

    return 0


if __name__ == "__main__":
    asyncio.run(main())
