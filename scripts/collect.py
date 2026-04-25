#!/usr/bin/env python3
"""
数据采集统一入口

支持三种采集场景：
1. 历史数据采集 (historical) - 收盘后执行
2. 实时数据采集 (realtime) - 定时执行
3. 盘中数据采集 (intraday) - 交易时段执行

使用方式:
    # 历史数据采集
    python scripts/collect.py historical --mode daily
    python scripts/collect.py historical --mode full
    python scripts/collect.py historical --mode incremental --codes 000001,000002

    # 实时数据采集
    python scripts/collect.py realtime --types quotes,limitup
    python scripts/collect.py realtime --schedule --interval 5

    # 盘中数据采集
    python scripts/collect.py intraday --codes 000001,000002 --duration 60

    # 检查市场状态
    python scripts/collect.py check-market
"""
import sys
import argparse
import asyncio
from pathlib import Path
from datetime import datetime
import json

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.logger import setup_logger
from core.market_guardian import is_trading_time, enforce_market_closed
from services.data_service.collectors import (
    HistoricalCollector,
    RealtimeCollector,
    IntradayCollector
)

logger = setup_logger("collect", log_file="system/collect.log")


def print_banner():
    """打印启动横幅"""
    print("=" * 70)
    print("🚀 xcnstock 数据采集系统")
    print("=" * 70)
    print()


def print_usage():
    """打印使用说明"""
    print("""
使用方式:

  1. 历史数据采集 (Historical)
     用于采集收盘后的历史K线数据
     
     python scripts/collect.py historical --mode daily
     python scripts/collect.py historical --mode full
     python scripts/collect.py historical --mode incremental --codes 000001,000002

  2. 实时数据采集 (Real-time)
     用于定时采集实时行情快照
     
     python scripts/collect.py realtime --types quotes,limitup
     python scripts/collect.py realtime --schedule --interval 5

  3. 盘中数据采集 (Intraday)
     用于交易时段内高频采集
     
     python scripts/collect.py intraday --codes 000001,000002 --duration 60

  4. 检查市场状态
     python scripts/collect.py check-market

参数说明:
  --mode      采集模式: daily(每日增量), full(全量), incremental(指定股票)
  --codes     股票代码，逗号分隔
  --types     数据类型: quotes(实时行情), limitup(涨停池), fundflow(资金流向)
  --schedule  定时模式
  --interval  定时间隔（分钟）
  --duration  运行时长（分钟）
""")


async def cmd_historical(args):
    """历史数据采集命令"""
    print("\n📊 历史数据采集")
    print("-" * 70)

    # 检查市场状态
    if args.mode == 'daily' and not args.skip_check:
        try:
            enforce_market_closed(target_date=args.date)
        except SystemExit:
            print("\n❌ 市场未收盘，无法采集当日数据")
            print("提示: 可以使用 --skip-check 跳过检查（仅测试）")
            return

    # 创建采集器
    collector = HistoricalCollector()
    await collector.initialize()

    # 执行采集
    if args.mode == 'daily':
        stats = await collector.run_daily_collection(target_date=args.date)
    elif args.mode == 'full':
        # 全量采集
        stats = await collector.run_daily_collection()
    elif args.mode == 'incremental':
        if not args.codes:
            print("❌ incremental 模式需要指定 --codes")
            return
        codes = [c.strip() for c in args.codes.split(',')]
        results = await collector.batch_collect_kline(codes)
        stats = {
            'success': True,
            'total': len(codes),
            'success_count': sum(1 for r in results.values() if r.success)
        }
    else:
        print(f"❌ 未知模式: {args.mode}")
        return

    # 输出结果
    print("\n" + "=" * 70)
    print("📈 采集结果")
    print("=" * 70)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


async def cmd_realtime(args):
    """实时数据采集命令"""
    print("\n📡 实时数据采集")
    print("-" * 70)

    # 创建采集器
    collector = RealtimeCollector()
    await collector.initialize()

    # 解析数据类型
    data_types = args.types.split(',') if args.types else ['quotes']

    if args.schedule:
        # 定时模式
        print(f"🔄 启动定时采集，间隔: {args.interval} 分钟")
        await collector.run_scheduled(interval_minutes=args.interval)
    else:
        # 单次采集
        stats = await collector.run_collection(data_types)

        # 输出结果
        print("\n" + "=" * 70)
        print("📈 采集结果")
        print("=" * 70)
        print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


async def cmd_intraday(args):
    """盘中数据采集命令"""
    print("\n⏰ 盘中数据采集")
    print("-" * 70)

    # 检查是否在交易时段
    if not is_trading_time():
        print("⚠️  当前不在交易时段")
        response = input("是否继续? (y/N): ")
        if response.lower() != 'y':
            return

    # 解析股票代码
    codes = args.codes.split(',') if args.codes else ['000001', '000002', '600000']

    # 创建采集器
    collector = IntradayCollector()

    print(f"🔄 启动盘中采集: {len(codes)} 只股票")
    print(f"⏱️  运行时长: {args.duration} 分钟")
    print("按 Ctrl+C 停止采集\n")

    try:
        # 运行采集
        result = await collector.run_collection(mode='tick', codes=codes)

        # 输出结果
        print("\n" + "=" * 70)
        print("📈 采集结果")
        print("=" * 70)
        print(f"成功: {result.success}")
        print(f"数据类型: {result.data_type}")
        print(f"采集数量: {result.count}")
        print(f"消息: {result.message}")

    except KeyboardInterrupt:
        print("\n\n🛑 用户停止采集")
        collector.stop()


def cmd_check_market(args):
    """检查市场状态命令"""
    print("\n🔍 市场状态检查")
    print("-" * 70)

    now = datetime.now()
    print(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # 检查是否在交易时段
    if is_trading_time():
        print("✅ 当前在交易时段")
        print("⚠️  注意: 盘中不能采集当日K线数据")
    else:
        print("⏸️  当前不在交易时段")
        print("✅ 可以采集历史数据")

    # 检查是否可以采集当日数据
    try:
        enforce_market_closed()
        print("✅ 可以采集当日数据（已收盘）")
    except SystemExit:
        print("❌ 不能采集当日数据（未收盘）")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='xcnstock 数据采集系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 每日历史数据采集
  python scripts/collect.py historical --mode daily

  # 实时数据采集
  python scripts/collect.py realtime --types quotes,limitup

  # 盘中数据采集
  python scripts/collect.py intraday --codes 000001 --duration 30

  # 检查市场状态
  python scripts/collect.py check-market
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='采集命令')

    # 历史数据采集
    hist_parser = subparsers.add_parser('historical', help='历史数据采集')
    hist_parser.add_argument(
        '--mode',
        choices=['daily', 'full', 'incremental'],
        default='daily',
        help='采集模式'
    )
    hist_parser.add_argument('--date', help='目标日期 YYYY-MM-DD')
    hist_parser.add_argument('--codes', help='股票代码，逗号分隔')
    hist_parser.add_argument(
        '--skip-check',
        action='store_true',
        help='跳过市场状态检查'
    )

    # 实时数据采集
    rt_parser = subparsers.add_parser('realtime', help='实时数据采集')
    rt_parser.add_argument(
        '--types',
        default='quotes',
        help='数据类型，逗号分隔: quotes,limitup,fundflow'
    )
    rt_parser.add_argument(
        '--schedule',
        action='store_true',
        help='定时模式'
    )
    rt_parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='定时间隔（分钟）'
    )

    # 盘中数据采集
    intra_parser = subparsers.add_parser('intraday', help='盘中数据采集')
    intra_parser.add_argument(
        '--codes',
        help='股票代码，逗号分隔'
    )
    intra_parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='运行时长（分钟）'
    )

    # 检查市场状态
    subparsers.add_parser('check-market', help='检查市场状态')

    # 解析参数
    args = parser.parse_args()

    # 打印横幅
    print_banner()

    # 如果没有命令，打印帮助
    if not args.command:
        parser.print_help()
        print_usage()
        return

    # 执行命令
    if args.command == 'historical':
        await cmd_historical(args)
    elif args.command == 'realtime':
        await cmd_realtime(args)
    elif args.command == 'intraday':
        await cmd_intraday(args)
    elif args.command == 'check-market':
        cmd_check_market(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
