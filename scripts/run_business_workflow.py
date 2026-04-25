#!/usr/bin/env python3
"""
业务工作流运行脚本

统一入口运行所有业务工作流:
- 数据采集工作流
- 选股策略工作流
- 回测验证工作流
- 日常运营工作流

使用方法:
    python scripts/run_business_workflow.py <workflow_type> [options]

示例:
    # 运行数据采集
    python scripts/run_business_workflow.py data_collection --type all

    # 运行选股策略
    python scripts/run_business_workflow.py stock_selection --strategy comprehensive

    # 运行回测
    python scripts/run_business_workflow.py backtest --start-date 2023-01-01 --end-date 2023-12-31

    # 运行日常运营
    python scripts/run_business_workflow.py daily_operation --tasks all
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
from datetime import datetime

from core.logger import setup_logger
from workflows.workflow_runner import WorkflowRunner, WorkflowType


def print_header(title: str):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_footer():
    """打印页脚"""
    print("=" * 70)
    print()


def run_data_collection(args):
    """运行数据采集工作流"""
    print_header("数据采集工作流")

    runner = WorkflowRunner()

    params = {
        'type': args.type,
        'date': args.date,
        'codes': args.codes.split(',') if args.codes else None,
        'validate': not args.no_validate
    }

    print(f"参数:")
    print(f"  采集类型: {args.type}")
    print(f"  日期: {args.date or '今天'}")
    print(f"  股票代码: {args.codes or '全部'}")
    print(f"  验证: {'否' if args.no_validate else '是'}")
    print()

    try:
        result = runner.run(WorkflowType.DATA_COLLECTION, params)

        print("✅ 数据采集工作流完成")
        print()

        # 显示结果摘要
        if 'results' in result:
            for collection_type, collection_result in result['results'].items():
                status = collection_result.get('status', 'unknown')
                status_icon = "✅" if status == "success" else "⚠️" if status == "partial" else "❌"
                print(f"  {status_icon} {collection_type}: {status}")
                print(f"     采集记录: {collection_result.get('records_collected', 0)}")
                print(f"     更新记录: {collection_result.get('records_updated', 0)}")
                print(f"     失败记录: {collection_result.get('records_failed', 0)}")
                print(f"     耗时: {collection_result.get('duration_seconds', 0):.2f}秒")
                print()

        return True

    except Exception as e:
        print(f"❌ 数据采集工作流失败: {e}")
        return False


def run_stock_selection(args):
    """运行选股策略工作流"""
    print_header("选股策略工作流")

    runner = WorkflowRunner()

    params = {
        'strategy': args.strategy,
        'top_n': args.top_n,
        'date': args.date,
        'universe': args.codes.split(',') if args.codes else None
    }

    print(f"参数:")
    print(f"  策略类型: {args.strategy}")
    print(f"  选股数量: {args.top_n}")
    print(f"  日期: {args.date or '今天'}")
    print(f"  股票池: {args.codes or '全市场'}")
    print()

    try:
        result = runner.run(WorkflowType.STOCK_SELECTION, params)

        if 'result' in result:
            selection_result = result['result']
            status = selection_result.get('status', 'unknown')
            status_icon = "✅" if status == "success" else "❌"

            print(f"{status_icon} 选股策略工作流完成")
            print()
            print(f"  状态: {status}")
            print(f"  总股票数: {selection_result.get('total_stocks', 0)}")
            print(f"  选中股票数: {selection_result.get('selected_stocks', 0)}")
            print(f"  耗时: {selection_result.get('duration_seconds', 0):.2f}秒")
            print()

            # 显示Top股票
            top_stocks = selection_result.get('top_stocks', [])
            if top_stocks:
                print(f"  Top {min(10, len(top_stocks))} 股票:")
                print(f"  {'排名':<6}{'代码':<10}{'名称':<12}{'评分':<10}")
                print(f"  {'-' * 40}")
                for stock in top_stocks[:10]:
                    print(f"  {stock.get('rank', 0):<6}{stock.get('code', ''):<10}"
                          f"{stock.get('name', ''):<12}{stock.get('score', 0):<10.2f}")
                print()

        return True

    except Exception as e:
        print(f"❌ 选股策略工作流失败: {e}")
        return False


def run_backtest(args):
    """运行回测验证工作流"""
    print_header("回测验证工作流")

    runner = WorkflowRunner()

    params = {
        'strategy': args.strategy,
        'start_date': args.start_date,
        'end_date': args.end_date,
        'initial_capital': args.initial_capital,
        'rebalance': args.rebalance,
        'top_n': args.top_n
    }

    print(f"参数:")
    print(f"  策略类型: {args.strategy}")
    print(f"  回测区间: {args.start_date} 至 {args.end_date}")
    print(f"  初始资金: {args.initial_capital:,.0f}")
    print(f"  调仓频率: {args.rebalance}")
    print(f"  选股数量: {args.top_n}")
    print()

    try:
        result = runner.run(WorkflowType.BACKTEST, params)

        if 'result' in result:
            backtest_result = result['result']
            status = backtest_result.get('status', 'unknown')
            status_icon = "✅" if status == "success" else "❌"

            print(f"{status_icon} 回测验证工作流完成")
            print()

            # 显示绩效指标
            performance = backtest_result.get('performance', {})
            print("  📈 绩效指标:")
            print(f"     总收益率: {performance.get('total_return', 0):+.2%}")
            print(f"     年化收益率: {performance.get('annualized_return', 0):+.2%}")
            print(f"     最大回撤: {performance.get('max_drawdown', 0):.2%}")
            print(f"     夏普比率: {performance.get('sharpe_ratio', 0):.2f}")
            print(f"     波动率: {performance.get('volatility', 0):.2%}")
            print(f"     胜率: {performance.get('win_rate', 0):.2%}")
            print()

            # 显示交易统计
            trading_stats = backtest_result.get('trading_stats', {})
            print("  💼 交易统计:")
            print(f"     总交易次数: {trading_stats.get('total_trades', 0)}")
            print(f"     盈利交易: {trading_stats.get('winning_trades', 0)}")
            print(f"     亏损交易: {trading_stats.get('losing_trades', 0)}")
            print()

        return True

    except Exception as e:
        print(f"❌ 回测验证工作流失败: {e}")
        return False


def run_daily_operation(args):
    """运行日常运营工作流"""
    print_header("日常运营工作流")

    runner = WorkflowRunner()

    params = {
        'tasks': args.tasks,
        'date': args.date,
        'skip_market_check': args.skip_market_check
    }

    print(f"参数:")
    print(f"  任务: {', '.join(args.tasks)}")
    print(f"  日期: {args.date or '今天'}")
    print(f"  跳过市场检查: {'是' if args.skip_market_check else '否'}")
    print()

    try:
        result = runner.run(WorkflowType.DAILY_OPERATION, params)

        if 'result' in result:
            operation_result = result['result']
            status = operation_result.get('overall_status', 'unknown')
            status_icon = "✅" if status == "success" else "⚠️" if status == "partial" else "❌"

            print(f"{status_icon} 日常运营工作流完成")
            print()

            summary = operation_result.get('summary', {})
            print(f"  总体状态: {status}")
            print(f"  任务统计: 完成 {summary.get('tasks_completed', 0)}, "
                  f"失败 {summary.get('tasks_failed', 0)}, "
                  f"跳过 {summary.get('tasks_skipped', 0)}")
            print(f"  数据质量评分: {summary.get('data_quality_score', 0):.1f}")
            print(f"  系统健康: {summary.get('system_health', 'unknown')}")
            print()

            # 显示各任务状态
            results = operation_result.get('results', {})
            if results:
                print("  📋 任务详情:")
                for task_name, task_result in results.items():
                    task_status = task_result.get('status', 'unknown')
                    task_icon = "✅" if task_status == "success" else "⚠️" if task_status == "partial" else "❌"
                    print(f"     {task_icon} {task_name}: {task_status}")
                print()

        return True

    except Exception as e:
        print(f"❌ 日常运营工作流失败: {e}")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='业务工作流运行脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 数据采集
    python scripts/run_business_workflow.py data_collection --type all

    # 选股策略
    python scripts/run_business_workflow.py stock_selection --strategy comprehensive --top-n 50

    # 回测验证
    python scripts/run_business_workflow.py backtest --start-date 2023-01-01 --end-date 2023-12-31

    # 日常运营
    python scripts/run_business_workflow.py daily_operation --tasks data_update quality_check
        """
    )

    subparsers = parser.add_subparsers(dest='workflow', help='工作流类型')

    # 数据采集子命令
    collection_parser = subparsers.add_parser('data_collection', help='数据采集工作流')
    collection_parser.add_argument('--type', choices=['financial', 'market_behavior', 'announcement', 'all'],
                                  default='all', help='采集类型')
    collection_parser.add_argument('--date', help='指定日期 (YYYY-MM-DD)')
    collection_parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    collection_parser.add_argument('--no-validate', action='store_true', help='跳过验证')

    # 选股策略子命令
    selection_parser = subparsers.add_parser('stock_selection', help='选股策略工作流')
    selection_parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                                 default='comprehensive', help='策略类型')
    selection_parser.add_argument('--top-n', type=int, default=50, help='输出数量')
    selection_parser.add_argument('--date', help='选股日期 (YYYY-MM-DD)')
    selection_parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')

    # 回测验证子命令
    backtest_parser = subparsers.add_parser('backtest', help='回测验证工作流')
    backtest_parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                                default='comprehensive', help='策略类型')
    backtest_parser.add_argument('--start-date', required=True, help='开始日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--end-date', required=True, help='结束日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--initial-capital', type=float, default=1000000, help='初始资金')
    backtest_parser.add_argument('--rebalance', choices=['daily', 'weekly', 'monthly', 'quarterly'],
                                default='weekly', help='调仓频率')
    backtest_parser.add_argument('--top-n', type=int, default=20, help='选股数量')

    # 日常运营子命令
    operation_parser = subparsers.add_parser('daily_operation', help='日常运营工作流')
    operation_parser.add_argument('--tasks', nargs='+',
                                 choices=['data_update', 'quality_check', 'health_check', 'audit_report', 'cleanup', 'all'],
                                 default=['all'], help='要执行的任务')
    operation_parser.add_argument('--date', help='运营日期 (YYYY-MM-DD)')
    operation_parser.add_argument('--skip-market-check', action='store_true', help='跳过市场状态检查')

    args = parser.parse_args()

    if not args.workflow:
        parser.print_help()
        return 1

    # 设置日志
    logger = setup_logger("run_business_workflow")
    logger.info(f"启动业务工作流: {args.workflow}")

    # 运行对应工作流
    success = False

    if args.workflow == 'data_collection':
        success = run_data_collection(args)
    elif args.workflow == 'stock_selection':
        success = run_stock_selection(args)
    elif args.workflow == 'backtest':
        success = run_backtest(args)
    elif args.workflow == 'daily_operation':
        success = run_daily_operation(args)

    print_footer()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
