"""多流程选股对比主脚本

同时运行三种选股流程:
- 流程A: 过滤优先 (conservative)
- 流程B: 因子优先 (balanced_factor)
- 流程C: 信号优先 (aggressive_signal)

使用方法:
    python scripts/pipeline/stock_pick_multi.py
    python scripts/pipeline/stock_pick_multi.py --compare  # 运行对比分析
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import argparse
import logging
from pathlib import Path
from datetime import datetime

from flows.conservative_flow import ConservativeFlow
from flows.balanced_factor_flow import BalancedFactorFlow
from flows.aggressive_signal_flow import AggressiveSignalFlow
from flows.flow_comparator import FlowComparator


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def run_all_flows(kline_dir: str, scores_path: str = None) -> dict:
    """运行所有选股流程"""
    flows = {
        "conservative": ConservativeFlow(),
        "balanced_factor": BalancedFactorFlow(),
        "aggressive_signal": AggressiveSignalFlow()
    }

    results = {}
    logger = logging.getLogger(__name__)

    for name, flow in flows.items():
        logger.info(f"=" * 50)
        logger.info(f"开始运行流程: {name}")
        logger.info(f"=" * 50)

        try:
            result = flow.select(kline_dir, scores_path)
            results[name] = result
            logger.info(f"流程 {name} 完成: 选出 {len(result.stocks)} 只股票")
        except Exception as e:
            logger.error(f"流程 {name} 执行失败: {e}")
            results[name] = None

    return results


def save_results(results: dict, output_dir: str = "data/picks"):
    """保存所有流程结果"""
    comparator = FlowComparator(output_dir)

    for name, result in results.items():
        if result is not None:
            comparator.save_result(result)


def run_comparison(kline_dir: str, days: int = 30) -> str:
    """运行流程对比分析"""
    comparator = FlowComparator()

    performances = comparator.compare_flows(kline_dir, days=days)

    report = comparator.generate_report(performances)

    report_path = comparator.save_report(report)

    return report, report_path


def print_summary(results: dict):
    """打印选股结果汇总"""
    print("\n" + "=" * 60)
    print("多流程选股结果汇总")
    print("=" * 60)

    for name, result in results.items():
        if result is None:
            print(f"\n{name}: 执行失败")
            continue

        print(f"\n【{result.flow_name}】选出 {len(result.stocks)} 只")
        if result.stocks:
            print("  代码    等级  评分  信号")
            print("  " + "-" * 40)
            for stock in result.stocks[:10]:
                signals = ",".join([k for k, v in stock.signals.items() if v]) if stock.signals else "-"
                print(f"  {stock.code}  {stock.grade}  {stock.score:.1f}  {signals}")
            if len(result.stocks) > 10:
                print(f"  ... 还有 {len(result.stocks) - 10} 只")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="多流程选股对比系统")
    parser.add_argument("--kline-dir", type=str, default="data/kline",
                        help="K线数据目录")
    parser.add_argument("--scores", type=str, default=None,
                        help="预计算评分文件路径")
    parser.add_argument("--compare", action="store_true",
                        help="运行对比分析")
    parser.add_argument("--days", type=int, default=30,
                        help="对比分析天数")

    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    project_root = Path(__file__).parent.parent.parent
    kline_dir = str(project_root / args.kline_dir)
    scores_path = str(project_root / args.scores) if args.scores else None

    if not Path(kline_dir).exists():
        logger.error(f"K线目录不存在: {kline_dir}")
        return

    if args.compare:
        logger.info("运行流程对比分析...")
        report, report_path = run_comparison(kline_dir, days=args.days)
        print("\n" + report)
        logger.info(f"报告已保存: {report_path}")
    else:
        logger.info("开始多流程选股...")
        results = run_all_flows(kline_dir, scores_path)

        save_results(results)

        print_summary(results)

        logger.info("\n选股完成！可使用 --compare 参数运行对比分析")


if __name__ == "__main__":
    main()
