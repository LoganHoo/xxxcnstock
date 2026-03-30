"""
冠军策略一键流水线
流程：
  Step 1: 运行遗传算法优化 → 找到冠军因子组合
  Step 2: 生成 champion.yaml → 生产级策略配置
  Step 3: 用冠军策略运行选股 → 选出目标股票
  Step 4: 更新K线数据 → 获取最新股价
  Step 5: 输出最终报告 → 含今日表现
"""
import sys
import os
import argparse
import logging
import subprocess
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent


def run_step(name: str, cmd: list, cwd: Path) -> bool:
    print(f"\n{'=' * 60}")
    print(f"  {name}")
    print(f"{'=' * 60}")

    start = datetime.now()
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=False,
        text=True
    )
    elapsed = (datetime.now() - start).total_seconds()

    if result.returncode != 0:
        print(f"\n  {name} 失败 (耗时 {elapsed:.1f}s)")
        return False

    print(f"\n  {name} 完成 (耗时 {elapsed:.1f}s)")
    return True


def run_step1_optimization(args) -> bool:
    cmd = [
        sys.executable,
        "scripts/run_optimization.py",
        "--data-dir", args.data_dir,
        "--population", str(args.population),
        "--generations", str(args.generations),
        "--output-dir", args.output_dir
    ]
    return run_step("Step 1: 因子组合优化", cmd, project_root)


def run_step2_strategy_selection(args) -> bool:
    champion_yaml = project_root / "config" / "strategies" / "champion.yaml"
    if not champion_yaml.exists():
        print(f"  错误: 冠军策略文件不存在: {champion_yaml}")
        return False

    cmd = [
        sys.executable,
        "scripts/run_strategy.py",
        "--strategy", str(champion_yaml.relative_to(project_root)),
        "--output", args.strategy_output,
        "--top-n", str(args.top_n)
    ]
    return run_step("Step 2: 冠军策略选股", cmd, project_root)


def run_step3_update_kline(args) -> bool:
    strategy_output = project_root / args.strategy_output
    if not strategy_output.exists():
        print(f"  跳过: 选股结果不存在: {strategy_output}")
        return True

    cmd = [
        sys.executable,
        "scripts/update_kline_today.py",
        "--result-file", args.strategy_output,
        "--output", args.updated_output,
        "--delay", str(args.delay),
        "--target-codes-only"
    ]
    return run_step("Step 3: 更新K线 & 刷新选股结果", cmd, project_root)


def main():
    parser = argparse.ArgumentParser(description="冠军策略一键流水线")

    parser.add_argument("--population", type=int, default=50, help="遗传算法种群大小")
    parser.add_argument("--generations", type=int, default=30, help="遗传算法迭代代数")
    parser.add_argument("--data-dir", type=str, default="data", help="数据目录")
    parser.add_argument("--output-dir", type=str, default="optimization/results", help="优化结果目录")
    parser.add_argument("--strategy-output", type=str, default="reports/champion_strategy_result.json", help="选股结果输出")
    parser.add_argument("--updated-output", type=str, default="reports/champion_strategy_result_updated.json", help="更新后的选股结果")
    parser.add_argument("--top-n", type=int, default=20, help="选股数量")
    parser.add_argument("--delay", type=float, default=0.15, help="API请求间隔(秒)")
    parser.add_argument("--skip-optimization", action="store_true", help="跳过优化，直接用已有champion.yaml选股")
    parser.add_argument("--skip-update", action="store_true", help="跳过K线更新")

    args = parser.parse_args()

    total_start = datetime.now()

    print("=" * 60)
    print("  冠军策略一键流水线")
    print(f"  时间: {total_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"\n  配置:")
    print(f"    种群大小: {args.population}")
    print(f"    迭代代数: {args.generations}")
    print(f"    选股数量: {args.top_n}")
    print(f"    跳过优化: {args.skip_optimization}")
    print(f"    跳过更新: {args.skip_update}")

    success = True

    if not args.skip_optimization:
        if not run_step1_optimization(args):
            logger.error("Step 1 优化失败，终止流水线")
            success = False
    else:
        print("\n  跳过 Step 1 (优化)")

    if success:
        if not run_step2_strategy_selection(args):
            logger.error("Step 2 选股失败")
            success = False

    if success and not args.skip_update:
        if not run_step3_update_kline(args):
            logger.error("Step 3 更新失败")
            success = False
    elif args.skip_update:
        print("\n  跳过 Step 3 (更新)")

    total_elapsed = (datetime.now() - total_start).total_seconds()

    print(f"\n{'=' * 60}")
    if success:
        print(f"  流水线全部完成!")
        print(f"  总耗时: {total_elapsed:.1f}s ({total_elapsed / 60:.1f}min)")
        print(f"\n  输出文件:")
        print(f"    冠军策略: config/strategies/champion.yaml")
        print(f"    选股结果: {args.strategy_output}")
        if not args.skip_update:
            print(f"    更新结果: {args.updated_output}")
        print(f"    优化报告: {args.output_dir}/")
    else:
        print(f"  流水线执行失败")
    print(f"{'=' * 60}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
