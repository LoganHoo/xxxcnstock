#!/usr/bin/env python
"""
运行因子组合优化
使用遗传算法搜索最优因子组合和参数
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from optimization.factor_combination_optimizer import FactorCombinationOptimizer
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="因子组合优化")
    parser.add_argument("--data-dir", type=str, default="data", help="数据目录")
    parser.add_argument("--population", type=int, default=30, help="种群大小")
    parser.add_argument("--generations", type=int, default=20, help="迭代代数")
    parser.add_argument("--output-dir", type=str, default="optimization/results", help="输出目录")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("因子组合优化系统")
    logger.info("=" * 60)
    logger.info(f"数据目录: {args.data_dir}")
    logger.info(f"种群大小: {args.population}")
    logger.info(f"迭代代数: {args.generations}")
    logger.info(f"输出目录: {args.output_dir}")
    logger.info("=" * 60)
    
    optimizer = FactorCombinationOptimizer(data_dir=args.data_dir)
    
    best = optimizer.run_optimization(
        population_size=args.population,
        generations=args.generations,
        output_dir=args.output_dir
    )
    
    print("\n" + "=" * 60)
    print("优化完成 - 最优策略配置")
    print("=" * 60)
    print(f"\n选中的因子 ({len(best.factors)} 个):")
    for factor in best.factors:
        weight = best.factor_weights.get(factor, 0)
        params = best.factor_params.get(factor, {})
        params_str = ", ".join([f"{k}={v}" for k, v in params.items()]) if params else "默认"
        print(f"  - {factor}: 权重={weight:.4f}, 参数={params_str}")
    
    print(f"\n选中的过滤器 ({len(best.filters)} 个):")
    for f in best.filters:
        print(f"  - {f}")
    
    print(f"\n执行参数:")
    print(f"  - 持仓天数: {best.holding_days}")
    print(f"  - 持仓数量: {best.position_size}")
    
    print(f"\n适应度: {best.fitness:.4f}")
    
    print("\n" + "=" * 60)
    print(f"结果已保存到: {args.output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
