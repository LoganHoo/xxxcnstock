"""
测试因子组合优化系统
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from optimization.genetic_optimizer import GeneticOptimizer, Chromosome


def test_chromosome():
    """测试染色体类"""
    print("=" * 60)
    print("测试染色体类")
    print("=" * 60)
    
    chromosome = Chromosome(
        factors=["rsi", "macd", "kdj"],
        factor_weights={"rsi": 0.4, "macd": 0.35, "kdj": 0.25},
        factor_params={
            "rsi": {"period": 14},
            "macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9}
        },
        filters=["st_filter", "market_cap_filter"],
        holding_days=5,
        position_size=5
    )
    
    print(f"因子: {chromosome.factors}")
    print(f"权重: {chromosome.factor_weights}")
    print(f"参数: {chromosome.factor_params}")
    print(f"过滤器: {chromosome.filters}")
    print(f"持仓天数: {chromosome.holding_days}")
    print(f"持仓数量: {chromosome.position_size}")
    
    data = chromosome.to_dict()
    print(f"\n转换为字典: {data}")
    
    new_chromosome = Chromosome.from_dict(data)
    print(f"\n从字典创建: {new_chromosome.factors}")
    
    print("✓ 染色体类测试通过\n")


def test_genetic_optimizer():
    """测试遗传算法优化器"""
    print("=" * 60)
    print("测试遗传算法优化器")
    print("=" * 60)
    
    available_factors = ["rsi", "macd", "kdj", "bollinger", "atr", "cci", "wr", "dmi"]
    available_filters = ["st_filter", "market_cap_filter", "turnover_filter", "pe_filter"]
    
    factor_param_ranges = {
        "rsi": {"period": [7, 14, 21]},
        "macd": {"fast_period": [8, 10, 12], "slow_period": [20, 24, 26], "signal_period": [7, 9]},
        "kdj": {"n": [5, 9, 14], "m1": [2, 3], "m2": [2, 3]},
        "bollinger": {"period": [10, 20], "std_dev": [1.5, 2.0]},
        "atr": {"period": [10, 14]},
        "cci": {"period": [10, 14, 20]},
        "wr": {"period": [10, 14]},
        "dmi": {"period": [10, 14]}
    }
    
    optimizer = GeneticOptimizer(
        available_factors=available_factors,
        available_filters=available_filters,
        factor_param_ranges=factor_param_ranges,
        population_size=10,
        generations=3,
        crossover_rate=0.8,
        mutation_rate=0.3,
        elite_size=2,
        min_factors=2,
        max_factors=5,
        min_filters=1,
        max_filters=3
    )
    
    print(f"可用因子: {available_factors}")
    print(f"可用过滤器: {available_filters}")
    print(f"种群大小: 10")
    print(f"迭代代数: 3")
    
    def mock_backtest(chromosome):
        import random
        return {
            "annual_return": random.uniform(-0.1, 0.3),
            "sharpe_ratio": random.uniform(-0.5, 2.0),
            "max_drawdown": random.uniform(0.1, 0.5),
            "win_rate": random.uniform(0.3, 0.7)
        }
    
    best = optimizer.evolve(mock_backtest)
    
    print("\n最优结果:")
    print(f"  因子: {best.factors}")
    print(f"  权重: {best.factor_weights}")
    print(f"  参数: {best.factor_params}")
    print(f"  过滤器: {best.filters}")
    print(f"  持仓天数: {best.holding_days}")
    print(f"  持仓数量: {best.position_size}")
    print(f"  适应度: {best.fitness:.4f}")
    
    history = optimizer.get_history()
    print(f"\n进化历史 ({len(history)} 代):")
    for h in history:
        print(f"  第 {h['generation']} 代: 最优={h['best_fitness']:.4f}, 平均={h['avg_fitness']:.4f}")
    
    print("\n✓ 遗传算法优化器测试通过\n")


def test_crossover_and_mutation():
    """测试交叉和变异操作"""
    print("=" * 60)
    print("测试交叉和变异操作")
    print("=" * 60)
    
    available_factors = ["rsi", "macd", "kdj", "bollinger", "atr"]
    available_filters = ["st_filter", "market_cap_filter", "turnover_filter"]
    
    factor_param_ranges = {
        "rsi": {"period": [7, 14, 21]},
        "macd": {"fast_period": [12], "slow_period": [26], "signal_period": [9]}
    }
    
    optimizer = GeneticOptimizer(
        available_factors=available_factors,
        available_filters=available_filters,
        factor_param_ranges=factor_param_ranges,
        population_size=10,
        generations=1
    )
    
    parent1 = Chromosome(
        factors=["rsi", "macd", "kdj"],
        factor_weights={"rsi": 0.5, "macd": 0.3, "kdj": 0.2},
        factor_params={"rsi": {"period": 14}, "macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9}},
        filters=["st_filter", "market_cap_filter"],
        holding_days=5,
        position_size=5
    )
    
    parent2 = Chromosome(
        factors=["macd", "bollinger", "atr"],
        factor_weights={"macd": 0.4, "bollinger": 0.35, "atr": 0.25},
        factor_params={"macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9}},
        filters=["market_cap_filter", "turnover_filter"],
        holding_days=7,
        position_size=3
    )
    
    print("父代1:")
    print(f"  因子: {parent1.factors}, 权重: {parent1.factor_weights}")
    print("父代2:")
    print(f"  因子: {parent2.factors}, 权重: {parent2.factor_weights}")
    
    child1, child2 = optimizer.crossover(parent1, parent2)
    
    print("\n子代1:")
    print(f"  因子: {child1.factors}, 权重: {child1.factor_weights}")
    print("子代2:")
    print(f"  因子: {child2.factors}, 权重: {child2.factor_weights}")
    
    mutated = optimizer.mutate(child1)
    print("\n变异后:")
    print(f"  因子: {mutated.factors}, 权重: {mutated.factor_weights}")
    
    print("\n✓ 交叉和变异操作测试通过\n")


def main():
    """主测试函数"""
    print("\n" + "=" * 60)
    print("因子组合优化系统测试")
    print("=" * 60 + "\n")
    
    test_chromosome()
    test_crossover_and_mutation()
    test_genetic_optimizer()
    
    print("=" * 60)
    print("所有测试通过!")
    print("=" * 60)


if __name__ == "__main__":
    main()
