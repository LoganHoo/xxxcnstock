import sys
sys.path.insert(0, '.')
from optimization.factor_combination_optimizer import FactorCombinationOptimizer

opt = FactorCombinationOptimizer(max_stocks=10)
factors = opt.get_available_factors()

print('可用因子:')
for f in sorted(factors):
    print(f"  {f}")

print('\n市场因子是否存在:')
market_factors = ["market_trend", "market_breadth", "market_sentiment", "market_temperature"]
for f in market_factors:
    print(f"  {f}: {'存在' if f in factors else '不存在'}")

print('\n因子参数范围:')
params = opt.get_factor_param_ranges()
for factor, ranges in params.items():
    if factor in market_factors:
        print(f"  {factor}: {ranges}")
