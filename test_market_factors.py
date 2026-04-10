import sys
sys.path.insert(0, '.')
from core.factor_engine import FactorEngine

engine = FactorEngine()
factors = engine.list_factors()

print('当前因子列表:')
for f in factors:
    print(f"  {f['name']:20} {f['category']:15} {f['weight']:>5.2f} {'启用' if f['enabled'] else '禁用'}")

print('\n市场因子:')
market_factors = engine.list_factors(category='market')
for f in market_factors:
    print(f"  {f['name']:20} {f['category']:15} {f['weight']:>5.2f} {'启用' if f['enabled'] else '禁用'}")
