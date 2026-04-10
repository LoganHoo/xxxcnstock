import sys
sys.path.insert(0, '.')
import polars as pl
from core.factor_engine import FactorEngine

# 加载一些测试数据
test_data = pl.DataFrame({
    "code": ["000001" for _ in range(10)],
    "trade_date": [
        "2026-03-20", "2026-03-21", "2026-03-22", "2026-03-23", "2026-03-24",
        "2026-03-25", "2026-03-26", "2026-03-27", "2026-03-28", "2026-03-29"
    ],
    "open": [3500, 3520, 3530, 3550, 3540, 3560, 3570, 3580, 3590, 3600],
    "close": [3510, 3530, 3540, 3560, 3550, 3570, 3580, 3590, 3600, 3610],
    "high": [3520, 3540, 3550, 3570, 3560, 3580, 3590, 3600, 3610, 3620],
    "low": [3490, 3510, 3520, 3540, 3530, 3550, 3560, 3570, 3580, 3590],
    "volume": [100000, 110000, 120000, 130000, 125000, 140000, 150000, 160000, 170000, 180000]
})

print("测试数据:")
print(test_data)

engine = FactorEngine()

# 测试市场因子
market_factors = ["market_trend", "market_breadth", "market_sentiment", "market_temperature"]

for factor_name in market_factors:
    print(f"\n计算因子: {factor_name}")
    try:
        result = engine.calculate_factor(test_data, factor_name)
        factor_column = f"factor_{factor_name}"
        print(result[["trade_date", factor_column]].sort("trade_date"))
    except Exception as e:
        print(f"  计算失败: {e}")
