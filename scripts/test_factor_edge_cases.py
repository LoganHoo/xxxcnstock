"""
测试所有因子的除零保护
"""
import sys
from pathlib import Path
import polars as pl

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.factor_library import FactorRegistry

import factors.technical
import factors.technical.ma_trend
import factors.technical.macd
import factors.technical.rsi
import factors.technical.kdj
import factors.technical.bollinger
import factors.technical.atr
import factors.technical.cci
import factors.technical.wr
import factors.technical.dmi
import factors.technical.roc
import factors.technical.mtm
import factors.technical.asi
import factors.technical.emv
import factors.technical.psy

import factors.volume_price
import factors.volume_price.volume_ratio
import factors.volume_price.turnover
import factors.volume_price.obv
import factors.volume_price.vr
import factors.volume_price.mfi
import factors.volume_price.vma
import factors.volume_price.vosc
import factors.volume_price.wvad


def create_edge_case_data():
    """创建边界测试数据"""
    data = pl.DataFrame({
        "code": ["TEST"] * 30,
        "trade_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
        "open": [10.0] * 30,
        "high": [10.0] * 30,
        "low": [10.0] * 30,
        "close": [10.0] * 30,
        "volume": [1000000.0] * 30,
    })
    return data


def test_factor(factor_name: str, category: str):
    """测试单个因子"""
    try:
        factor_class = FactorRegistry.get(factor_name)
        factor = factor_class(name=factor_name, category=category)
        
        data = create_edge_case_data()
        result = factor.calculate(data)
        
        factor_col = f"factor_{factor_name}"
        if factor_col in result.columns:
            score = result[factor_col].tail(1).item()
            return True, score
        return True, None
    except Exception as e:
        return False, str(e)


def main():
    print("=" * 60)
    print("因子除零保护测试")
    print("=" * 60)
    
    technical_factors = [
        "ma_trend", "macd", "rsi", "kdj", "bollinger", "atr",
        "cci", "wr", "dmi", "roc", "mtm", "asi", "emv", "psy"
    ]
    
    volume_price_factors = [
        "volume_ratio", "turnover", "obv", "vr", "mfi", "vma", "vosc", "wvad"
    ]
    
    print("\n测试技术指标因子 (边界数据: 价格恒定)")
    print("-" * 60)
    
    passed = 0
    failed = 0
    
    for factor_name in technical_factors:
        success, result = test_factor(factor_name, "technical")
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {factor_name:15} {status}  score={result}")
        if success:
            passed += 1
        else:
            failed += 1
    
    print("\n测试量价因子 (边界数据: 成交量恒定)")
    print("-" * 60)
    
    for factor_name in volume_price_factors:
        success, result = test_factor(factor_name, "volume_price")
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {factor_name:15} {status}  score={result}")
        if success:
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"测试结果: 通过 {passed}/{passed + failed}")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
