"""
测试因子配置加载器
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.factor_config_loader import FactorConfigLoader


def main():
    print("=" * 60)
    print("因子配置加载器测试")
    print("=" * 60)
    
    loader = FactorConfigLoader()
    
    print("\n1. 测试加载单个因子配置")
    print("-" * 60)
    
    macd_config = loader.load_factor_config("macd", "technical")
    print(f"MACD配置: {macd_config.get('factor', {}).get('description')}")
    
    print("\n2. 测试获取参数预设")
    print("-" * 60)
    
    for preset in ["default", "conservative", "aggressive", "standard"]:
        params = loader.get_params("macd", preset, "technical")
        print(f"MACD {preset} 参数: {params}")
    
    print("\n3. 测试获取评分配置")
    print("-" * 60)
    
    scoring = loader.get_scoring("rsi", "technical")
    print(f"RSI权重: {scoring.get('weight')}")
    print(f"RSI阈值: {scoring.get('threshold')}")
    print(f"RSI评分规则:")
    for rule in scoring.get("rules", []):
        print(f"  - {rule['condition']}: {rule['score']}分")
    
    print("\n4. 测试获取优化参数")
    print("-" * 60)
    
    opt = loader.get_optimization_params("kdj", "technical")
    print(f"KDJ优化参数范围: {opt.get('param_ranges')}")
    
    print("\n5. 测试获取因子完整信息")
    print("-" * 60)
    
    info = loader.get_factor_info("bollinger", "technical")
    print(f"因子名称: {info['name']}")
    print(f"因子类别: {info['category']}")
    print(f"因子描述: {info['description']}")
    print(f"可用预设: {info['presets']}")
    print(f"权重: {info['weight']}")
    print(f"阈值: {info['threshold']}")
    
    print("\n6. 加载所有因子配置")
    print("-" * 60)
    
    all_configs = loader.load_all_factors()
    print(f"共加载 {len(all_configs)} 个因子配置")
    
    for key in sorted(all_configs.keys()):
        config = all_configs[key]
        desc = config.get("factor", {}).get("description", "")
        print(f"  - {key}: {desc}")
    
    print("\n7. 测试量价因子")
    print("-" * 60)
    
    vma_params = loader.get_params("vma", "default", "volume_price")
    print(f"VMA默认参数: {vma_params}")
    
    mfi_presets = loader.list_available_presets("mfi", "volume_price")
    print(f"MFI可用预设: {mfi_presets}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
