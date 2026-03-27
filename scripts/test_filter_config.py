"""
测试过滤器配置加载器
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from core.filter_config_loader import filter_config_loader


def test_filter_config_loader():
    """测试过滤器配置加载"""
    print("=" * 60)
    print("过滤器配置加载器测试")
    print("=" * 60)
    
    print("\n【测试1: 加载所有过滤器配置】")
    all_configs = filter_config_loader.load_all_filters()
    print(f"共加载 {len(all_configs)} 个过滤器配置")
    
    for cache_key in sorted(all_configs.keys()):
        config = all_configs[cache_key]
        filter_config = config.get("filter", {})
        print(f"  - {cache_key}: {filter_config.get('description', 'N/A')}")
    
    print("\n【测试2: 按类别加载过滤器】")
    categories = ["stock", "market", "fundamental", "technical", "liquidity", "valuation", "pattern"]
    
    for category in categories:
        configs = filter_config_loader.load_category_filters(category)
        print(f"\n{category} 类别 ({len(configs)} 个):")
        for name, config in configs.items():
            filter_config = config.get("filter", {})
            print(f"  - {name}: {filter_config.get('description', 'N/A')}")
    
    print("\n【测试3: 获取单个过滤器参数】")
    test_filters = ["st_filter", "market_cap_filter", "turnover_rate_filter"]
    
    for filter_name in test_filters:
        print(f"\n{filter_name}:")
        
        params = filter_config_loader.get_params(filter_name, "default")
        print(f"  默认参数: {params}")
        
        presets = filter_config_loader.list_available_presets(filter_name)
        print(f"  可用预设: {presets}")
        
        for preset in presets:
            if preset != "default":
                preset_params = filter_config_loader.get_params(filter_name, preset)
                print(f"  {preset} 参数: {preset_params}")
    
    print("\n【测试4: 获取过滤器完整信息】")
    test_filters = ["st_filter", "valuation_filter", "limit_up_trap_filter"]
    
    for filter_name in test_filters:
        info = filter_config_loader.get_filter_info(filter_name)
        print(f"\n{filter_name}:")
        print(f"  名称: {info['name']}")
        print(f"  类别: {info['category']}")
        print(f"  描述: {info['description']}")
        print(f"  启用: {info['enabled']}")
        print(f"  风险等级: {info['risk_level']}")
        print(f"  参数: {info['params']}")
        print(f"  预设: {info['presets']}")
    
    print("\n【测试5: 应用预设配置】")
    filter_name = "market_cap_filter"
    
    for preset in ["default", "large_cap", "mid_cap", "small_cap"]:
        config = filter_config_loader.apply_preset(filter_name, preset)
        filter_config = config.get("filter", {})
        print(f"\n{filter_name} - {preset}:")
        print(f"  参数: {filter_config.get('params', {})}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_filter_config_loader()
