#!/usr/bin/env python3
"""
测试脚本：验证过滤器在完整流程中的执行情况
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
import yaml
from datetime import date, timedelta

from filters.base_filter import FilterRegistry

def test_filter_execution():
    """测试过滤器执行流程"""
    print("=" * 60)
    print("测试过滤器执行流程")
    print("=" * 60)
    
    # 1. 加载配置
    filter_config_path = PROJECT_ROOT / "config/filters/fund_behavior_filters.yaml"
    with open(filter_config_path, 'r', encoding='utf-8') as f:
        filter_config = yaml.safe_load(f)
    
    filters_config = filter_config.get('filters', {})
    filter_order = filter_config.get('filter_order', list(filters_config.keys()))
    
    print(f"\n过滤器配置:")
    print(f"  配置文件: {filter_config_path}")
    print(f"  过滤器顺序: {filter_order}")
    
    # 2. 创建测试数据（包含000542）
    test_data = pl.DataFrame({
        "code": ["000542", "000001", "000002", "000003"],
        "name": ["中电电机", "平安银行", "万科A", "退市股"],
        "trade_date": ["2004-01-06", "2026-04-09", "2026-04-09", "2026-04-09"],
        "open": [10.0, 10.0, 10.0, 10.0],
        "close": [10.0, 10.0, 10.0, 10.0],
        "high": [10.0, 10.0, 10.0, 10.0],
        "low": [10.0, 10.0, 10.0, 10.0],
        "volume": [1000000, 1000000, 1000000, 1000000]
    })
    
    print(f"\n测试数据:")
    print(f"  原始数量: {len(test_data)}")
    print(test_data)
    
    # 3. 执行过滤器
    print(f"\n" + "-" * 60)
    print("执行过滤器")
    print("-" * 60)
    
    original_count = len(test_data)
    applied_filters = []
    
    for filter_name in filter_order:
        filter_params = filters_config.get(filter_name, {})
        if not filter_params.get('enabled', False):
            print(f"\n[跳过] {filter_name} (未启用)")
            continue
        
        filter_class = FilterRegistry.get(filter_name)
        if not filter_class:
            print(f"\n[跳过] {filter_name} (未注册)")
            continue
        
        try:
            filter_instance = filter_class(params=filter_params)
            if not filter_instance.is_enabled():
                print(f"\n[跳过] {filter_name} (is_enabled返回False)")
                continue
            
            print(f"\n[执行] {filter_name}")
            print(f"  参数: {filter_params}")
            
            before_count = len(test_data)
            test_data = filter_instance.filter(test_data)
            after_count = len(test_data)
            
            if before_count != after_count:
                applied_filters.append({
                    'name': filter_name,
                    'removed': before_count - after_count
                })
                print(f"  过滤结果: {before_count} -> {after_count} (移除 {before_count - after_count})")
            else:
                print(f"  过滤结果: 无变化 ({after_count})")
                
        except Exception as e:
            print(f"\n[失败] {filter_name}: {e}")
    
    print(f"\n" + "-" * 60)
    print("过滤完成")
    print("-" * 60)
    
    print(f"\n应用的过滤器:")
    for f in applied_filters:
        print(f"  {f['name']}: 移除 {f['removed']} 只")
    
    print(f"\n最终结果:")
    print(f"  原始数量: {original_count}")
    print(f"  最终数量: {len(test_data)}")
    print(f"  移除数量: {original_count - len(test_data)}")
    
    print(f"\n过滤后的数据:")
    print(test_data)
    
    # 4. 检查000542是否还在
    print(f"\n" + "=" * 60)
    print("验证结果")
    print("=" * 60)
    
    if "000542" in test_data["code"].to_list():
        print(f"\n❌ 失败: 000542 仍然在过滤后的数据中!")
        return False
    else:
        print(f"\n✅ 成功: 000542 已被正确过滤!")
        return True

if __name__ == "__main__":
    success = test_filter_execution()
    sys.exit(0 if success else 1)
