#!/usr/bin/env python3
"""
测试 Great Expectations 配置
"""
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import great_expectations as ge
from great_expectations.data_context import EphemeralDataContext, FileDataContext


def test_ge_setup():
    """测试 GE 配置"""
    print("=" * 80)
    print("🧪 测试 Great Expectations 配置")
    print("=" * 80)
    
    # 1. 检查 GE 版本
    print(f"\n📦 GE 版本: {ge.__version__}")
    
    # 2. 加载 DataContext
    print("\n📂 加载 DataContext...")
    try:
        context = FileDataContext(context_root_dir="/Volumes/Xdata/workstation/xxxcnstock/gx")
        print("   ✅ DataContext 加载成功")
    except Exception as e:
        print(f"   ❌ DataContext 加载失败: {e}")
        return False
    
    # 3. 检查 Datasources
    print("\n📊 检查 Datasources:")
    datasources = context.list_datasources()
    for ds in datasources:
        print(f"   ✅ {ds['name']}")
    
    # 4. 检查 Expectation Suites
    print("\n📋 检查 Expectation Suites:")
    suites = context.list_expectation_suites()
    for suite in suites:
        print(f"   ✅ {suite.name}")
    
    # 5. 检查 Checkpoints
    print("\n🚦 检查 Checkpoints:")
    try:
        checkpoints = context.list_checkpoints()
        for cp in checkpoints:
            print(f"   ✅ {cp}")
    except Exception as e:
        print(f"   ⚠️  无法列出 checkpoints: {e}")
    
    # 6. 测试验证
    print("\n🧪 测试验证股票列表:")
    try:
        import polars as pl
        
        # 加载股票列表
        stock_list_path = Path("/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet")
        if stock_list_path.exists():
            df = pl.read_parquet(stock_list_path)
            print(f"   股票数量: {len(df)}")
            print(f"   列: {df.columns}")
            
            # 运行验证
            checkpoint = context.get_checkpoint("stock_list_checkpoint")
            checkpoint_result = checkpoint.run()
            
            print(f"   验证结果: {'✅ 通过' if checkpoint_result.success else '❌ 失败'}")
            
            # 显示详细信息
            for validation_result in checkpoint_result.run_results.values():
                statistics = validation_result.results[0].result.get('statistics', {})
                print(f"   成功: {statistics.get('successful_expectations', 0)}/{statistics.get('evaluated_expectations', 0)}")
        else:
            print(f"   ⚠️  股票列表文件不存在")
    
    except Exception as e:
        print(f"   ❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("✅ GE 配置测试完成")
    print("=" * 80)
    
    return True


if __name__ == "__main__":
    test_ge_setup()
