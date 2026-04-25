#!/usr/bin/env python3
"""
使用 GE 1.16.1 新 API 设置 Great Expectations
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import great_expectations as ge
import great_expectations.expectations as gxe
import polars as pl


def setup_and_validate():
    """设置 GE 并运行验证"""
    print("=" * 80)
    print("🔧 设置 Great Expectations 1.16.1")
    print("=" * 80)
    
    print(f"\n📦 GE 版本: {ge.__version__}")
    
    # 1. 获取 DataContext
    print("\n📂 初始化 DataContext...")
    context = ge.get_context(mode="ephemeral")
    print("✅ DataContext 创建成功")
    
    # 2. 创建 Expectation Suite - 股票列表
    print("\n📋 创建股票列表期望套件...")
    suite_stock = context.suites.add(ge.ExpectationSuite(name="stock_list_suite"))
    
    # 添加期望
    suite_stock.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1000, max_value=10000))
    suite_stock.add_expectation(gxe.ExpectColumnToExist(column="code"))
    suite_stock.add_expectation(gxe.ExpectColumnToExist(column="name"))
    suite_stock.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="code"))
    suite_stock.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="code", regex=r"^\d{6}$"))
    
    print("✅ 股票列表期望套件创建成功")
    print(f"   期望数量: {len(suite_stock.expectations)}")
    
    # 3. 创建 Expectation Suite - K线数据
    print("\n📋 创建K线数据期望套件...")
    suite_kline = context.suites.add(ge.ExpectationSuite(name="kline_data_suite"))
    
    suite_kline.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000))
    suite_kline.add_expectation(gxe.ExpectColumnToExist(column="code"))
    suite_kline.add_expectation(gxe.ExpectColumnToExist(column="trade_date"))
    suite_kline.add_expectation(gxe.ExpectColumnToExist(column="close"))
    suite_kline.add_expectation(gxe.ExpectColumnToExist(column="volume"))
    suite_kline.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="trade_date"))
    suite_kline.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="close"))
    suite_kline.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0.01, max_value=10000))
    suite_kline.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0, max_value=1e9))
    
    print("✅ K线数据期望套件创建成功")
    print(f"   期望数量: {len(suite_kline.expectations)}")
    
    # 4. 验证股票列表
    print("\n🧪 验证股票列表数据...")
    try:
        stock_list_path = Path("/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet")
        if stock_list_path.exists():
            df = pl.read_parquet(stock_list_path)
            print(f"   数据行数: {len(df)}")
            print(f"   数据列: {df.columns}")
            
            # 转换为 pandas 进行验证
            df_pd = df.to_pandas()
            
            # 运行验证
            results = suite_stock.validate(df_pd)
            
            print(f"\n   验证结果:")
            print(f"   - 整体状态: {'✅ 通过' if results.success else '❌ 失败'}")
            print(f"   - 统计信息:")
            for key, value in results.statistics.items():
                print(f"     • {key}: {value}")
            
            # 显示失败的期望
            if not results.success:
                print(f"\n   失败的期望:")
                for result in results.results:
                    if not result.success:
                        print(f"     ❌ {result.expectation_config.type}: {result.result}")
        else:
            print(f"   ⚠️  股票列表文件不存在")
    
    except Exception as e:
        print(f"   ❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 5. 验证K线数据样本
    print("\n🧪 验证K线数据样本...")
    try:
        kline_files = list(Path("/Volumes/Xdata/workstation/xxxcnstock/data/kline").glob("*.parquet"))
        if kline_files:
            sample_file = kline_files[0]
            print(f"   样本文件: {sample_file.name}")
            
            df = pl.read_parquet(sample_file)
            print(f"   数据行数: {len(df)}")
            print(f"   数据列: {df.columns}")
            
            # 转换为 pandas 进行验证
            df_pd = df.to_pandas()
            
            # 运行验证
            results = suite_kline.validate(df_pd)
            
            print(f"\n   验证结果:")
            print(f"   - 整体状态: {'✅ 通过' if results.success else '❌ 失败'}")
            print(f"   - 统计信息:")
            for key, value in results.statistics.items():
                print(f"     • {key}: {value}")
            
            # 显示失败的期望
            if not results.success:
                print(f"\n   失败的期望:")
                for result in results.results:
                    if not result.success:
                        print(f"     ❌ {result.expectation_config.type}: {result.result}")
        else:
            print(f"   ⚠️  K线数据文件不存在")
    
    except Exception as e:
        print(f"   ❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("✅ GE 设置和验证完成")
    print("=" * 80)
    
    return context, suite_stock, suite_kline


if __name__ == "__main__":
    setup_and_validate()
