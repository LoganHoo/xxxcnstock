#!/usr/bin/env python3
"""
使用 Python API 设置 Great Expectations
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import great_expectations as ge
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def setup_ge():
    """设置 GE 配置"""
    print("=" * 80)
    print("🔧 设置 Great Expectations")
    print("=" * 80)
    
    # 创建 DataContext 配置
    data_context_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        datasources={
            "xcnstock_kline": {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            },
            "xcnstock_stock_list": {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
        }
    )
    
    # 创建临时 DataContext
    context = EphemeralDataContext(project_config=data_context_config)
    print("✅ DataContext 创建成功")
    
    # 创建期望套件 - 股票列表
    print("\n📋 创建股票列表期望套件...")
    suite = context.suites.add(
        ge.ExpectationSuite(name="stock_list_suite")
    )
    
    # 添加期望
    suite.add_expectation(
        ge.expectations.ExpectTableRowCountToBeBetween(min_value=1000, max_value=10000)
    )
    suite.add_expectation(
        ge.expectations.ExpectColumnToExist(column="code")
    )
    suite.add_expectation(
        ge.expectations.ExpectColumnToExist(column="name")
    )
    suite.add_expectation(
        ge.expectations.ExpectColumnValuesToNotBeNull(column="code")
    )
    
    print("✅ 股票列表期望套件创建成功")
    
    # 创建期望套件 - K线数据
    print("\n📋 创建K线数据期望套件...")
    suite_kline = context.suites.add(
        ge.ExpectationSuite(name="kline_data_suite")
    )
    
    suite_kline.add_expectation(
        ge.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000)
    )
    suite_kline.add_expectation(
        ge.expectations.ExpectColumnToExist(column="code")
    )
    suite_kline.add_expectation(
        ge.expectations.ExpectColumnToExist(column="trade_date")
    )
    suite_kline.add_expectation(
        ge.expectations.ExpectColumnToExist(column="close")
    )
    suite_kline.add_expectation(
        ge.expectations.ExpectColumnValuesToNotBeNull(column="trade_date")
    )
    suite_kline.add_expectation(
        ge.expectations.ExpectColumnValuesToNotBeNull(column="close")
    )
    
    print("✅ K线数据期望套件创建成功")
    
    # 测试验证
    print("\n🧪 测试验证股票列表...")
    try:
        import polars as pl
        
        stock_list_path = Path("/Volumes/Xdata/workstation/xxxcnstock/data/stock_list.parquet")
        if stock_list_path.exists():
            df = pl.read_parquet(stock_list_path)
            print(f"   股票数量: {len(df)}")
            
            # 转换为 pandas 进行验证
            df_pd = df.to_pandas()
            
            # 创建 batch
            batch = context.data_sources["xcnstock_stock_list"].add_batch(
                name="stock_list_batch",
                data=df_pd
            )
            
            # 运行验证
            validation_results = batch.validate(suite)
            
            print(f"   验证结果: {'✅ 通过' if validation_results.success else '❌ 失败'}")
            print(f"   统计: {validation_results.statistics}")
        else:
            print(f"   ⚠️  股票列表文件不存在")
    
    except Exception as e:
        print(f"   ❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("✅ GE 设置完成")
    print("=" * 80)
    
    return context


if __name__ == "__main__":
    setup_ge()
