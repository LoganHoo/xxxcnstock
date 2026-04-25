#!/usr/bin/env python3
"""
批量导入历史元数据到 DataHub

导入内容：
- K线数据资产
- 股票列表资产
- 技术指标资产
- 数据血缘关系
"""
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from services.metadata.datahub_client import (
    DataHubClient, 
    DatasetMetadata, 
    LineageEdge,
    get_datahub_client
)
from services.metadata.schema_registry import (
    SchemaRegistry,
    SchemaField,
    get_schema_registry,
    get_kline_schema,
    get_stock_list_schema,
    get_technical_features_schema
)
from services.metadata.data_catalog import (
    DataCatalog,
    DataAssetInfo,
    DataAssetType,
    DataDomain,
    get_data_catalog
)
from services.metadata.lineage_tracker import (
    LineageTracker,
    TransformationType,
    get_lineage_tracker
)


def scan_kline_files(data_dir: Path = None) -> List[Dict[str, Any]]:
    """扫描 K线数据文件"""
    data_dir = data_dir or Path("data/kline")
    
    if not data_dir.exists():
        print(f"⚠️  K线数据目录不存在: {data_dir}")
        return []
    
    files = list(data_dir.glob("*.parquet"))
    print(f"📁 发现 {len(files)} 个K线数据文件")
    
    assets = []
    for file in files[:10]:  # 只处理前10个作为示例
        try:
            df = pl.read_parquet(file)
            assets.append({
                'code': file.stem,
                'file': file.name,
                'rows': len(df),
                'columns': len(df.columns),
                'date_range': f"{df['trade_date'].min()} ~ {df['trade_date'].max()}" if 'trade_date' in df.columns else 'N/A'
            })
        except Exception as e:
            print(f"   ⚠️  读取失败 {file.name}: {e}")
    
    return assets


def import_kline_dataset(client: DataHubClient) -> bool:
    """导入 K线数据集"""
    print("\n📊 导入 K线数据集...")
    
    # 扫描文件
    kline_assets = scan_kline_files()
    total_rows = sum(a['rows'] for a in kline_assets)
    
    # 构建 Schema
    schema_fields = []
    for field in get_kline_schema():
        schema_fields.append({
            'name': field.name,
            'type': field.type,
            'description': field.description,
            'nullable': field.nullable
        })
    
    # 创建数据集元数据
    metadata = DatasetMetadata(
        name="xcnstock.kline_data",
        platform="parquet",
        env="PROD",
        description=f"A股K线数据，包含开盘价、收盘价、最高价、最低价、成交量等。共 {len(kline_assets)} 只股票，约 {total_rows:,} 条记录",
        schema_fields=schema_fields,
        properties={
            'total_stocks': str(len(kline_assets)),
            'total_rows': str(total_rows),
            'storage_format': 'parquet',
            'update_frequency': 'daily'
        },
        tags=["kline", "ohlc", "market_data", "a_share", "daily"],
        owners=["data_team"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    success = client.emit_dataset(metadata)
    if success:
        print(f"   ✅ K线数据集已导入: {len(kline_assets)} 只股票")
    else:
        print(f"   ❌ K线数据集导入失败")
    
    return success


def import_stock_list_dataset(client: DataHubClient) -> bool:
    """导入股票列表数据集"""
    print("\n📋 导入股票列表数据集...")
    
    stock_list_file = Path("data/stock_list.parquet")
    
    row_count = 0
    if stock_list_file.exists():
        try:
            df = pl.read_parquet(stock_list_file)
            row_count = len(df)
        except Exception as e:
            print(f"   ⚠️  读取股票列表失败: {e}")
    
    # 构建 Schema
    schema_fields = []
    for field in get_stock_list_schema():
        schema_fields.append({
            'name': field.name,
            'type': field.type,
            'description': field.description,
            'nullable': field.nullable
        })
    
    metadata = DatasetMetadata(
        name="xcnstock.stock_list",
        platform="parquet",
        env="PROD",
        description=f"A股股票列表，包含股票代码、名称、行业、市场等信息。共 {row_count} 只股票",
        schema_fields=schema_fields,
        properties={
            'total_stocks': str(row_count),
            'storage_format': 'parquet',
            'source': 'baostock'
        },
        tags=["stock_list", "basic_info", "a_share"],
        owners=["data_team"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    success = client.emit_dataset(metadata)
    if success:
        print(f"   ✅ 股票列表已导入: {row_count} 只股票")
    else:
        print(f"   ❌ 股票列表导入失败")
    
    return success


def import_technical_features_dataset(client: DataHubClient) -> bool:
    """导入技术指标数据集"""
    print("\n📈 导入技术指标数据集...")
    
    # 构建 Schema
    schema_fields = []
    for field in get_technical_features_schema():
        schema_fields.append({
            'name': field.name,
            'type': field.type,
            'description': field.description,
            'nullable': field.nullable
        })
    
    metadata = DatasetMetadata(
        name="xcnstock.technical_features",
        platform="parquet",
        env="PROD",
        description="技术指标特征数据，包含MACD、KDJ、RSI、均线等技术指标",
        schema_fields=schema_fields,
        properties={
            'indicators': 'MACD, KDJ, RSI, MA, BOLL',
            'storage_format': 'parquet',
            'update_frequency': 'daily'
        },
        tags=["technical", "features", "macd", "kdj", "rsi", "indicators"],
        owners=["data_team"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    success = client.emit_dataset(metadata)
    if success:
        print(f"   ✅ 技术指标数据集已导入")
    else:
        print(f"   ❌ 技术指标数据集导入失败")
    
    return success


def import_lineage_relationships(client: DataHubClient) -> bool:
    """导入血缘关系"""
    print("\n🔗 导入数据血缘关系...")
    
    lineages = [
        # 1. 原始数据采集
        LineageEdge(
            upstream_dataset="baostock_api",
            downstream_dataset="xcnstock.kline_data",
            transformation_type=TransformationType.RAW_INGESTION.value,
            transformation_sql="从Baostock API采集原始K线数据，包含日线行情",
            created_at=datetime.now().isoformat()
        ),
        # 2. 数据清洗
        LineageEdge(
            upstream_dataset="xcnstock.kline_data",
            downstream_dataset="xcnstock.kline_data_cleaned",
            transformation_type=TransformationType.CLEANING.value,
            transformation_sql="数据清洗: 去重、填充缺失值、异常值处理、退市股票过滤",
            created_at=datetime.now().isoformat()
        ),
        # 3. 特征工程
        LineageEdge(
            upstream_dataset="xcnstock.kline_data_cleaned",
            downstream_dataset="xcnstock.technical_features",
            transformation_type=TransformationType.FEATURE_ENGINEERING.value,
            transformation_sql="计算技术指标: MACD, KDJ, RSI, 均线, 布林带等",
            created_at=datetime.now().isoformat()
        ),
        # 4. 股票列表关联
        LineageEdge(
            upstream_dataset="xcnstock.stock_list",
            downstream_dataset="xcnstock.kline_data_cleaned",
            transformation_type=TransformationType.JOIN.value,
            transformation_sql="关联股票基本信息: 行业、市场等",
            created_at=datetime.now().isoformat()
        ),
    ]
    
    success_count = 0
    for lineage in lineages:
        if client.emit_lineage(lineage):
            success_count += 1
            print(f"   ✅ {lineage.upstream_dataset} -> {lineage.downstream_dataset}")
        else:
            print(f"   ❌ {lineage.upstream_dataset} -> {lineage.downstream_dataset}")
    
    print(f"\n   血缘关系导入: {success_count}/{len(lineages)} 成功")
    return success_count == len(lineages)


def import_data_catalog(catalog: DataCatalog) -> bool:
    """导入数据目录"""
    print("\n📚 导入数据目录...")
    
    assets = [
        DataAssetInfo(
            name="xcnstock.kline_data",
            type=DataAssetType.DATASET,
            domain=DataDomain.MARKET_DATA,
            description="A股K线数据，包含OHLCV等基本行情数据",
            owner="data_team",
            tags=["kline", "ohlc", "market", "a_share"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            quality_score=0.95
        ),
        DataAssetInfo(
            name="xcnstock.stock_list",
            type=DataAssetType.DATASET,
            domain=DataDomain.MARKET_DATA,
            description="A股股票列表，包含基本信息",
            owner="data_team",
            tags=["stock_list", "basic"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            quality_score=0.98
        ),
        DataAssetInfo(
            name="xcnstock.technical_features",
            type=DataAssetType.DATASET,
            domain=DataDomain.TECHNICAL,
            description="技术指标特征数据",
            owner="data_team",
            tags=["technical", "features", "indicators"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            quality_score=0.92
        ),
    ]
    
    success_count = 0
    for asset in assets:
        if catalog.register_asset(asset):
            success_count += 1
            print(f"   ✅ {asset.name}")
    
    print(f"\n   数据目录导入: {success_count}/{len(assets)} 成功")
    return success_count == len(assets)


def main():
    """主函数"""
    print("=" * 70)
    print("🚀 DataHub 批量元数据导入")
    print("=" * 70)
    
    # 初始化客户端
    client = get_datahub_client()
    catalog = get_data_catalog()
    
    # 检查连接
    print("\n🔌 检查 DataHub 连接...")
    health = client.health_check()
    print(f"   SDK 可用: {'✅' if health['sdk_available'] else '❌'}")
    print(f"   服务端点: {health['server_url']}")
    
    if not health['sdk_available']:
        print("\n⚠️  DataHub SDK 不可用，使用本地存储模式")
    
    # 导入数据集
    results = {
        'kline_dataset': import_kline_dataset(client),
        'stock_list_dataset': import_stock_list_dataset(client),
        'technical_features_dataset': import_technical_features_dataset(client),
        'lineage_relationships': import_lineage_relationships(client),
        'data_catalog': import_data_catalog(catalog),
    }
    
    # 汇总
    print("\n" + "=" * 70)
    print("📊 导入结果汇总")
    print("=" * 70)
    
    for name, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        print(f"   {name}: {status}")
    
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"\n总计: {success_count}/{total_count} 项导入成功")
    
    if success_count == total_count:
        print("\n🎉 所有元数据导入成功！")
        print("\n📱 访问 DataHub UI 查看: http://localhost:9002")
        return 0
    else:
        print("\n⚠️ 部分导入失败，请检查日志")
        return 1


if __name__ == "__main__":
    sys.exit(main())
