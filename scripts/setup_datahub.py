#!/usr/bin/env python3
"""
DataHub 集成设置脚本

初始化 DataHub 集成，包括：
- 注册数据资产
- 建立血缘关系
- 初始化 Schema
- 创建数据目录
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.metadata.data_catalog import initialize_default_catalog, get_data_catalog
from services.metadata.lineage_tracker import get_lineage_tracker, TransformationType
from services.metadata.schema_registry import get_schema_registry, get_kline_schema


def setup_datahub():
    """设置 DataHub 集成"""
    print("=" * 70)
    print("🚀 DataHub 集成初始化")
    print("=" * 70)
    
    # 1. 初始化数据目录
    print("\n📋 步骤 1: 初始化数据目录")
    catalog = initialize_default_catalog()
    stats = catalog.get_statistics()
    print(f"   ✅ 已注册 {stats['total_assets']} 个数据资产")
    
    # 2. 建立血缘关系
    print("\n🔗 步骤 2: 建立数据血缘关系")
    tracker = get_lineage_tracker()
    edges = tracker.build_kline_lineage()
    print(f"   ✅ 已建立 {len(edges)} 条血缘关系")
    
    # 3. 保存血缘图
    print("\n💾 步骤 3: 保存血缘关系图")
    lineage_file = tracker.save_lineage_to_file()
    print(f"   ✅ 血缘图已保存: {lineage_file}")
    
    # 4. 导出目录
    print("\n📊 步骤 4: 导出数据目录")
    catalog_data = catalog.export_catalog()
    print(f"   资产总数: {catalog_data['statistics']['total_assets']}")
    print(f"   按类型分布: {catalog_data['statistics']['by_type']}")
    print(f"   按域分布: {catalog_data['statistics']['by_domain']}")
    
    # 5. 检查 DataHub 连接
    print("\n🔌 步骤 5: 检查 DataHub 连接")
    from services.metadata.datahub_client import get_datahub_client
    client = get_datahub_client()
    health = client.health_check()
    
    print(f"   SDK 可用: {'✅' if health['sdk_available'] else '❌'}")
    print(f"   连接状态: {health.get('connection', '未配置')}")
    print(f"   服务端点: {health['server_url']}")
    
    print("\n" + "=" * 70)
    print("✅ DataHub 集成初始化完成")
    print("=" * 70)
    print("\n📚 使用指南:")
    print("   1. 查看数据目录: python -c \"from services.metadata.data_catalog import get_data_catalog; c = get_data_catalog(); print(c.get_statistics())\"")
    print("   2. 搜索资产: python -c \"from services.metadata.data_catalog import get_data_catalog; c = get_data_catalog(); print([a.name for a in c.search('kline')])\"")
    print("   3. 查看血缘: cat data/metadata/lineage/lineage_graph.json")
    
    return True


if __name__ == "__main__":
    try:
        setup_datahub()
    except Exception as e:
        print(f"\n❌ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
