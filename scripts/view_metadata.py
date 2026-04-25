#!/usr/bin/env python3
"""
本地元数据查看器

在 DataHub 服务启动前，可以使用此脚本查看已导入的元数据
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.metadata.data_catalog import get_data_catalog
from services.metadata.lineage_tracker import get_lineage_tracker
from services.metadata.schema_registry import get_schema_registry


def view_catalog():
    """查看数据目录"""
    print("=" * 70)
    print("📚 数据目录")
    print("=" * 70)
    
    catalog = get_data_catalog()
    stats = catalog.get_statistics()
    
    print(f"\n📊 统计信息:")
    print(f"   总资产数: {stats['total_assets']}")
    print(f"   按类型: {stats['by_type']}")
    print(f"   按域: {stats['by_domain']}")
    
    print(f"\n📋 资产列表:")
    for name, asset in catalog._assets.items():
        print(f"\n   📁 {name}")
        print(f"      类型: {asset.type.value}")
        print(f"      域: {asset.domain.value}")
        print(f"      描述: {asset.description}")
        print(f"      标签: {', '.join(asset.tags)}")
        print(f"      质量评分: {asset.quality_score}")
        print(f"      更新: {asset.updated_at}")


def view_lineage():
    """查看血缘关系"""
    print("\n" + "=" * 70)
    print("🔗 数据血缘关系")
    print("=" * 70)
    
    tracker = get_lineage_tracker()
    
    if not tracker.lineage_edges:
        # 重新构建血缘
        tracker.build_kline_lineage()
    
    print(f"\n📊 血缘边数: {len(tracker.lineage_edges)}")
    
    print(f"\n📋 血缘链路:")
    for i, edge in enumerate(tracker.lineage_edges, 1):
        print(f"\n   {i}. {edge.upstream_dataset}")
        print(f"      ↓ [{edge.transformation_type}]")
        print(f"      {edge.downstream_dataset}")
        print(f"      描述: {edge.transformation_sql[:60]}...")


def view_schemas():
    """查看 Schema"""
    print("\n" + "=" * 70)
    print("📐 Schema 注册中心")
    print("=" * 70)
    
    registry = get_schema_registry()
    
    print(f"\n📊 注册的数据集: {len(registry._schemas)}")
    
    for dataset_name, versions in registry._schemas.items():
        latest = versions[-1]
        print(f"\n   📁 {dataset_name}")
        print(f"      版本: v{latest.version}")
        print(f"      字段数: {len(latest.fields)}")
        print(f"      创建: {latest.created_at}")
        print(f"      变更: {latest.change_log}")
        
        print(f"      字段列表:")
        for field in latest.fields[:5]:  # 只显示前5个
            nullable = "可空" if field.nullable else "非空"
            print(f"         • {field.name} ({field.type}) - {field.description[:30]}... [{nullable}]")
        if len(latest.fields) > 5:
            print(f"         ... 还有 {len(latest.fields) - 5} 个字段")


def main():
    """主函数"""
    print("\n" + "=" * 70)
    print("🚀 XCNStock 元数据查看器")
    print("=" * 70)
    print("\n💡 提示: DataHub 服务启动中，当前显示本地存储的元数据")
    print("   服务就绪后访问: http://localhost:9002")
    
    view_catalog()
    view_lineage()
    view_schemas()
    
    print("\n" + "=" * 70)
    print("✅ 查看完成")
    print("=" * 70)


if __name__ == "__main__":
    main()
