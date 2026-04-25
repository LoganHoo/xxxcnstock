#!/usr/bin/env python3
"""
列出通过 Kestra API 部署的工作流

使用方式:
    python kestra/list_api_flows.py
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import create_client


def main():
    print("=" * 70)
    print("📋 Kestra API 工作流列表")
    print("=" * 70)
    
    client = create_client()
    
    # 测试连接
    ok, msg = client.test_connection()
    print(f"\n连接状态: {'✅' if ok else '❌'} {msg}")
    
    if not ok:
        print("\n⚠️ 无法连接到 Kestra 服务")
        return 1
    
    # 列出所有工作流
    print("\n通过 API 部署的工作流:")
    print("-" * 70)
    
    flows = client.list_flows()
    
    if not flows:
        print("暂无通过 API 部署的工作流")
        print("\n提示: 使用以下命令部署工作流")
        print("  python kestra/deploy.py")
        return 0
    
    # 按命名空间分组
    by_namespace = {}
    for flow in flows:
        ns = flow.namespace
        if ns not in by_namespace:
            by_namespace[ns] = []
        by_namespace[ns].append(flow)
    
    total = 0
    for namespace, ns_flows in sorted(by_namespace.items()):
        print(f"\n📁 命名空间: {namespace}")
        for flow in sorted(ns_flows, key=lambda x: x.id):
            print(f"   ✅ {flow.id}")
            print(f"      任务数: {flow.tasks_count}")
            if flow.description:
                print(f"      描述: {flow.description}")
            total += 1
    
    print("\n" + "=" * 70)
    print(f"总计: {total} 个工作流")
    print("=" * 70)
    
    # 对比本地文件
    print("\n📁 本地工作流文件 (kestra/flows/):")
    flows_dir = Path("kestra/flows")
    if flows_dir.exists():
        local_files = list(flows_dir.glob("*.yml"))
        print(f"   共 {len(local_files)} 个 YAML 文件")
        
        # 检查哪些已部署
        deployed_ids = {f"{f.namespace}/{f.id}" for f in flows}
        
        print("\n部署状态对比:")
        for f in sorted(local_files):
            import yaml
            try:
                with open(f, 'r') as file:
                    data = yaml.safe_load(file)
                flow_id = data.get('id', 'unknown')
                ns = data.get('namespace', 'xcnstock')
                full_id = f"{ns}/{flow_id}"
                
                if full_id in deployed_ids:
                    print(f"   ✅ {f.name} -> 已部署")
                else:
                    print(f"   ⏳ {f.name} -> 未部署")
            except:
                print(f"   ⚠️  {f.name} -> 无法解析")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
