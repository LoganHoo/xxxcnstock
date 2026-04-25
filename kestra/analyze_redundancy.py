#!/usr/bin/env python3
"""
分析Kestra工作流冗余和重复

检查本地文件和服务器部署的工作流，识别冗余和重复

使用方式:
    python kestra/analyze_redundancy.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import yaml

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import create_client


def extract_workflow_signature(flow_file: Path) -> dict:
    """提取工作流签名（用于比较相似度）"""
    try:
        with open(flow_file, 'r', encoding='utf-8') as f:
            content = f.read()
            data = yaml.safe_load(content)
        
        # 提取关键特征
        tasks = data.get('tasks', [])
        task_types = [t.get('type', '') for t in tasks]
        task_ids = [t.get('id', '') for t in tasks]
        
        # 提取脚本内容（用于比较）
        scripts = []
        for task in tasks:
            if 'script' in task:
                scripts.append(task['script'][:100])  # 前100字符
        
        return {
            'id': data.get('id', ''),
            'namespace': data.get('namespace', ''),
            'task_count': len(tasks),
            'task_types': task_types,
            'task_ids': task_ids,
            'scripts_hash': hash('\n'.join(scripts)) if scripts else 0,
            'triggers': len(data.get('triggers', [])),
        }
    except Exception as e:
        return {'error': str(e)}


def compare_workflows(sig1: dict, sig2: dict) -> float:
    """比较两个工作流的相似度 (0-1)"""
    if sig1.get('error') or sig2.get('error'):
        return 0.0
    
    # ID相同则认为是同一工作流的不同版本
    if sig1['id'] == sig2['id']:
        return 1.0
    
    similarity = 0.0
    
    # 任务数量相同
    if sig1['task_count'] == sig2['task_count']:
        similarity += 0.2
    
    # 任务类型相同
    if sig1['task_types'] == sig2['task_types']:
        similarity += 0.3
    
    # 任务ID相同
    if sig1['task_ids'] == sig2['task_ids']:
        similarity += 0.3
    
    # 脚本内容相同
    if sig1['scripts_hash'] == sig2['scripts_hash']:
        similarity += 0.2
    
    return similarity


def main():
    print("=" * 80)
    print("🔍 Kestra 工作流冗余分析")
    print("=" * 80)
    
    flows_dir = Path('kestra/flows')
    if not flows_dir.exists():
        print("❌ 工作流目录不存在")
        return 1
    
    # 获取所有本地工作流文件
    flow_files = list(flows_dir.glob('*.yml'))
    print(f"\n📁 本地工作流文件: {len(flow_files)} 个")
    
    # 提取签名
    signatures = {}
    for flow_file in flow_files:
        sig = extract_workflow_signature(flow_file)
        signatures[flow_file.name] = sig
        print(f"  - {flow_file.name}: {sig.get('id', 'unknown')} ({sig.get('task_count', 0)} tasks)")
    
    # 检查服务器上的工作流
    print("\n" + "=" * 80)
    print("🌐 服务器工作流检查")
    print("=" * 80)
    
    client = create_client()
    ok, msg = client.test_connection()
    
    server_flows = {}
    if ok:
        flows = client.list_flows(namespace='xcnstock')
        print(f"\n✅ 连接成功，发现 {len(flows)} 个工作流")
        for flow in flows:
            server_flows[flow.id] = flow
            print(f"  - {flow.id} ({flow.tasks_count} tasks)")
    else:
        print(f"\n⚠️ 无法连接服务器: {msg}")
    
    # 分析冗余
    print("\n" + "=" * 80)
    print("📊 冗余分析")
    print("=" * 80)
    
    # 1. 同名不同文件（ID相同但文件名不同）
    print("\n1. 同名工作流（ID相同）:")
    id_to_files = {}
    for filename, sig in signatures.items():
        wid = sig.get('id', '')
        if wid:
            if wid not in id_to_files:
                id_to_files[wid] = []
            id_to_files[wid].append(filename)
    
    duplicates = {wid: files for wid, files in id_to_files.items() if len(files) > 1}
    if duplicates:
        for wid, files in duplicates.items():
            print(f"\n   ⚠️  ID: {wid}")
            for f in files:
                print(f"      - {f}")
    else:
        print("   ✅ 无同名工作流")
    
    # 2. 相似工作流
    print("\n2. 相似工作流（相似度>0.7）:")
    similar_pairs = []
    files = list(signatures.keys())
    for i in range(len(files)):
        for j in range(i + 1, len(files)):
            sim = compare_workflows(signatures[files[i]], signatures[files[j]])
            if sim > 0.7:
                similar_pairs.append((files[i], files[j], sim))
    
    if similar_pairs:
        for f1, f2, sim in similar_pairs:
            print(f"   ⚠️  {f1} <-> {f2} (相似度: {sim:.1%})")
    else:
        print("   ✅ 无高度相似工作流")
    
    # 3. 文件与服务器不一致
    print("\n3. 本地与服务器状态:")
    local_only = []
    server_only = list(server_flows.keys())
    synced = []
    
    for filename, sig in signatures.items():
        wid = sig.get('id', '')
        if wid in server_flows:
            synced.append((filename, wid))
            if wid in server_only:
                server_only.remove(wid)
        else:
            local_only.append(filename)
    
    print(f"\n   ✅ 已同步 ({len(synced)} 个):")
    for filename, wid in synced:
        print(f"      - {filename} -> {wid}")
    
    if local_only:
        print(f"\n   ⚠️  仅本地存在 ({len(local_only)} 个):")
        for f in local_only:
            print(f"      - {f}")
    
    if server_only:
        print(f"\n   ⚠️  仅服务器存在 ({len(server_only)} 个):")
        for wid in server_only:
            print(f"      - {wid}")
    
    # 4. 识别具体问题
    print("\n" + "=" * 80)
    print("🎯 具体问题识别")
    print("=" * 80)
    
    issues = []
    
    # 检查 _simple 后缀的工作流
    simple_versions = [f for f in flow_files if '_simple' in f.name]
    if simple_versions:
        issues.append({
            'type': '简化版本冗余',
            'files': [f.name for f in simple_versions],
            'description': '这些工作流是简化版本，可能与完整版本功能重复',
            'recommendation': '如果完整版本稳定运行，可以删除简化版本'
        })
    
    # 检查重复ID
    if duplicates:
        for wid, files in duplicates.items():
            issues.append({
                'type': 'ID重复',
                'files': files,
                'description': f'多个文件使用相同的ID: {wid}',
                'recommendation': '保留最新版本，删除旧版本，或修改ID'
            })
    
    if issues:
        for i, issue in enumerate(issues, 1):
            print(f"\n{i}. {issue['type']}")
            print(f"   文件: {', '.join(issue['files'])}")
            print(f"   说明: {issue['description']}")
            print(f"   建议: {issue['recommendation']}")
    else:
        print("\n   ✅ 未发现明显冗余问题")
    
    # 生成报告
    print("\n" + "=" * 80)
    print("📝 清理建议")
    print("=" * 80)
    
    # 建议删除的文件
    to_delete = []
    
    # 1. _simple 版本（如果完整版本运行正常）
    for f in simple_versions:
        base_name = f.name.replace('_simple', '')
        if base_name in [fn.name for fn in flow_files]:
            to_delete.append({
                'file': f.name,
                'reason': f'存在完整版本: {base_name}'
            })
    
    if to_delete:
        print("\n建议删除的文件:")
        for item in to_delete:
            print(f"   - {item['file']} ({item['reason']})")
        print("\n删除命令:")
        for item in to_delete:
            print(f"   rm kestra/flows/{item['file']}")
    else:
        print("\n✅ 当前无需清理")
    
    # 保存报告
    report_dir = Path('data/test_reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    md_path = report_dir / f'kestra_redundancy_analysis_{timestamp}.md'
    
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 工作流冗余分析报告\n\n')
        f.write(f'分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        f.write('## 概述\n\n')
        f.write(f'- 本地工作流文件: {len(flow_files)} 个\n')
        f.write(f'- 服务器工作流: {len(server_flows)} 个\n')
        f.write(f'- 已同步: {len(synced)} 个\n\n')
        
        if duplicates:
            f.write('## ID重复问题\n\n')
            for wid, files in duplicates.items():
                f.write(f'### {wid}\n')
                for fn in files:
                    f.write(f'- {fn}\n')
                f.write('\n')
        
        if simple_versions:
            f.write('## 简化版本工作流\n\n')
            f.write('以下工作流可能是简化版本，建议评估是否需要保留:\n\n')
            for sf in simple_versions:
                f.write(f'- {sf.name}\n')
            f.write('\n')
        
        if to_delete:
            f.write('## 建议删除\n\n')
            for item in to_delete:
                f.write(f'- **{item["file"]}**: {item["reason"]}\n')
        
        f.write('\n## 结论\n\n')
        if to_delete or duplicates:
            f.write('⚠️ 发现冗余工作流，建议按上述建议清理\n')
        else:
            f.write('✅ 未发现明显冗余问题\n')
    
    print(f"\n📄 报告已保存: {md_path}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
