#!/usr/bin/env python3
"""
检查Kestra工作流执行状态

分析每个工作流的执行历史，检查是否成功执行

使用方式:
    python kestra/check_executions.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import create_client, ExecutionStatus


def get_status_icon(status: ExecutionStatus) -> str:
    """获取状态图标"""
    icons = {
        ExecutionStatus.SUCCESS: "✅",
        ExecutionStatus.FAILED: "❌",
        ExecutionStatus.RUNNING: "🔄",
        ExecutionStatus.CREATED: "⏳",
        ExecutionStatus.PAUSED: "⏸️",
        ExecutionStatus.WARNING: "⚠️",
        ExecutionStatus.KILLED: "🛑",
        ExecutionStatus.QUEUED: "📥",
        ExecutionStatus.RETRYING: "🔄",
        ExecutionStatus.CANCELLED: "❌",
    }
    return icons.get(status, "❓")


def format_duration(duration_ms: int) -> str:
    """格式化持续时间"""
    if not duration_ms:
        return "N/A"
    
    seconds = duration_ms // 1000
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def check_flow_executions(client, namespace: str, flow_id: str, limit: int = 5):
    """检查单个工作流的执行历史"""
    executions = client.list_executions(namespace=namespace, flow_id=flow_id, limit=limit)
    
    if not executions:
        return None, "无执行记录"
    
    # 统计
    total = len(executions)
    success = sum(1 for e in executions if e.status == ExecutionStatus.SUCCESS)
    failed = sum(1 for e in executions if e.status == ExecutionStatus.FAILED)
    running = sum(1 for e in executions if e.status == ExecutionStatus.RUNNING)
    
    # 最新执行
    latest = executions[0]
    
    return {
        'total': total,
        'success': success,
        'failed': failed,
        'running': running,
        'latest': latest,
        'executions': executions
    }, None


def main():
    print("=" * 80)
    print("🔍 Kestra 工作流执行状态检查")
    print("=" * 80)
    
    client = create_client()
    
    # 测试连接
    ok, msg = client.test_connection()
    print(f"\n连接状态: {'✅' if ok else '❌'} {msg}")
    
    if not ok:
        print("\n⚠️ 无法连接到 Kestra 服务")
        return 1
    
    # 获取xcnstock命名空间的工作流
    namespace = "xcnstock"
    flows = client.list_flows(namespace=namespace)
    
    if not flows:
        print(f"\n⚠️ 命名空间 {namespace} 中没有工作流")
        return 1
    
    print(f"\n发现 {len(flows)} 个工作流\n")
    
    # 检查每个工作流
    results = []
    
    for flow in sorted(flows, key=lambda x: x.id):
        print(f"📋 {flow.id}")
        print("-" * 80)
        
        stats, error = check_flow_executions(client, namespace, flow.id)
        
        if error:
            print(f"   状态: ⚠️ {error}")
            results.append({
                'flow_id': flow.id,
                'status': 'no_executions',
                'message': error
            })
        else:
            # 显示统计
            print(f"   最近执行统计 (最近{stats['total']}次):")
            print(f"      ✅ 成功: {stats['success']}")
            print(f"      ❌ 失败: {stats['failed']}")
            print(f"      🔄 运行中: {stats['running']}")
            
            # 显示最新执行
            latest = stats['latest']
            icon = get_status_icon(latest.status)
            print(f"\n   最新执行:")
            print(f"      {icon} 状态: {latest.status.value}")
            print(f"      🆔 ID: {latest.id[:20]}...")
            print(f"      🕐 开始: {latest.start_date or 'N/A'}")
            print(f"      ⏱️  耗时: {format_duration(latest.duration_ms)}")
            
            # 判断状态
            if latest.status == ExecutionStatus.SUCCESS:
                flow_status = 'success'
            elif latest.status == ExecutionStatus.FAILED:
                flow_status = 'failed'
            elif latest.status == ExecutionStatus.RUNNING:
                flow_status = 'running'
            else:
                flow_status = 'other'
            
            results.append({
                'flow_id': flow.id,
                'status': flow_status,
                'latest_status': latest.status.value,
                'success_rate': stats['success'] / stats['total'] * 100 if stats['total'] > 0 else 0,
                'total_executions': stats['total']
            })
        
        print()
    
    # 汇总
    print("=" * 80)
    print("📊 执行状态汇总")
    print("=" * 80)
    
    success_flows = [r for r in results if r['status'] == 'success']
    failed_flows = [r for r in results if r['status'] == 'failed']
    running_flows = [r for r in results if r['status'] == 'running']
    no_exec_flows = [r for r in results if r['status'] == 'no_executions']
    
    print(f"\n✅ 最近执行成功: {len(success_flows)} 个")
    for r in success_flows:
        print(f"   - {r['flow_id']} (成功率: {r.get('success_rate', 0):.0f}%)")
    
    print(f"\n❌ 最近执行失败: {len(failed_flows)} 个")
    for r in failed_flows:
        print(f"   - {r['flow_id']}")
    
    print(f"\n🔄 正在运行: {len(running_flows)} 个")
    for r in running_flows:
        print(f"   - {r['flow_id']}")
    
    print(f"\n⚠️  从未执行: {len(no_exec_flows)} 个")
    for r in no_exec_flows:
        print(f"   - {r['flow_id']}")
    
    # 生成报告
    report_dir = Path('data/test_reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    md_path = report_dir / f'kestra_executions_{timestamp}.md'
    
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 工作流执行状态报告\n\n')
        f.write(f'检查时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        f.write('## 执行状态汇总\n\n')
        f.write(f'- ✅ 最近执行成功: {len(success_flows)} 个\n')
        f.write(f'- ❌ 最近执行失败: {len(failed_flows)} 个\n')
        f.write(f'- 🔄 正在运行: {len(running_flows)} 个\n')
        f.write(f'- ⚠️  从未执行: {len(no_exec_flows)} 个\n\n')
        
        f.write('## 详细状态\n\n')
        f.write('| 工作流 | 状态 | 成功率 | 总执行次数 |\n')
        f.write('|--------|------|--------|------------|\n')
        
        for r in results:
            status_text = {
                'success': '✅ 成功',
                'failed': '❌ 失败',
                'running': '🔄 运行中',
                'no_executions': '⚠️ 未执行'
            }.get(r['status'], r['status'])
            
            success_rate = f"{r.get('success_rate', 0):.0f}%" if 'success_rate' in r else 'N/A'
            total = r.get('total_executions', 'N/A')
            
            f.write(f"| {r['flow_id']} | {status_text} | {success_rate} | {total} |\n")
        
        f.write('\n## 结论\n\n')
        if len(failed_flows) == 0 and len(no_exec_flows) == 0:
            f.write('✅ **所有工作流执行正常**\n')
        elif len(failed_flows) > 0:
            f.write(f'⚠️ **有 {len(failed_flows)} 个工作流最近执行失败，需要检查**\n')
        else:
            f.write(f'ℹ️ **有 {len(no_exec_flows)} 个工作流尚未执行，这是正常的（新部署或定时任务未到触发时间）**\n')
    
    print(f"\n📄 报告已保存: {md_path}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
