#!/usr/bin/env python3
"""
Kestra 工作流监控脚本

功能：
- 查看工作流列表
- 查看执行历史
- 实时查看执行日志
- 监控执行状态

用法：
    python kestra/monitor.py --list-flows                    # 列出所有工作流
    python kestra/monitor.py --executions                    # 查看最近执行
    python kestra/monitor.py --logs --execution <ID>         # 查看执行日志
    python kestra/monitor.py --watch --execution <ID>        # 实时监控
    python kestra/monitor.py --status --execution <ID>       # 查看执行状态
"""
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import KestraClient, create_client, ExecutionStatus


def print_flows_table(flows):
    """打印工作流列表"""
    if not flows:
        print("❌ 没有找到工作流")
        return
    
    print("\n" + "=" * 80)
    print(f"工作流列表 (共 {len(flows)} 个)")
    print("=" * 80)
    print(f"{'命名空间':<20} {'ID':<30} {'任务数':<10}")
    print("-" * 80)
    
    for flow in flows:
        print(f"{flow.namespace:<20} {flow.id:<30} {flow.tasks_count:<10}")
    
    print("=" * 80)


def print_executions_table(executions):
    """打印执行历史"""
    if not executions:
        print("❌ 没有找到执行记录")
        return
    
    print("\n" + "=" * 100)
    print(f"执行历史 (共 {len(executions)} 条)")
    print("=" * 100)
    print(f"{'执行ID':<36} {'工作流':<35} {'状态':<12} {'开始时间':<20}")
    print("-" * 100)
    
    for exec in executions:
        status_icon = {
            ExecutionStatus.SUCCESS: "✅",
            ExecutionStatus.FAILED: "❌",
            ExecutionStatus.RUNNING: "🔄",
            ExecutionStatus.CREATED: "⏳",
            ExecutionStatus.QUEUED: "📥",
            ExecutionStatus.RETRYING: "🔁",
            ExecutionStatus.PAUSED: "⏸️ ",
            ExecutionStatus.KILLED: "💀",
            ExecutionStatus.CANCELLED: "🚫",
        }.get(exec.status, "❓")
        
        start_time = exec.start_date[:19] if exec.start_date else "N/A"
        flow_name = f"{exec.namespace}.{exec.flow_id}"
        
        print(f"{exec.id:<36} {flow_name:<35} {status_icon} {exec.status.value:<10} {start_time:<20}")
    
    print("=" * 100)


def print_execution_detail(execution):
    """打印执行详情"""
    if not execution:
        print("❌ 执行记录不存在")
        return
    
    print("\n" + "=" * 70)
    print("执行详情")
    print("=" * 70)
    print(f"执行ID:     {execution.id}")
    print(f"工作流:     {execution.namespace}.{execution.flow_id}")
    print(f"状态:       {execution.status.value}")
    print(f"开始时间:   {execution.start_date or 'N/A'}")
    print(f"结束时间:   {execution.end_date or 'N/A'}")
    
    if execution.duration_ms:
        duration_sec = execution.duration_ms / 1000
        print(f"执行时长:   {duration_sec:.2f} 秒")
    
    if execution.inputs:
        print(f"输入参数:   {execution.inputs}")
    
    print("=" * 70)


def print_logs(logs, follow=False):
    """打印日志"""
    if not logs:
        if not follow:
            print("❌ 没有找到日志")
        return
    
    for log in logs:
        timestamp = log.get('timestamp', '')[:19]
        level = log.get('level', 'INFO')
        message = log.get('message', '')
        task = log.get('taskId', '')
        
        level_color = {
            'INFO': '',
            'WARN': '\033[33m',
            'ERROR': '\033[31m',
            'DEBUG': '\033[36m',
        }.get(level, '')
        
        reset_color = '\033[0m'
        
        if task:
            print(f"[{timestamp}] [{level_color}{level}{reset_color}] [{task}] {message}")
        else:
            print(f"[{timestamp}] [{level_color}{level}{reset_color}] {message}")


def watch_execution(client: KestraClient, execution_id: str, poll_interval: int = 5):
    """实时监控执行"""
    print(f"\n🔍 开始监控执行: {execution_id}")
    print("按 Ctrl+C 停止监控\n")
    
    last_log_count = 0
    
    try:
        while True:
            # 获取执行状态
            execution = client.get_execution(execution_id)
            if not execution:
                print("❌ 执行记录不存在")
                break
            
            # 打印状态（只在状态变化时）
            status_line = f"\r状态: {execution.status.value}"
            if execution.duration_ms:
                status_line += f" | 时长: {execution.duration_ms / 1000:.1f}s"
            print(status_line, end='', flush=True)
            
            # 获取新日志
            logs = client.get_logs(execution_id)
            if len(logs) > last_log_count:
                new_logs = logs[last_log_count:]
                print()  # 换行
                print_logs(new_logs)
                last_log_count = len(logs)
            
            # 检查是否结束
            if execution.status in [
                ExecutionStatus.SUCCESS,
                ExecutionStatus.FAILED,
                ExecutionStatus.KILLED,
                ExecutionStatus.CANCELLED
            ]:
                print(f"\n\n✅ 执行已结束，最终状态: {execution.status.value}")
                break
            
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  监控已停止")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Kestra 工作流监控工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python kestra/monitor.py --list-flows
  python kestra/monitor.py --executions --limit 20
  python kestra/monitor.py --executions --flow xcnstock_data_pipeline
  python kestra/monitor.py --status --execution <ID>
  python kestra/monitor.py --logs --execution <ID>
  python kestra/monitor.py --watch --execution <ID>
        """
    )
    
    # 操作选项
    parser.add_argument(
        "--list-flows",
        action="store_true",
        help="列出所有工作流"
    )
    parser.add_argument(
        "--executions",
        action="store_true",
        help="查看执行历史"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="查看执行状态"
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="查看执行日志"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="实时监控执行"
    )
    
    # 过滤选项
    parser.add_argument(
        "--flow",
        type=str,
        help="工作流 ID 过滤"
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="xcnstock",
        help="命名空间 (默认: xcnstock)"
    )
    parser.add_argument(
        "--execution",
        type=str,
        help="执行 ID"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="返回数量限制 (默认: 10)"
    )
    parser.add_argument(
        "--status-filter",
        type=str,
        choices=['SUCCESS', 'FAILED', 'RUNNING', 'CREATED'],
        help="状态过滤"
    )
    
    args = parser.parse_args()
    
    # 如果没有指定操作，显示帮助
    if not any([args.list_flows, args.executions, args.status, args.logs, args.watch]):
        parser.print_help()
        sys.exit(0)
    
    # 打印头部信息
    print("=" * 70)
    print("Kestra 工作流监控工具")
    print("=" * 70)
    
    # 创建客户端
    client = create_client()
    print(f"API URL: {client.api_url}")
    print("=" * 70)
    
    # 测试连接
    success, message = client.test_connection()
    if not success:
        print(f"❌ {message}")
        sys.exit(1)
    print(f"✅ {message}\n")
    
    # 执行操作
    if args.list_flows:
        flows = client.list_flows(args.namespace)
        print_flows_table(flows)
    
    elif args.executions:
        executions = client.list_executions(
            namespace=args.namespace if args.flow else None,
            flow_id=args.flow,
            status=args.status_filter,
            limit=args.limit
        )
        print_executions_table(executions)
    
    elif args.status:
        if not args.execution:
            print("❌ 请指定执行 ID: --execution <ID>")
            sys.exit(1)
        execution = client.get_execution(args.execution)
        print_execution_detail(execution)
    
    elif args.logs:
        if not args.execution:
            print("❌ 请指定执行 ID: --execution <ID>")
            sys.exit(1)
        logs = client.get_logs(args.execution)
        print_logs(logs)
    
    elif args.watch:
        if not args.execution:
            print("❌ 请指定执行 ID: --execution <ID>")
            sys.exit(1)
        watch_execution(client, args.execution)


if __name__ == "__main__":
    main()
