#!/usr/bin/env python3
"""
Kestra API 工作流执行脚本
支持：
- 列出所有工作流
- 触发工作流执行
- 查询执行状态
"""
import os
import sys
import json
import time
import argparse
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

KESTRA_API_URL = os.getenv('KESTRA_API_URL', 'http://localhost:8082/api/v1')
KESTRA_USERNAME = os.getenv('KESTRA_USERNAME', 'admin@kestra.io')
KESTRA_PASSWORD = os.getenv('KESTRA_PASSWORD', 'Kestra123')


def get_auth():
    """获取认证信息"""
    return (KESTRA_USERNAME, KESTRA_PASSWORD)


def list_flows(namespace: str = None) -> list:
    """列出所有工作流"""
    try:
        import requests
    except ImportError:
        print("错误: 需要安装 requests 库")
        print("运行: pip install requests")
        sys.exit(1)
    
    url = f"{KESTRA_API_URL}/flows/search"
    params = {"size": 100}
    if namespace:
        params["namespace"] = namespace
    
    response = requests.get(url, auth=get_auth(), params=params)
    response.raise_for_status()
    
    data = response.json()
    flows = data.get("results", [])
    
    print("\n" + "=" * 70)
    print(f"工作流列表 (共 {len(flows)} 个)")
    print("=" * 70)
    
    for flow in flows:
        flow_id = flow.get("id")
        ns = flow.get("namespace")
        desc = flow.get("description", "")[:50].replace("\n", " ")
        print(f"\n📋 {ns}.{flow_id}")
        if desc:
            print(f"   描述: {desc}...")
    
    print("\n" + "=" * 70)
    return flows


def execute_flow(namespace: str, flow_id: str, inputs: dict = None) -> str:
    """
    触发工作流执行
    
    Args:
        namespace: 命名空间
        flow_id: 工作流 ID
        inputs: 输入参数 (可选)
    
    Returns:
        execution_id: 执行 ID
    """
    try:
        import requests
    except ImportError:
        print("错误: 需要安装 requests 库")
        sys.exit(1)
    
    url = f"{KESTRA_API_URL}/executions/{namespace}/{flow_id}"
    
    # Kestra API 需要表单格式或特定 JSON 格式
    payload = {}
    if inputs:
        for key, value in inputs.items():
            payload[f"inputs.{key}"] = value
    
    print(f"\n🚀 触发工作流: {namespace}.{flow_id}")
    if inputs:
        print(f"   输入参数: {json.dumps(inputs, ensure_ascii=False)}")
    
    response = requests.post(url, auth=get_auth(), data=payload)
    response.raise_for_status()
    
    data = response.json()
    execution_id = data.get("id")
    
    print(f"✅ 执行已启动")
    print(f"   执行 ID: {execution_id}")
    
    return execution_id


def get_execution_status(execution_id: str) -> dict:
    """查询执行状态"""
    try:
        import requests
    except ImportError:
        print("错误: 需要安装 requests 库")
        sys.exit(1)
    
    url = f"{KESTRA_API_URL}/executions/{execution_id}"
    
    response = requests.get(url, auth=get_auth())
    response.raise_for_status()
    
    return response.json()


def wait_for_execution(execution_id: str, timeout: int = 3600, poll_interval: int = 5):
    """
    等待工作流执行完成
    
    Args:
        execution_id: 执行 ID
        timeout: 超时时间 (秒)
        poll_interval: 轮询间隔 (秒)
    """
    print(f"\n⏳ 等待执行完成 (每 {poll_interval} 秒检查一次)...")
    print("-" * 70)
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            print(f"\n⚠️ 超时 ({timeout} 秒)")
            return None
        
        status_data = get_execution_status(execution_id)
        state = status_data.get("state", {}).get("current", "UNKNOWN")
        
        # 显示进度
        print(f"[{elapsed:.0f}s] 状态: {state}", end="\r")
        
        # 检查是否完成
        if state in ["SUCCESS", "FAILED", "KILLED", "WARNING"]:
            print()  # 换行
            print("-" * 70)
            print(f"✅ 执行完成!")
            print(f"   最终状态: {state}")
            print(f"   总耗时: {elapsed:.1f} 秒")
            
            if state == "FAILED":
                error = status_data.get("state", {}).get("error", {})
                if error:
                    print(f"   错误: {error.get('message', '未知错误')}")
            
            return status_data
        
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(
        description="Kestra API 工作流执行工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 列出所有工作流
  python execute_flow.py list

  # 列出指定命名空间的工作流
  python execute_flow.py list --namespace xcnstock

  # 执行工作流
  python execute_flow.py exec --namespace xcnstock --flow xcnstock_data_pipeline

  # 执行工作流并等待完成
  python execute_flow.py exec --namespace xcnstock --flow xcnstock_morning_report --wait

  # 执行工作流并传递输入参数
  python execute_flow.py exec --namespace xcnstock --flow my_flow --input '{"key": "value"}'
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # list 命令
    list_parser = subparsers.add_parser("list", help="列出工作流")
    list_parser.add_argument("--namespace", "-n", help="按命名空间过滤")
    
    # exec 命令
    exec_parser = subparsers.add_parser("exec", help="执行工作流")
    exec_parser.add_argument("--namespace", "-n", required=True, help="命名空间")
    exec_parser.add_argument("--flow", "-f", required=True, help="工作流 ID")
    exec_parser.add_argument("--input", "-i", help="输入参数 (JSON 格式)")
    exec_parser.add_argument("--wait", "-w", action="store_true", help="等待执行完成")
    exec_parser.add_argument("--timeout", "-t", type=int, default=3600, help="超时时间 (秒)")
    
    # status 命令
    status_parser = subparsers.add_parser("status", help="查询执行状态")
    status_parser.add_argument("execution_id", help="执行 ID")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "list":
        list_flows(args.namespace)
    
    elif args.command == "exec":
        inputs = None
        if args.input:
            try:
                inputs = json.loads(args.input)
            except json.JSONDecodeError as e:
                print(f"错误: 输入参数 JSON 格式无效: {e}")
                sys.exit(1)
        
        execution_id = execute_flow(args.namespace, args.flow, inputs)
        
        if args.wait:
            wait_for_execution(execution_id, timeout=args.timeout)
        else:
            print(f"\n💡 使用以下命令查看状态:")
            print(f"   python execute_flow.py status {execution_id}")
    
    elif args.command == "status":
        status_data = get_execution_status(args.execution_id)
        state = status_data.get("state", {}).get("current", "UNKNOWN")
        flow_id = status_data.get("flowId")
        namespace = status_data.get("namespace")
        
        print("\n" + "=" * 70)
        print(f"执行状态: {namespace}.{flow_id}")
        print("=" * 70)
        print(f"执行 ID: {args.execution_id}")
        print(f"当前状态: {state}")
        print(f"开始时间: {status_data.get('state', {}).get('startDate', 'N/A')}")
        print(f"结束时间: {status_data.get('state', {}).get('endDate', 'N/A')}")
        
        if state == "FAILED":
            error = status_data.get("state", {}).get("error", {})
            if error:
                print(f"\n❌ 错误信息:")
                print(f"   {error.get('message', '未知错误')}")
        
        print("=" * 70)


if __name__ == "__main__":
    main()
