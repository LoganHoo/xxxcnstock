#!/usr/bin/env python3
"""
Kestra 工作流部署脚本

功能：
- 批量部署所有工作流
- 部署前 YAML 语法验证
- 部署后自动验证
- 生成部署报告

用法：
    python kestra/deploy.py                    # 部署所有工作流
    python kestra/deploy.py --flow <文件名>     # 部署单个工作流
    python kestra/deploy.py --validate-only     # 仅验证不部署
    python kestra/deploy.py --dry-run           # 模拟部署
"""
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import KestraClient, create_client


def validate_yaml(flow_file: Path) -> Tuple[bool, str]:
    """
    验证 YAML 文件语法
    
    Returns:
        (是否有效, 错误信息)
    """
    try:
        import yaml
        with open(flow_file, 'r') as f:
            data = yaml.safe_load(f)
        
        # 检查必需字段
        if not isinstance(data, dict):
            return False, "YAML 内容必须是字典"
        
        if 'id' not in data:
            return False, "缺少必需字段: id"
        
        if 'namespace' not in data:
            return False, "缺少必需字段: namespace"
        
        if 'tasks' not in data:
            return False, "缺少必需字段: tasks"
        
        return True, "验证通过"
        
    except ImportError:
        return False, "需要安装 PyYAML: pip install pyyaml"
    except yaml.YAMLError as e:
        return False, f"YAML 解析错误: {e}"
    except Exception as e:
        return False, f"验证异常: {e}"


def deploy_single_flow(client: KestraClient, flow_file: Path, dry_run: bool = False) -> Tuple[bool, str]:
    """
    部署单个工作流
    
    Args:
        client: Kestra 客户端
        flow_file: 工作流文件路径
        dry_run: 是否仅模拟
        
    Returns:
        (成功标志, 消息)
    """
    print(f"\n📄 {flow_file.name}")
    print("   " + "-" * 50)
    
    # 1. 验证 YAML 语法
    valid, message = validate_yaml(flow_file)
    if not valid:
        print(f"   ❌ YAML 验证失败: {message}")
        return False, message
    print(f"   ✅ YAML 验证通过")
    
    if dry_run:
        print(f"   ⏸️  模拟模式，跳过部署")
        return True, "模拟部署成功"
    
    # 2. 部署到 Kestra
    success, message = client.deploy_flow(flow_file)
    if success:
        print(f"   ✅ {message}")
    else:
        print(f"   ❌ {message}")
    
    return success, message


def deploy_all_flows(client: KestraClient, flows_dir: Path, dry_run: bool = False) -> Dict[str, Tuple[bool, str]]:
    """
    批量部署所有工作流
    
    Returns:
        部署结果字典
    """
    results = {}
    
    # 查找所有 YAML 文件
    flow_files = sorted(list(flows_dir.glob("*.yml")) + list(flows_dir.glob("*.yaml")))
    
    if not flow_files:
        print(f"❌ 未找到工作流文件: {flows_dir}")
        return results
    
    print(f"\n📁 发现 {len(flow_files)} 个工作流文件")
    print("=" * 70)
    
    # 逐个部署
    for flow_file in flow_files:
        success, message = deploy_single_flow(client, flow_file, dry_run)
        results[flow_file.name] = (success, message)
    
    return results


def print_deploy_report(results: Dict[str, Tuple[bool, str]]) -> None:
    """打印部署报告"""
    if not results:
        return
    
    success_count = sum(1 for s, _ in results.values() if s)
    total_count = len(results)
    
    print("\n" + "=" * 70)
    print("部署报告")
    print("=" * 70)
    print(f"总计: {total_count} | 成功: {success_count} | 失败: {total_count - success_count}")
    print("-" * 70)
    
    for filename, (success, message) in results.items():
        status = "✅" if success else "❌"
        print(f"{status} {filename}")
        if not success:
            print(f"   错误: {message}")
    
    print("=" * 70)


def verify_deployments(client: KestraClient) -> bool:
    """验证部署结果"""
    print("\n🔍 验证部署...")
    
    flows = client.list_flows()
    if flows:
        print(f"   ✅ Kestra 中共有 {len(flows)} 个工作流")
        for flow in flows[:5]:
            print(f"      - {flow.namespace}/{flow.id}")
        if len(flows) > 5:
            print(f"      ... 还有 {len(flows) - 5} 个")
        return True
    else:
        print("   ⚠️  Kestra 中没有找到工作流")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Kestra 工作流部署工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python kestra/deploy.py                    # 部署所有工作流
  python kestra/deploy.py --flow xcnstock_data_pipeline.yml
  python kestra/deploy.py --validate-only    # 仅验证 YAML
  python kestra/deploy.py --dry-run          # 模拟部署
        """
    )
    
    parser.add_argument(
        "--flow",
        type=str,
        help="部署单个工作流文件"
    )
    parser.add_argument(
        "--flows-dir",
        type=str,
        default=None,
        help="工作流目录 (默认: kestra/flows/)"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="仅验证 YAML 语法，不部署"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="模拟部署，不实际执行"
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="跳过部署后验证"
    )
    
    args = parser.parse_args()
    
    # 打印头部信息
    print("=" * 70)
    print("Kestra 工作流部署工具")
    print("=" * 70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 创建客户端
    client = create_client()
    print(f"API URL: {client.api_url}")
    print(f"Namespace: {os.getenv('KESTRA_NAMESPACE', 'xcnstock')}")
    print("=" * 70)
    
    # 测试连接
    print("\n🔌 测试 API 连接...")
    success, message = client.test_connection()
    if not success:
        print(f"❌ {message}")
        sys.exit(1)
    print(f"✅ {message}")
    
    # 确定工作流目录
    if args.flows_dir:
        flows_dir = Path(args.flows_dir)
    else:
        flows_dir = Path(__file__).parent / "flows"
    
    # 执行部署
    if args.flow:
        # 部署单个文件
        flow_file = flows_dir / args.flow
        if not flow_file.exists():
            # 尝试直接作为路径
            flow_file = Path(args.flow)
        
        success, message = deploy_single_flow(client, flow_file, args.dry_run)
        results = {flow_file.name: (success, message)}
    else:
        # 批量部署
        results = deploy_all_flows(client, flows_dir, args.dry_run)
    
    # 打印报告
    print_deploy_report(results)
    
    # 验证部署
    if not args.validate_only and not args.dry_run and not args.skip_verify:
        verify_deployments(client)
    
    # 打印 Web UI 地址
    web_url = os.getenv('KESTRA_WEB_URL', 'http://localhost:8082/ui/')
    print(f"\n🌐 Kestra Web UI: {web_url}")
    
    # 退出码
    if all(success for success, _ in results.values()):
        print("\n✅ 所有工作流部署成功")
        sys.exit(0)
    else:
        print("\n❌ 部分工作流部署失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
