#!/usr/bin/env python3
"""
Kestra 工作流测试脚本

测试所有Kestra工作流的配置和可用性

使用方式:
    python kestra/test_workflows.py
    python kestra/test_workflows.py --test-connection
    python kestra/test_workflows.py --validate-flows
"""
import os
import sys
import argparse
import yaml
import json
from pathlib import Path
from typing import Dict, List, Tuple

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='测试Kestra工作流')
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='测试Kestra连接'
    )
    parser.add_argument(
        '--validate-flows',
        action='store_true',
        help='验证工作流配置'
    )
    parser.add_argument(
        '--test-all',
        action='store_true',
        help='运行所有测试'
    )
    return parser.parse_args()


def test_kestra_connection() -> Tuple[bool, Dict]:
    """测试Kestra连接"""
    print("=" * 60)
    print("🔌 测试Kestra连接")
    print("=" * 60)
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        import requests
        
        KESTRA_API_URL = os.getenv('KESTRA_API_URL', 'http://localhost:8082/api/v1')
        KESTRA_USERNAME = os.getenv('KESTRA_USERNAME', 'admin@kestra.io')
        KESTRA_PASSWORD = os.getenv('KESTRA_PASSWORD', 'Kestra123')
        
        print(f"API URL: {KESTRA_API_URL}")
        print(f"用户名: {KESTRA_USERNAME}")
        
        auth = (KESTRA_USERNAME, KESTRA_PASSWORD)
        
        # 测试API连接
        endpoints = [
            '/namespaces',
            '/flows',
            '/executions'
        ]
        
        results = {}
        for endpoint in endpoints:
            url = f"{KESTRA_API_URL}{endpoint}"
            try:
                response = requests.get(url, auth=auth, timeout=5)
                results[endpoint] = {
                    'status_code': response.status_code,
                    'success': response.status_code == 200
                }
                status = "✅" if response.status_code == 200 else "❌"
                print(f"  {status} {endpoint} -> {response.status_code}")
            except Exception as e:
                results[endpoint] = {
                    'error': str(e),
                    'success': False
                }
                print(f"  ❌ {endpoint} -> 错误: {e}")
        
        all_success = all(r.get('success', False) for r in results.values())
        
        if all_success:
            print("\n✅ Kestra连接测试通过")
        else:
            print("\n⚠️  Kestra连接测试部分失败")
        
        return all_success, results
        
    except Exception as e:
        print(f"\n❌ Kestra连接测试失败: {e}")
        return False, {'error': str(e)}


def validate_flow_file(flow_path: Path) -> Tuple[bool, List[str]]:
    """验证单个工作流文件"""
    errors = []
    
    try:
        with open(flow_path, 'r', encoding='utf-8') as f:
            flow = yaml.safe_load(f)
        
        # 检查必要字段
        if 'id' not in flow:
            errors.append("缺少 'id' 字段")
        if 'namespace' not in flow:
            errors.append("缺少 'namespace' 字段")
        if 'tasks' not in flow and 'triggers' not in flow:
            errors.append("缺少 'tasks' 或 'triggers' 字段")
        
        # 检查任务配置
        if 'tasks' in flow:
            for i, task in enumerate(flow['tasks']):
                if 'id' not in task:
                    errors.append(f"任务 {i} 缺少 'id'")
                if 'type' not in task:
                    errors.append(f"任务 {task.get('id', i)} 缺少 'type'")
        
        return len(errors) == 0, errors
        
    except yaml.YAMLError as e:
        return False, [f"YAML解析错误: {e}"]
    except Exception as e:
        return False, [f"验证错误: {e}"]


def validate_all_flows() -> Tuple[bool, Dict]:
    """验证所有工作流配置"""
    print("\n" + "=" * 60)
    print("📋 验证工作流配置")
    print("=" * 60)
    
    flows_dir = Path('kestra/flows')
    if not flows_dir.exists():
        return False, {'error': '工作流目录不存在'}
    
    flow_files = list(flows_dir.glob('*.yml'))
    
    print(f"发现 {len(flow_files)} 个工作流文件")
    
    results = {}
    valid_count = 0
    invalid_count = 0
    
    for flow_file in sorted(flow_files):
        valid, errors = validate_flow_file(flow_file)
        
        results[flow_file.name] = {
            'valid': valid,
            'errors': errors
        }
        
        status = "✅" if valid else "❌"
        print(f"\n{status} {flow_file.name}")
        
        if errors:
            for error in errors:
                print(f"   - {error}")
        
        if valid:
            valid_count += 1
        else:
            invalid_count += 1
    
    print(f"\n{'=' * 60}")
    print(f"有效: {valid_count} | 无效: {invalid_count}")
    
    all_valid = invalid_count == 0
    
    if all_valid:
        print("✅ 所有工作流配置有效")
    else:
        print(f"⚠️  {invalid_count} 个工作流配置有问题")
    
    return all_valid, results


def analyze_flow_dependencies() -> Dict:
    """分析工作流依赖关系"""
    print("\n" + "=" * 60)
    print("🔗 分析工作流依赖")
    print("=" * 60)
    
    flows_dir = Path('kestra/flows')
    flow_files = list(flows_dir.glob('*.yml'))
    
    dependencies = {}
    
    for flow_file in flow_files:
        try:
            with open(flow_file, 'r', encoding='utf-8') as f:
                content = f.read()
                flow = yaml.safe_load(content)
            
            flow_id = flow.get('id', flow_file.stem)
            
            # 分析依赖
            deps = []
            
            # 检查是否引用其他工作流
            if 'tasks' in flow:
                for task in flow['tasks']:
                    task_type = task.get('type', '')
                    
                    # Flow触发器
                    if 'Flow' in task_type and 'flowId' in task:
                        deps.append(task['flowId'])
                    
                    # 子流程
                    if task_type == 'io.kestra.core.tasks.flows.Subflow':
                        deps.append(task.get('flowId', 'unknown'))
            
            # 检查触发器
            if 'triggers' in flow:
                for trigger in flow['triggers']:
                    trigger_type = trigger.get('type', '')
                    if 'Flow' in trigger_type:
                        deps.append(trigger.get('flowId', 'unknown'))
            
            dependencies[flow_id] = {
                'file': flow_file.name,
                'dependencies': list(set(deps))
            }
            
        except Exception as e:
            dependencies[flow_file.stem] = {
                'file': flow_file.name,
                'error': str(e)
            }
    
    # 打印依赖关系
    for flow_id, info in dependencies.items():
        print(f"\n{flow_id}")
        print(f"  文件: {info.get('file', 'N/A')}")
        if 'dependencies' in info and info['dependencies']:
            print(f"  依赖: {', '.join(info['dependencies'])}")
        if 'error' in info:
            print(f"  错误: {info['error']}")
    
    return dependencies


def generate_test_report(connection_result: Tuple[bool, Dict], 
                         validation_result: Tuple[bool, Dict],
                         dependencies: Dict) -> Path:
    """生成测试报告"""
    report_dir = Path('data/test_reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
    
    report = {
        'tested_at': __import__('datetime').datetime.now().isoformat(),
        'connection': {
            'success': connection_result[0],
            'details': connection_result[1]
        },
        'validation': {
            'success': validation_result[0],
            'details': validation_result[1]
        },
        'dependencies': dependencies
    }
    
    # JSON报告
    json_path = report_dir / f'kestra_test_{timestamp}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # Markdown报告
    md_path = report_dir / f'kestra_test_{timestamp}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 工作流测试报告\n\n')
        f.write(f'测试时间: {__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        # 连接测试
        f.write('## Kestra连接测试\n\n')
        conn_success = connection_result[0]
        f.write(f'状态: {"✅ 通过" if conn_success else "❌ 失败"}\n\n')
        
        if isinstance(connection_result[1], dict):
            for endpoint, result in connection_result[1].items():
                status = "✅" if result.get('success') else "❌"
                f.write(f'- {status} {endpoint}\n')
        
        # 工作流验证
        f.write('\n## 工作流配置验证\n\n')
        val_success = validation_result[0]
        f.write(f'状态: {"✅ 全部有效" if val_success else "⚠️ 部分无效"}\n\n')
        
        if isinstance(validation_result[1], dict):
            for flow_name, result in validation_result[1].items():
                status = "✅" if result.get('valid') else "❌"
                f.write(f'- {status} {flow_name}\n')
                if result.get('errors'):
                    for error in result['errors']:
                        f.write(f'  - {error}\n')
        
        # 依赖关系
        f.write('\n## 工作流依赖关系\n\n')
        for flow_id, info in dependencies.items():
            f.write(f'### {flow_id}\n')
            f.write(f'- 文件: {info.get("file", "N/A")}\n')
            if info.get('dependencies'):
                f.write(f'- 依赖: {", ".join(info["dependencies"])}\n')
            f.write('\n')
    
    return md_path


def main():
    """主函数"""
    args = parse_args()
    
    print("=" * 60)
    print("🚀 Kestra 工作流测试")
    print("=" * 60)
    
    # 如果没有指定参数，运行所有测试
    if not any([args.test_connection, args.validate_flows, args.test_all]):
        args.test_all = True
    
    connection_result = (False, {})
    validation_result = (False, {})
    dependencies = {}
    
    # 测试连接
    if args.test_connection or args.test_all:
        connection_result = test_kestra_connection()
    
    # 验证工作流
    if args.validate_flows or args.test_all:
        validation_result = validate_all_flows()
        dependencies = analyze_flow_dependencies()
    
    # 生成报告
    if args.test_all:
        report_path = generate_test_report(connection_result, validation_result, dependencies)
        print(f"\n📄 测试报告已保存: {report_path}")
    
    # 输出摘要
    print("\n" + "=" * 60)
    print("📊 测试摘要")
    print("=" * 60)
    
    if args.test_connection or args.test_all:
        conn_status = "✅ 通过" if connection_result[0] else "❌ 失败"
        print(f"Kestra连接: {conn_status}")
    
    if args.validate_flows or args.test_all:
        val_status = "✅ 全部有效" if validation_result[0] else "⚠️ 部分无效"
        print(f"工作流配置: {val_status}")
    
    # 返回退出码
    all_passed = True
    if args.test_connection or args.test_all:
        all_passed = all_passed and connection_result[0]
    if args.validate_flows or args.test_all:
        all_passed = all_passed and validation_result[0]
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
