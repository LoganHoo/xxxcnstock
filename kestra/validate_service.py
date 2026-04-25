#!/usr/bin/env python3
"""
Kestra 服务验证脚本

验证 .env 中的Kestra配置并测试服务连接

使用方式:
    python kestra/validate_service.py
"""
import os
import sys
import re
import socket
import requests
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


def validate_url(url: str, name: str) -> tuple:
    """验证URL格式"""
    try:
        parsed = urlparse(url)
        if not parsed.scheme:
            return False, f"{name}缺少协议 (http/https)"
        if not parsed.netloc:
            return False, f"{name}格式错误"
        if parsed.scheme not in ['http', 'https']:
            return False, f"{name}协议必须是http或https"
        return True, "格式正确"
    except Exception as e:
        return False, f"{name}解析错误: {e}"


def validate_username(username: str) -> tuple:
    """验证用户名格式"""
    if not username:
        return False, "用户名不能为空"
    if '@' not in username:
        return False, "用户名应该是邮箱格式"
    return True, "格式正确"


def validate_password(password: str) -> tuple:
    """验证密码"""
    if not password:
        return False, "密码不能为空"
    if len(password) < 6:
        return False, "密码长度应该至少6位"
    return True, "格式正确"


def validate_namespace(namespace: str) -> tuple:
    """验证命名空间"""
    if not namespace:
        return False, "命名空间不能为空"
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', namespace):
        return False, "命名空间格式错误 (应以字母开头，包含字母数字下划线)"
    return True, "格式正确"


def check_port_available(host: str, port: int) -> tuple:
    """检查端口是否可用"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return True, "端口可连接"
        else:
            return False, f"端口无法连接 (错误码: {result})"
    except Exception as e:
        return False, f"连接测试失败: {e}"


def test_kestra_connection():
    """测试Kestra连接"""
    print("\n" + "=" * 70)
    print("🔗 Kestra服务连接测试")
    print("=" * 70)
    
    # 获取配置
    api_url = os.getenv('KESTRA_API_URL', '')
    username = os.getenv('KESTRA_USERNAME', '')
    password = os.getenv('KESTRA_PASSWORD', '')
    
    if not api_url:
        print("❌ KESTRA_API_URL 未配置")
        return False
    
    # 解析URL获取主机和端口
    try:
        parsed = urlparse(api_url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 80
        
        print(f"\n目标: {host}:{port}")
        
        # 测试端口连通性
        port_ok, port_msg = check_port_available(host, port)
        print(f"端口连通性: {'✅' if port_ok else '❌'} {port_msg}")
        
        if not port_ok:
            return False
        
        # 测试API
        print(f"\nAPI测试:")
        print(f"  URL: {api_url}")
        print(f"  用户名: {username}")
        
        try:
            response = requests.get(
                f"{api_url}/namespaces",
                auth=(username, password),
                timeout=5
            )
            
            if response.status_code == 200:
                print(f"  状态: ✅ 连接成功 (HTTP 200)")
                try:
                    data = response.json()
                    namespaces = data.get('results', [])
                    print(f"  命名空间数量: {len(namespaces)}")
                    for ns in namespaces[:5]:
                        print(f"    - {ns.get('id', 'unknown')}")
                except:
                    pass
                return True
            elif response.status_code == 401:
                print(f"  状态: ❌ 认证失败 (HTTP 401)")
                print(f"  提示: 请检查用户名和密码")
                return False
            elif response.status_code == 404:
                print(f"  状态: ❌ API路径不存在 (HTTP 404)")
                print(f"  提示: Kestra服务可能未完全启动")
                return False
            else:
                print(f"  状态: ❌ 错误 (HTTP {response.status_code})")
                print(f"  响应: {response.text[:200]}")
                return False
                
        except requests.exceptions.ConnectionError:
            print(f"  状态: ❌ 连接被拒绝")
            print(f"  提示: Kestra服务未运行")
            return False
        except requests.exceptions.Timeout:
            print(f"  状态: ❌ 连接超时")
            return False
        except Exception as e:
            print(f"  状态: ❌ 请求错误: {e}")
            return False
            
    except Exception as e:
        print(f"❌ URL解析错误: {e}")
        return False


def validate_env_config():
    """验证环境变量配置"""
    print("=" * 70)
    print("📋 环境变量配置验证")
    print("=" * 70)
    
    # 获取配置
    config = {
        'KESTRA_API_URL': os.getenv('KESTRA_API_URL', ''),
        'KESTRA_WEB_URL': os.getenv('KESTRA_WEB_URL', ''),
        'KESTRA_USERNAME': os.getenv('KESTRA_USERNAME', ''),
        'KESTRA_PASSWORD': os.getenv('KESTRA_PASSWORD', ''),
        'KESTRA_NAMESPACE': os.getenv('KESTRA_NAMESPACE', '')
    }
    
    # 检查是否配置
    print("\n配置项检查:")
    all_configured = True
    for key, value in config.items():
        if value:
            # 隐藏密码
            display_value = value if 'PASSWORD' not in key else '*' * len(value)
            print(f"  ✅ {key}: {display_value}")
        else:
            print(f"  ❌ {key}: 未配置")
            all_configured = False
    
    if not all_configured:
        print("\n⚠️  部分配置项未设置")
        return False
    
    # 验证各项格式
    print("\n格式验证:")
    results = []
    
    # API URL
    ok, msg = validate_url(config['KESTRA_API_URL'], 'API URL')
    results.append(('API URL', ok, msg))
    print(f"  {'✅' if ok else '❌'} API URL: {msg}")
    
    # Web URL
    ok, msg = validate_url(config['KESTRA_WEB_URL'], 'Web URL')
    results.append(('Web URL', ok, msg))
    print(f"  {'✅' if ok else '❌'} Web URL: {msg}")
    
    # 用户名
    ok, msg = validate_username(config['KESTRA_USERNAME'])
    results.append(('用户名', ok, msg))
    print(f"  {'✅' if ok else '❌'} 用户名: {msg}")
    
    # 密码
    ok, msg = validate_password(config['KESTRA_PASSWORD'])
    results.append(('密码', ok, msg))
    print(f"  {'✅' if ok else '❌'} 密码: {msg}")
    
    # 命名空间
    ok, msg = validate_namespace(config['KESTRA_NAMESPACE'])
    results.append(('命名空间', ok, msg))
    print(f"  {'✅' if ok else '❌'} 命名空间: {msg}")
    
    all_valid = all(r[1] for r in results)
    
    if all_valid:
        print("\n✅ 所有配置项格式正确")
    else:
        print("\n❌ 部分配置项格式错误")
    
    return all_valid


def check_workflows():
    """检查工作流配置"""
    print("\n" + "=" * 70)
    print("📁 工作流配置检查")
    print("=" * 70)
    
    flows_dir = Path('kestra/flows')
    if not flows_dir.exists():
        print("❌ 工作流目录不存在")
        return 0
    
    flow_files = list(flows_dir.glob('*.yml'))
    print(f"\n工作流文件数量: {len(flow_files)}")
    
    namespace = os.getenv('KESTRA_NAMESPACE', 'xcnstock')
    matching = 0
    
    for flow_file in sorted(flow_files):
        try:
            import yaml
            with open(flow_file, 'r', encoding='utf-8') as f:
                flow = yaml.safe_load(f)
            
            flow_id = flow.get('id', 'unknown')
            flow_ns = flow.get('namespace', 'unknown')
            tasks = len(flow.get('tasks', []))
            
            ns_match = "✅" if flow_ns == namespace else "⚠️"
            if flow_ns == namespace:
                matching += 1
            
            print(f"  {ns_match} {flow_file.name}")
            print(f"     ID: {flow_id}")
            print(f"     命名空间: {flow_ns}")
            print(f"     任务数: {tasks}")
            
        except Exception as e:
            print(f"  ❌ {flow_file.name}: 解析错误 - {e}")
    
    print(f"\n命名空间匹配: {matching}/{len(flow_files)}")
    
    return len(flow_files)


def generate_report(config_valid, service_ok, workflow_count):
    """生成验证报告"""
    from datetime import datetime
    
    report_dir = Path('data/test_reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 获取配置
    config = {
        'API URL': os.getenv('KESTRA_API_URL', '未配置'),
        'Web URL': os.getenv('KESTRA_WEB_URL', '未配置'),
        '用户名': os.getenv('KESTRA_USERNAME', '未配置'),
        '命名空间': os.getenv('KESTRA_NAMESPACE', '未配置')
    }
    
    md_path = report_dir / f'kestra_service_validation_{timestamp}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 服务验证报告\n\n')
        f.write(f'验证时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        # 配置信息
        f.write('## 环境变量配置\n\n')
        for key, value in config.items():
            f.write(f'- **{key}**: {value}\n')
        
        # 验证结果
        f.write('\n## 验证结果\n\n')
        f.write(f'| 检查项 | 状态 |\n')
        f.write(f'|--------|------|\n')
        f.write(f'| 配置格式 | {"✅ 通过" if config_valid else "❌ 失败"} |\n')
        f.write(f'| 服务连接 | {"✅ 正常" if service_ok else "❌ 失败"} |\n')
        f.write(f'| 工作流配置 | ✅ {workflow_count}个 |\n')
        
        # 结论
        f.write('\n## 结论\n\n')
        if config_valid and service_ok:
            f.write('✅ **Kestra服务验证通过，可以正常使用**\n')
        elif config_valid and not service_ok:
            f.write('⚠️ **配置正确，但服务未运行**\n\n')
            f.write('**启动命令:**\n')
            f.write('```bash\n')
            f.write('# Docker方式启动\n')
            f.write('docker run -d --name kesta -p 8082:8080 kestra/kestra:latest server local\n')
            f.write('```\n')
        else:
            f.write('❌ **配置有误，请检查.env文件**\n')
    
    return md_path


def main():
    """主函数"""
    print("=" * 70)
    print("🚀 Kestra 服务验证")
    print("=" * 70)
    
    # 验证配置
    config_valid = validate_env_config()
    
    # 检查工作流
    workflow_count = check_workflows()
    
    # 测试连接（仅在配置正确时）
    service_ok = False
    if config_valid:
        service_ok = test_kestra_connection()
    
    # 生成报告
    report_path = generate_report(config_valid, service_ok, workflow_count)
    
    # 输出摘要
    print("\n" + "=" * 70)
    print("📊 验证摘要")
    print("=" * 70)
    print(f"配置格式: {'✅ 通过' if config_valid else '❌ 失败'}")
    print(f"服务连接: {'✅ 正常' if service_ok else '❌ 失败'}")
    print(f"工作流配置: ✅ {workflow_count}个")
    print(f"\n📄 报告已保存: {report_path}")
    
    # 结论
    print("\n" + "=" * 70)
    if config_valid and service_ok:
        print("✅ Kestra服务验证通过")
        return 0
    elif config_valid and not service_ok:
        print("⚠️  配置正确，但服务未运行")
        print("   请启动Kestra服务后再测试")
        return 1
    else:
        print("❌ 配置有误，请检查.env文件")
        return 1


if __name__ == '__main__':
    sys.exit(main())
