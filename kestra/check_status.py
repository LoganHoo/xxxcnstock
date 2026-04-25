#!/usr/bin/env python3
"""
Kestra 状态检查脚本

检查Kestra服务状态、工作流配置和可执行性

使用方式:
    python kestra/check_status.py
"""
import os
import sys
import requests
import yaml
from pathlib import Path
from datetime import datetime

# 加载环境变量
from dotenv import load_dotenv
load_dotenv()

# 配置
KESTRA_API_URL = os.getenv('KESTRA_API_URL', 'http://localhost:8082/api/v1')
KESTRA_WEB_URL = os.getenv('KESTRA_WEB_URL', 'http://localhost:8082/ui/')
KESTRA_USERNAME = os.getenv('KESTRA_USERNAME', 'admin@kestra.io')
KESTRA_PASSWORD = os.getenv('KESTRA_PASSWORD', 'Kestra123')
KESTRA_NAMESPACE = os.getenv('KESTRA_NAMESPACE', 'xcnstock')


def check_service_status():
    """检查Kestra服务状态"""
    print("=" * 70)
    print("🔍 Kestra 服务状态检查")
    print("=" * 70)
    
    print(f"\n配置信息:")
    print(f"  API URL: {KESTRA_API_URL}")
    print(f"  Web URL: {KESTRA_WEB_URL}")
    print(f"  用户名: {KESTRA_USERNAME}")
    print(f"  命名空间: {KESTRA_NAMESPACE}")
    
    print(f"\n连接测试:")
    
    # 测试API
    try:
        response = requests.get(
            f"{KESTRA_API_URL}/namespaces",
            auth=(KESTRA_USERNAME, KESTRA_PASSWORD),
            timeout=5
        )
        if response.status_code == 200:
            print(f"  ✅ API服务正常 (HTTP 200)")
            return True
        else:
            print(f"  ❌ API服务异常 (HTTP {response.status_code})")
            return False
    except requests.exceptions.ConnectionError:
        print(f"  ❌ 无法连接到Kestra服务")
        print(f"     请检查:")
        print(f"     1. Kestra服务是否已启动")
        print(f"     2. 端口配置是否正确 (当前: {KESTRA_API_URL})")
        return False
    except Exception as e:
        print(f"  ❌ 连接错误: {e}")
        return False


def check_workflows():
    """检查工作流配置"""
    print("\n" + "=" * 70)
    print("📋 工作流配置检查")
    print("=" * 70)
    
    flows_dir = Path('kestra/flows')
    if not flows_dir.exists():
        print("❌ 工作流目录不存在")
        return []
    
    flow_files = list(flows_dir.glob('*.yml'))
    print(f"\n发现 {len(flow_files)} 个工作流文件")
    
    valid_flows = []
    invalid_flows = []
    
    for flow_file in sorted(flow_files):
        try:
            with open(flow_file, 'r', encoding='utf-8') as f:
                flow = yaml.safe_load(f)
            
            flow_id = flow.get('id', 'unknown')
            namespace = flow.get('namespace', 'unknown')
            tasks = len(flow.get('tasks', []))
            triggers = len(flow.get('triggers', []))
            
            print(f"\n  ✅ {flow_file.name}")
            print(f"     ID: {flow_id}")
            print(f"     命名空间: {namespace}")
            print(f"     任务数: {tasks}")
            print(f"     触发器: {triggers}")
            
            valid_flows.append({
                'file': flow_file.name,
                'id': flow_id,
                'namespace': namespace
            })
            
        except yaml.YAMLError as e:
            print(f"\n  ❌ {flow_file.name}")
            print(f"     YAML错误: {e}")
            invalid_flows.append(flow_file.name)
        except Exception as e:
            print(f"\n  ❌ {flow_file.name}")
            print(f"     错误: {e}")
            invalid_flows.append(flow_file.name)
    
    print(f"\n{'=' * 70}")
    print(f"有效: {len(valid_flows)} | 无效: {len(invalid_flows)}")
    
    return valid_flows


def check_scripts():
    """检查脚本存在性"""
    print("\n" + "=" * 70)
    print("🐍 脚本依赖检查")
    print("=" * 70)
    
    # 从工作流中提取引用的脚本
    flows_dir = Path('kestra/flows')
    flow_files = list(flows_dir.glob('*.yml'))
    
    all_scripts = set()
    
    for flow_file in flow_files:
        try:
            with open(flow_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取脚本引用
            lines = content.split('\n')
            for line in lines:
                if 'from scripts' in line and 'import' in line:
                    parts = line.strip().split()
                    if 'from' in parts:
                        idx = parts.index('from')
                        if idx + 1 < len(parts):
                            module = parts[idx + 1]
                            if module.startswith('scripts'):
                                all_scripts.add(module)
        except:
            pass
    
    print(f"\n发现 {len(all_scripts)} 个脚本引用")
    
    existing = 0
    missing = 0
    
    for module in sorted(all_scripts):
        # 转换为文件路径
        parts = module.split('.')
        file_path = Path(*parts).with_suffix('.py')
        full_path = Path(file_path)
        
        if full_path.exists():
            print(f"  ✅ {module}")
            existing += 1
        else:
            print(f"  ❌ {module} (文件不存在)")
            missing += 1
    
    print(f"\n存在: {existing} | 缺失: {missing}")
    
    return missing == 0


def generate_status_report(service_ok, flows, scripts_ok):
    """生成状态报告"""
    report_dir = Path('data/test_reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Markdown报告
    md_path = report_dir / f'kestra_status_{timestamp}.md'
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('# Kestra 状态检查报告\n\n')
        f.write(f'检查时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        # 服务状态
        f.write('## 服务状态\n\n')
        if service_ok:
            f.write('✅ **Kestra服务运行正常**\n\n')
        else:
            f.write('❌ **Kestra服务未运行**\n\n')
            f.write('**可能原因:**\n')
            f.write('- Kestra服务未启动\n')
            f.write('- 端口配置错误\n')
            f.write('- 网络连接问题\n\n')
            f.write('**启动命令:**\n')
            f.write('```bash\n')
            f.write('# 使用Docker Compose启动Kestra\n')
            f.write('docker-compose -f docker-compose.kestra.yml up -d\n')
            f.write('```\n\n')
        
        # 工作流状态
        f.write('## 工作流配置\n\n')
        f.write(f'工作流数量: {len(flows)}\n\n')
        for flow in flows:
            f.write(f'- ✅ {flow["file"]} ({flow["id"]})\n')
        
        # 脚本状态
        f.write('\n## 脚本依赖\n\n')
        if scripts_ok:
            f.write('✅ **所有脚本存在**\n')
        else:
            f.write('❌ **部分脚本缺失**\n')
        
        # 总体结论
        f.write('\n## 结论\n\n')
        if service_ok and flows and scripts_ok:
            f.write('✅ **Kestra完全正常，可以执行工作流**\n')
        elif not service_ok:
            f.write('⚠️ **Kestra服务未运行，需要启动服务**\n')
        else:
            f.write('⚠️ **配置有问题，需要修复**\n')
    
    return md_path


def main():
    """主函数"""
    print("\n" + "=" * 70)
    print("🚀 Kestra 状态检查")
    print("=" * 70)
    
    # 检查服务状态
    service_ok = check_service_status()
    
    # 检查工作流
    flows = check_workflows()
    
    # 检查脚本
    scripts_ok = check_scripts()
    
    # 生成报告
    report_path = generate_status_report(service_ok, flows, scripts_ok)
    
    # 输出摘要
    print("\n" + "=" * 70)
    print("📊 检查摘要")
    print("=" * 70)
    print(f"服务状态: {'✅ 正常' if service_ok else '❌ 未运行'}")
    print(f"工作流配置: {'✅ 有效' if flows else '❌ 无效'} ({len(flows)}个)")
    print(f"脚本依赖: {'✅ 完整' if scripts_ok else '❌ 缺失'}")
    
    print(f"\n📄 报告已保存: {report_path}")
    
    # 如果服务未运行，提供启动建议
    if not service_ok:
        print("\n" + "=" * 70)
        print("💡 启动Kestra服务")
        print("=" * 70)
        print("""
Kestra服务未运行，您可以选择以下方式启动:

方式1: 使用Docker Compose（推荐）
    # 创建 docker-compose.kestra.yml
    docker-compose -f docker-compose.kestra.yml up -d

方式2: 使用Kestra CLI
    # 下载Kestra
    curl -o kestra https://raw.githubusercontent.com/kestra-io/kestra/develop/kestra
    chmod +x kestra
    
    # 启动服务
    ./kestra server local

方式3: 使用Docker直接运行
    docker run -d --name kestra \\
        -p 8082:8080 \\
        -v $(pwd)/kestra/flows:/app/flows \\
        kestra/kestra:latest server local

注意: Kestra需要MySQL或H2数据库，详细配置请参考Kestra文档。
        """)
    
    # 返回状态码
    return 0 if service_ok else 1


if __name__ == '__main__':
    sys.exit(main())
