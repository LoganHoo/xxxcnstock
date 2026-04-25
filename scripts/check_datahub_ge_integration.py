#!/usr/bin/env python3
"""
DataHub 与 Great Expectations 集成检查
"""
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))


def check_datahub_ge_integration():
    print('=' * 100)
    print('🔍 DataHub 与 Great Expectations 集成情况分析')
    print('=' * 100)
    print(f'检查时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # 1. 检查依赖安装
    print('=' * 100)
    print('📦 一、依赖安装检查')
    print('=' * 100)

    try:
        import subprocess
        result = subprocess.run(
            ['pip', 'list'],
            capture_output=True,
            text=True
        )
        installed_packages = result.stdout.lower()

        dependencies = {
            'acryl-datahub': 'acryl-datahub' in installed_packages,
            'acryl-datahub-classify': 'acryl-datahub-classify' in installed_packages,
            'acryl_great_expectations': 'acryl_great_expectations' in installed_packages,
            'great-expectations': 'great-expectations' in installed_packages,
        }

        for pkg, installed in dependencies.items():
            status = '✅' if installed else '❌'
            print(f'   {status} {pkg}')

        installed_count = sum(dependencies.values())
        print(f'\n   安装进度: {installed_count}/{len(dependencies)}')

    except Exception as e:
        print(f'   ❌ 检查失败: {e}')

    print()

    # 2. 检查 DataHub 服务
    print('=' * 100)
    print('🌐 二、DataHub 服务检查')
    print('=' * 100)

    import requests

    # 检查 localhost:9002
    services = {
        'DataHub UI (localhost:9002)': 'http://localhost:9002',
        'DataHub UI (192.168.1.168:9002)': 'http://192.168.1.168:9002',
        'DataHub GMS (localhost:8080)': 'http://localhost:8080',
        'DataHub GMS (192.168.1.168:8080)': 'http://192.168.1.168:8080',
    }

    for name, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f'   ✅ {name} - 可访问')
            else:
                print(f'   ⚠️  {name} - 状态码 {response.status_code}')
        except requests.exceptions.ConnectionError:
            print(f'   ❌ {name} - 无法连接')
        except Exception as e:
            print(f'   ❌ {name} - {str(e)[:50]}')

    print()

    # 3. 检查 DataHub 客户端模块
    print('=' * 100)
    print('📋 三、DataHub 客户端模块检查')
    print('=' * 100)

    modules = [
        'services.data_service.datahub_client',
        'services.data_service.datahub_integration',
        'services.metadata.datahub_client',
    ]

    for module_name in modules:
        try:
            __import__(module_name)
            print(f'   ✅ {module_name}')
        except ImportError as e:
            print(f'   ❌ {module_name} - {e}')
        except Exception as e:
            print(f'   ⚠️  {module_name} - {e}')

    print()

    # 4. 检查 GE 官方集成
    print('=' * 100)
    print('🔗 四、Great Expectations 官方集成检查')
    print('=' * 100)

    try:
        import great_expectations as ge
        print(f'   ✅ Great Expectations 已安装')
        print(f'   版本: {ge.__version__}')

        # 检查 DataHub 集成
        try:
            from great_expectations.checkpoint import Checkpoint
            print(f'   ✅ Checkpoint 可用')
        except ImportError:
            print(f'   ⚠️  Checkpoint 不可用')

        # 检查 acryl 集成
        try:
            import acryl_datahub
            print(f'   ✅ acryl-datahub 可用')
        except ImportError:
            print(f'   ⚠️  acryl-datahub 不可用')

    except ImportError:
        print(f'   ❌ Great Expectations 未安装')

    print()

    # 5. 检查 DataHub 配置
    print('=' * 100)
    print('⚙️  五、DataHub 配置检查')
    print('=' * 100)

    try:
        from services.data_service.datahub_client import DataHubConfig

        config = DataHubConfig()
        print(f'   GMS URL: {config.gms_url}')
        print(f'   UI URL: {config.ui_url}')
        print(f'   用户名: {config.username}')
        print(f'   环境: {config.env}')

        # 检查环境变量
        import os
        env_vars = ['DATAHUB_GMS_URL', 'DATAHUB_UI_URL', 'DATAHUB_TOKEN']
        print(f'\n   环境变量:')
        for var in env_vars:
            value = os.getenv(var)
            if value:
                print(f'      ✅ {var}: 已设置')
            else:
                print(f'      ⚠️  {var}: 未设置')

    except Exception as e:
        print(f'   ❌ 配置检查失败: {e}')

    print()

    # 6. 检查 GE 与 DataHub 集成配置
    print('=' * 100)
    print('🎯 六、GE-DataHub 集成配置检查')
    print('=' * 100)

    # 查找相关配置文件
    config_files = [
        'great_expectations/great_expectations.yml',
        'gx/great_expectations.yml',
        '.gx/great_expectations.yml',
    ]

    found_config = False
    for config_file in config_files:
        config_path = Path(config_file)
        if config_path.exists():
            found_config = True
            print(f'   ✅ 找到配置: {config_file}')
            # 读取配置内容
            try:
                with open(config_path, 'r') as f:
                    content = f.read()
                    if 'datahub' in content.lower():
                        print(f'      ✅ 包含 DataHub 集成配置')
                    else:
                        print(f'      ⚠️  未包含 DataHub 集成配置')
            except Exception as e:
                print(f'      ❌ 读取失败: {e}')

    if not found_config:
        print(f'   ⚠️  未找到 GE 配置文件')
        print(f'      建议运行: great_expectations init')

    print()

    # 7. 测试 DataHub 连接
    print('=' * 100)
    print('🧪 七、DataHub 连接测试')
    print('=' * 100)

    try:
        from services.metadata.datahub_client import DataHubClient, get_datahub_client

        client = get_datahub_client()
        health = client.health_check()

        print(f'   SDK 可用: {"✅" if health["sdk_available"] else "❌"}')
        print(f'   服务端点: {health["server_url"]}')
        print(f'   连接状态: {health.get("connection", "未配置")}')

    except Exception as e:
        print(f'   ❌ 连接测试失败: {e}')

    print()

    # 8. 生成集成建议
    print('=' * 100)
    print('💡 八、集成建议')
    print('=' * 100)

    suggestions = []

    # 检查是否需要启动 DataHub
    if 'localhost:9002' in str(services):
        suggestions.append('DataHub 服务未在 localhost:9002 运行，建议启动 DataHub')

    # 检查 GE 配置
    if not found_config:
        suggestions.append('Great Expectations 未初始化，建议运行: great_expectations init')

    # 检查环境变量
    if not os.getenv('DATAHUB_GMS_URL'):
        suggestions.append('建议设置 DATAHUB_GMS_URL 环境变量')

    if suggestions:
        for i, suggestion in enumerate(suggestions, 1):
            print(f'   {i}. {suggestion}')
    else:
        print('   ✅ 所有检查通过，无需额外配置')

    print()
    print('=' * 100)
    print('✅ DataHub 与 Great Expectations 集成检查完成')
    print('=' * 100)


if __name__ == '__main__':
    check_datahub_ge_integration()
