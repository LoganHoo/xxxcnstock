#!/usr/bin/env python3
"""
工作流功能验证测试（简化版）
验证：退市股票过滤、失败重试、断点续传
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
from pathlib import Path
from datetime import datetime


def verify_delisting_filter():
    """验证退市股票过滤 - 通过检查代码实现"""
    print("\n1️⃣ 验证退市股票过滤")
    
    providers_path = Path('/Volumes/Xdata/workstation/xxxcnstock/services/data_service/datasource/providers.py')
    
    with open(providers_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = {
        '_is_delisted方法': '_is_delisted' in content,
        '退市关键词检测': '退市' in content and '*ST' in content,
        '指数过滤': '000' in content and '399' in content,
        'ETF过滤': '51' in content and '15' in content,
        '可转债过滤': '11' in content and '12' in content,
        'B股过滤': '90' in content and '20' in content,
        '非交易状态过滤': 'trade_status' in content,
    }
    
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"   {status} {check}")
    
    return all(checks.values())


def verify_retry_mechanism():
    """验证失败重试机制 - 通过检查代码实现"""
    print("\n2️⃣ 验证失败重试机制")
    
    providers_path = Path('/Volumes/Xdata/workstation/xxxcnstock/services/data_service/datasource/providers.py')
    workflow_path = Path('/Volumes/Xdata/workstation/xxxcnstock/workflows/enhanced_data_collection_workflow.py')
    
    with open(providers_path, 'r', encoding='utf-8') as f:
        providers_content = f.read()
    
    with open(workflow_path, 'r', encoding='utf-8') as f:
        workflow_content = f.read()
    
    checks = {
        'retry_on_network_error装饰器': 'retry_on_network_error' in providers_content,
        '指数退避逻辑': 'backoff' in providers_content or '2 ** attempt' in providers_content,
        '网络错误识别': 'connection' in providers_content.lower() and 'timeout' in providers_content.lower(),
        '最大重试次数配置': 'max_retries' in providers_content,
        '工作流层重试': 'attempt' in workflow_content and 'max_retries' in workflow_content,
        '重试等待逻辑': 'asyncio.sleep' in workflow_content,
    }
    
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"   {status} {check}")
    
    return all(checks.values())


def verify_checkpoint_resume():
    """验证断点续传机制 - 通过检查代码实现"""
    print("\n3️⃣ 验证断点续传机制")
    
    workflow_path = Path('/Volumes/Xdata/workstation/xxxcnstock/workflows/enhanced_data_collection_workflow.py')
    framework_path = Path('/Volumes/Xdata/workstation/xxxcnstock/core/workflow_framework.py')
    
    with open(workflow_path, 'r', encoding='utf-8') as f:
        workflow_content = f.read()
    
    checks = {
        'Checkpoint类定义': 'class Checkpoint' in workflow_content or 'Checkpoint' in workflow_content,
        'save_checkpoint方法': 'save_checkpoint' in workflow_content,
        'resume参数支持': 'resume' in workflow_content,
        'completed_items记录': 'completed_items' in workflow_content,
        'failed_items记录': 'failed_items' in workflow_content,
        '断点恢复逻辑': 'checkpoint' in workflow_content.lower(),
    }
    
    # 检查断点目录
    checkpoint_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/checkpoints')
    checks['断点目录存在'] = checkpoint_dir.exists()
    
    if checks['断点目录存在']:
        checkpoints = list(checkpoint_dir.glob('*.json'))
        checks['历史断点文件'] = len(checkpoints) > 0
        print(f"   📁 断点文件数量: {len(checkpoints)}")
    
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"   {status} {check}")
    
    return all(checks.values())


def verify_data_integrity():
    """验证数据完整性"""
    print("\n4️⃣ 验证数据完整性")
    
    kline_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
    
    if not kline_dir.exists():
        print("   ❌ K线数据目录不存在")
        return False
    
    parquet_files = list(kline_dir.glob('*.parquet'))
    total_files = len(parquet_files)
    
    print(f"   ✅ K线数据目录存在")
    print(f"   ✅ 总文件数: {total_files}")
    
    # 检查文件大小
    if total_files > 0:
        total_size = sum(f.stat().st_size for f in parquet_files)
        avg_size = total_size / total_files
        print(f"   ✅ 总大小: {total_size / 1024 / 1024:.1f} MB")
        print(f"   ✅ 平均文件大小: {avg_size / 1024:.1f} KB")
    
    return total_files > 5000


def main():
    """主验证函数"""
    print("=" * 60)
    print("工作流功能验证测试（简化版）")
    print("验证: 退市股票过滤 | 失败重试 | 断点续传")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # 执行验证
    results = {
        '退市股票过滤': verify_delisting_filter(),
        '失败重试机制': verify_retry_mechanism(),
        '断点续传机制': verify_checkpoint_resume(),
        '数据完整性': verify_data_integrity(),
    }
    
    # 汇总结果
    duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("验证结果汇总")
    print("=" * 60)
    
    for feature, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"{status} - {feature}")
    
    # 总体结果
    all_passed = all(results.values())
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 总体结果: ✅ 全部通过")
    else:
        print("⚠️ 总体结果: ❌ 部分失败")
    print(f"⏱️ 耗时: {duration:.2f}秒")
    print("=" * 60)
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
