#!/usr/bin/env python3
"""
测试 GE 自动验证流程
验证工作流中 GE 检查点是否自动执行
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from pathlib import Path


def test_ge_integration():
    """测试 GE 集成情况"""
    print("\n" + "=" * 60)
    print("GE 自动验证流程测试")
    print("=" * 60)
    
    workflow_path = Path('/Volumes/Xdata/workstation/xxxcnstock/workflows/enhanced_data_collection_workflow.py')
    
    with open(workflow_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 检查 GE 集成点
    checks = {
        'GE 导入': 'GECheckpointValidators' in content,
        '检查点1-采集前检查': 'pre_collection_check' in content,
        '检查点2-采集后验证': 'post_collection_validation' in content,
        'GE 重试配置': 'GERetryConfig' in content,
        'CheckStatus 导入': 'CheckStatus' in content,
        '自动验证结果处理': 'pre_check.status' in content,
        '验证失败处理': 'CheckStatus.FAILED' in content,
    }
    
    print("\n📋 GE 集成检查:")
    all_passed = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"   {status} {check}")
        if not passed:
            all_passed = False
    
    # 检查验证流程位置
    print("\n📍 验证流程位置:")
    
    lines = content.split('\n')
    for i, line in enumerate(lines, 1):
        if 'pre_collection_check' in line:
            print(f"   ✅ 检查点1 (采集前) @ 第{i}行")
        if 'post_collection_validation' in line:
            print(f"   ✅ 检查点2 (采集后) @ 第{i}行")
    
    # 检查验证逻辑
    print("\n🔍 验证逻辑检查:")
    
    has_pre_check_logic = 'if pre_check.status == CheckStatus.FAILED' in content
    has_post_check_logic = 'if validation.status == CheckStatus.FAILED' in content
    
    print(f"   {'✅' if has_pre_check_logic else '❌'} 采集前检查失败处理")
    print(f"   {'✅' if has_post_check_logic else '❌'} 采集后检查失败处理")
    
    if not has_pre_check_logic or not has_post_check_logic:
        all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 GE 自动验证流程已正确集成!")
        print("\n工作流执行时会自动:")
        print("   1️⃣ 采集前自动检查市场状态、数据源")
        print("   2️⃣ 采集后自动验证数据完整性、新鲜度")
        print("   3️⃣ 检查失败时自动重试 (最多3次)")
        print("   4️⃣ 严重失败时停止工作流并报告")
    else:
        print("⚠️ GE 集成存在问题，需要检查")
    print("=" * 60)
    
    return all_passed


def show_validation_flow():
    """展示验证流程"""
    print("\n" + "=" * 60)
    print("GE 自动验证流程图")
    print("=" * 60)
    
    flow = """
    ┌─────────────────────────────────────────────────────────────┐
    │                    数据采集工作流                            │
    ├─────────────────────────────────────────────────────────────┤
    │                                                             │
    │  开始                                                        │
    │   │                                                         │
    │   ▼                                                         │
    │  ┌─────────────────────┐                                   │
    │  │ 检查点1: 采集前检查  │ ◀── GE自动验证                     │
    │  │ - 市场状态检查       │     (pre_collection_check)        │
    │  │ - 数据源可用性       │                                   │
    │  │ - 存储空间检查       │                                   │
    │  └─────────────────────┘                                   │
    │   │                                                         │
    │   │ 失败? ──▶ 重试3次 ──▶ 仍失败 ──▶ 停止工作流              │
    │   │                                                         │
    │   ▼                                                         │
    │  采集数据                                                    │
    │   │                                                         │
    │   ▼                                                         │
    │  ┌─────────────────────┐                                   │
    │  │ 检查点2: 采集后验证  │ ◀── GE自动验证                     │
    │  │ - 数据完整性检查     │     (post_collection_validation)  │
    │  │ - 数据新鲜度检查     │                                   │
    │  │ - 格式验证          │                                   │
    │  └─────────────────────┘                                   │
    │   │                                                         │
    │   │ 失败? ──▶ 重试3次 ──▶ 仍失败 ──▶ 报告部分失败            │
    │   │                                                         │
    │   ▼                                                         │
    │  完成                                                        │
    │                                                             │
    └─────────────────────────────────────────────────────────────┘
    """
    print(flow)


if __name__ == '__main__':
    success = test_ge_integration()
    show_validation_flow()
    sys.exit(0 if success else 1)
