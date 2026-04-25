#!/usr/bin/env python3
"""
工作流迁移验证脚本

功能：
1. 验证新工作流是否正确部署
2. 检查旧工作流触发器是否已禁用
3. 生成迁移状态报告
4. 支持一键回滚
5. 健康检查所有工作流

使用方法：
    python scripts/migrate_workflows.py [command]

命令：
    verify      - 验证迁移状态
    rollback    - 回滚到旧工作流
    status      - 显示当前状态
    health      - 健康检查所有工作流

示例：
    python scripts/migrate_workflows.py verify
    python scripts/migrate_workflows.py health
    python scripts/migrate_workflows.py status
"""

import sys
import os
import yaml
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
KESTRA_FLOWS_DIR = PROJECT_ROOT / "kestra" / "flows"

# 迁移配置
MIGRATION_CONFIG = {
    "new_workflows": [
        "xcnstock_data_collection_unified.yml",
        "xcnstock_monitoring_unified.yml",
    ],
    "deprecated_workflows": [
        "xcnstock_data_collection.yml",
        "xcnstock_daily_update.yml",
        "xcnstock_data_collection_with_ge.yml",
        "xcnstock_system_monitor.yml",
        "xcnstock_data_inspection.yml",
        "xcnstock_smart_pipeline.yml",
        "xcnstock_data_pipeline_simple.yml",
        "xcnstock_morning_report_simple.yml",
        "xcnstock_debug.yml",
    ],
    "migration_date": "2025-05-25",
    "deletion_date": "2025-06-25",
}


def parse_workflow_file(filepath: Path) -> Dict:
    """解析工作流 YAML 文件"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"❌ 解析失败 {filepath.name}: {e}")
        return {}


def check_workflow_exists(filename: str) -> bool:
    """检查工作流文件是否存在"""
    filepath = KESTRA_FLOWS_DIR / filename
    return filepath.exists()


def check_trigger_status(filename: str) -> Tuple[bool, List[Dict]]:
    """
    检查工作流触发器状态
    返回: (是否全部禁用, 触发器列表)
    """
    filepath = KESTRA_FLOWS_DIR / filename
    workflow = parse_workflow_file(filepath)
    
    triggers = workflow.get('triggers', [])
    if not triggers:
        return True, []  # 没有触发器视为全部禁用
    
    all_disabled = True
    trigger_info = []
    
    for trigger in triggers:
        trigger_id = trigger.get('id', 'unknown')
        disabled = trigger.get('disabled', False)
        trigger_type = trigger.get('type', 'unknown')
        
        trigger_info.append({
            'id': trigger_id,
            'type': trigger_type,
            'disabled': disabled,
        })
        
        if not disabled:
            all_disabled = False
    
    return all_disabled, trigger_info


def check_deprecated_labels(filename: str) -> Tuple[bool, Dict]:
    """
    检查废弃工作流是否有正确的标签
    返回: (是否正确标记, 标签信息)
    """
    filepath = KESTRA_FLOWS_DIR / filename
    workflow = parse_workflow_file(filepath)
    
    labels = workflow.get('labels', {})
    description = workflow.get('description', '')
    
    has_deprecated_label = labels.get('deprecated') == 'true'
    has_replacement_label = 'replacement' in labels
    has_deprecated_notice = '已废弃' in description or 'DEPRECATED' in description
    
    return (
        has_deprecated_label and has_replacement_label and has_deprecated_notice,
        {
            'deprecated_label': has_deprecated_label,
            'replacement_label': has_replacement_label,
            'deprecated_notice': has_deprecated_notice,
            'labels': labels,
        }
    )


def verify_migration() -> Dict:
    """验证迁移状态"""
    print("=" * 60)
    print("🔍 工作流迁移验证")
    print("=" * 60)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'new_workflows': {},
        'deprecated_workflows': {},
        'overall_status': 'PASS',
    }
    
    # 验证新工作流
    print("\n📦 新工作流检查:")
    for wf in MIGRATION_CONFIG['new_workflows']:
        exists = check_workflow_exists(wf)
        results['new_workflows'][wf] = {
            'exists': exists,
            'status': 'OK' if exists else 'MISSING',
        }
        
        if exists:
            print(f"  ✅ {wf} - 已部署")
        else:
            print(f"  ❌ {wf} - 未找到")
            results['overall_status'] = 'FAIL'
    
    # 验证废弃工作流
    print("\n🗑️  废弃工作流检查:")
    for wf in MIGRATION_CONFIG['deprecated_workflows']:
        exists = check_workflow_exists(wf)
        
        if not exists:
            results['deprecated_workflows'][wf] = {
                'exists': False,
                'status': 'DELETED',
            }
            print(f"  ✅ {wf} - 已删除")
            continue
        
        # 检查标签
        labels_ok, labels_info = check_deprecated_labels(wf)
        
        # 检查触发器
        triggers_disabled, trigger_info = check_trigger_status(wf)
        
        status = 'OK' if labels_ok and triggers_disabled else 'WARNING'
        
        results['deprecated_workflows'][wf] = {
            'exists': True,
            'labels_ok': labels_ok,
            'triggers_disabled': triggers_disabled,
            'trigger_info': trigger_info,
            'status': status,
        }
        
        if labels_ok and triggers_disabled:
            print(f"  ✅ {wf} - 已正确标记并禁用")
        else:
            print(f"  ⚠️  {wf} - 需要检查")
            if not labels_ok:
                print(f"      - 标签不完整: {labels_info}")
            if not triggers_disabled:
                active_triggers = [t['id'] for t in trigger_info if not t['disabled']]
                print(f"      - 活动触发器: {active_triggers}")
            results['overall_status'] = 'WARNING'
    
    return results


def generate_report(results: Dict) -> str:
    """生成迁移报告"""
    report_lines = [
        "# XCNStock 工作流迁移报告",
        f"\n生成时间: {results['timestamp']}",
        f"总体状态: {results['overall_status']}",
        "\n## 新工作流状态",
    ]
    
    for wf, info in results['new_workflows'].items():
        status_icon = "✅" if info['status'] == 'OK' else "❌"
        report_lines.append(f"- {status_icon} {wf}: {info['status']}")
    
    report_lines.append("\n## 废弃工作流状态")
    
    for wf, info in results['deprecated_workflows'].items():
        if info['status'] == 'DELETED':
            report_lines.append(f"- ✅ {wf}: 已删除")
        elif info['status'] == 'OK':
            report_lines.append(f"- ✅ {wf}: 已正确标记并禁用")
        else:
            report_lines.append(f"- ⚠️  {wf}: 需要检查")
            if not info.get('labels_ok'):
                report_lines.append("  - 标签不完整")
            if not info.get('triggers_disabled'):
                report_lines.append("  - 触发器未完全禁用")
    
    report_lines.extend([
        "\n## 迁移时间线",
        f"- 迁移日期: {MIGRATION_CONFIG['migration_date']}",
        f"- 删除日期: {MIGRATION_CONFIG['deletion_date']}",
    ])
    
    return '\n'.join(report_lines)


def save_report(results: Dict):
    """保存报告到文件"""
    report = generate_report(results)
    
    report_dir = PROJECT_ROOT / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = report_dir / f"workflow_migration_report_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n📝 报告已保存: {report_file}")
    
    # 同时保存 JSON 格式
    json_file = report_dir / f"workflow_migration_report_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"📝 JSON 报告: {json_file}")


def rollback_workflow(filename: str):
    """回滚单个工作流"""
    filepath = KESTRA_FLOWS_DIR / filename
    
    if not filepath.exists():
        print(f"  ⚠️  {filename} 不存在，跳过")
        return
    
    # 读取工作流
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 启用触发器（将 disabled: true 改为 disabled: false）
    # 注意：这只处理注释掉的 disabled 行
    import re
    
    # 匹配带有废弃注释的 disabled: true
    pattern = r'disabled: true\s+#.*已废弃.*$'
    replacement = 'disabled: false  # 已回滚'
    
    new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {filename} - 已启用触发器")
    else:
        print(f"  ⚠️  {filename} - 未找到可回滚的触发器")


def rollback_all():
    """回滚所有废弃工作流"""
    print("=" * 60)
    print("🔄 工作流回滚")
    print("=" * 60)
    print("\n⚠️  警告: 这将重新启用旧工作流！")
    print("新工作流不会被删除，但需要手动禁用其触发器。\n")
    
    confirm = input("确认回滚? (yes/no): ")
    if confirm.lower() != 'yes':
        print("❌ 回滚已取消")
        return
    
    print("\n🔄 开始回滚...")
    
    for wf in MIGRATION_CONFIG['deprecated_workflows']:
        rollback_workflow(wf)
    
    print("\n✅ 回滚完成")
    print("⚠️  请记得手动禁用新工作流的触发器！")


def show_status():
    """显示当前状态"""
    print("=" * 60)
    print("📊 工作流迁移状态")
    print("=" * 60)
    
    print(f"\n迁移日期: {MIGRATION_CONFIG['migration_date']}")
    print(f"删除日期: {MIGRATION_CONFIG['deletion_date']}")
    
    days_since_migration = (datetime.now() - datetime.strptime(
        MIGRATION_CONFIG['migration_date'], '%Y-%m-%d'
    )).days
    days_until_deletion = (datetime.strptime(
        MIGRATION_CONFIG['deletion_date'], '%Y-%m-%d'
    ) - datetime.now()).days
    
    print(f"迁移已进行: {days_since_migration} 天")
    print(f"距离删除还有: {days_until_deletion} 天")
    
    print("\n📦 新工作流:")
    for wf in MIGRATION_CONFIG['new_workflows']:
        exists = "✅ 已部署" if check_workflow_exists(wf) else "❌ 未找到"
        print(f"  {exists} {wf}")
    
    print("\n🗑️  废弃工作流:")
    for wf in MIGRATION_CONFIG['deprecated_workflows']:
        if not check_workflow_exists(wf):
            print(f"  ✅ 已删除 {wf}")
        else:
            triggers_disabled, _ = check_trigger_status(wf)
            status = "✅ 已禁用" if triggers_disabled else "⚠️  活动"
            print(f"  {status} {wf}")


def health_check():
    """健康检查 - 验证所有工作流是否正常工作"""
    print("=" * 60)
    print("🏥 工作流健康检查")
    print("=" * 60)
    
    issues = []
    
    # 检查新工作流
    print("\n📦 检查新工作流...")
    for wf in MIGRATION_CONFIG['new_workflows']:
        if not check_workflow_exists(wf):
            issues.append(f"❌ 新工作流缺失: {wf}")
            print(f"  ❌ {wf} - 未找到")
        else:
            # 检查是否有触发器
            triggers_disabled, trigger_info = check_trigger_status(wf)
            if not trigger_info:
                issues.append(f"⚠️  {wf} 没有触发器")
                print(f"  ⚠️  {wf} - 没有触发器")
            else:
                active = sum(1 for t in trigger_info if not t['disabled'])
                print(f"  ✅ {wf} - {len(trigger_info)} 个触发器 ({active} 个活动)")
    
    # 检查废弃工作流
    print("\n🗑️  检查废弃工作流...")
    for wf in MIGRATION_CONFIG['deprecated_workflows']:
        if not check_workflow_exists(wf):
            print(f"  ✅ {wf} - 已删除")
            continue
        
        triggers_disabled, trigger_info = check_trigger_status(wf)
        if not triggers_disabled:
            issues.append(f"⚠️  废弃工作流仍有活动触发器: {wf}")
            active_triggers = [t['id'] for t in trigger_info if not t['disabled']]
            print(f"  ❌ {wf} - 仍有活动触发器: {active_triggers}")
        else:
            print(f"  ✅ {wf} - 已禁用")
    
    # 检查关键文件
    print("\n📁 检查关键文件...")
    critical_files = [
        "scripts/pipeline/data_collect.py",
        "scripts/pipeline/data_collect_with_validation.py",
        "scripts/pipeline/send_workflow_notification.py",
    ]
    
    for file_path in critical_files:
        full_path = PROJECT_ROOT / file_path
        if full_path.exists():
            print(f"  ✅ {file_path}")
        else:
            issues.append(f"❌ 关键文件缺失: {file_path}")
            print(f"  ❌ {file_path} - 未找到")
    
    # 总结
    print("\n" + "=" * 60)
    if issues:
        print(f"❌ 发现 {len(issues)} 个问题:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("✅ 所有检查通过！工作流健康状况良好。")
    print("=" * 60)
    
    return len(issues) == 0


def main():
    """主函数"""
    command = sys.argv[1] if len(sys.argv) > 1 else 'verify'
    
    if command == 'verify':
        results = verify_migration()
        save_report(results)
        
        print("\n" + "=" * 60)
        if results['overall_status'] == 'PASS':
            print("✅ 迁移验证通过！")
        elif results['overall_status'] == 'WARNING':
            print("⚠️  迁移验证通过，但有警告")
        else:
            print("❌ 迁移验证失败！")
        print("=" * 60)
        
    elif command == 'rollback':
        rollback_all()
        
    elif command == 'status':
        show_status()
        
    elif command == 'health':
        success = health_check()
        sys.exit(0 if success else 1)
        
    else:
        print(f"未知命令: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
