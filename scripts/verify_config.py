#!/usr/bin/env python3
"""
验证配置文件与脚本的匹配性
确保每个任务都能正常执行
"""
import sys
import yaml
from pathlib import Path
from collections import defaultdict

project_root = Path(__file__).parent.parent


def load_config():
    """加载cron_tasks.yaml"""
    config_path = project_root / 'config' / 'cron_tasks.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def extract_script_path(script_field):
    """从script字段提取脚本路径（去掉参数）"""
    if not script_field:
        return None
    parts = script_field.split()
    return parts[0] if parts else None


def verify_scripts():
    """验证所有脚本存在性"""
    config = load_config()
    tasks = config.get('tasks', [])
    
    print("=" * 80)
    print("配置验证报告")
    print("=" * 80)
    
    stats = {
        'total': 0,
        'enabled': 0,
        'exists': 0,
        'missing': 0,
        'warnings': []
    }
    
    missing_scripts = []
    
    for task in tasks:
        stats['total'] += 1
        name = task.get('name', 'unnamed')
        enabled = task.get('enabled', True)
        script_field = task.get('script', '')
        script_path = extract_script_path(script_field)
        
        if not enabled:
            continue
            
        stats['enabled'] += 1
        
        if not script_path:
            stats['warnings'].append(f"{name}: 未配置脚本路径")
            continue
        
        full_path = project_root / script_path
        
        if full_path.exists():
            stats['exists'] += 1
            status = "✅"
        else:
            stats['missing'] += 1
            status = "❌"
            missing_scripts.append({
                'task': name,
                'script': script_path,
                'full_path': str(full_path)
            })
        
        print(f"{status} {name}")
        print(f"   脚本: {script_path}")
        if not full_path.exists():
            print(f"   路径: {full_path}")
        print()
    
    # 汇总
    print("=" * 80)
    print("验证汇总")
    print("=" * 80)
    print(f"总任务数: {stats['total']}")
    print(f"启用任务: {stats['enabled']}")
    print(f"脚本存在: {stats['exists']}")
    print(f"脚本缺失: {stats['missing']}")
    
    if missing_scripts:
        print("\n❌ 缺失的脚本:")
        for item in missing_scripts:
            print(f"   - {item['task']}: {item['script']}")
    
    if stats['warnings']:
        print("\n⚠️ 警告:")
        for warning in stats['warnings']:
            print(f"   - {warning}")
    
    # 返回是否全部通过
    return stats['missing'] == 0 and len(stats['warnings']) == 0


def verify_schedule_conflicts():
    """检查时间冲突"""
    config = load_config()
    tasks = config.get('tasks', [])
    
    print("\n" + "=" * 80)
    print("时间冲突检查")
    print("=" * 80)
    
    # 按时间分组
    schedule_groups = defaultdict(list)
    
    for task in tasks:
        if not task.get('enabled', True):
            continue
        schedule = task.get('schedule', '')
        name = task.get('name', '')
        schedule_groups[schedule].append(name)
    
    conflicts = {k: v for k, v in schedule_groups.items() if len(v) > 1}
    
    if conflicts:
        print("\n⚠️ 发现时间冲突:")
        for schedule, names in conflicts.items():
            print(f"   时间: {schedule}")
            for name in names:
                print(f"      - {name}")
    else:
        print("\n✅ 未发现时间冲突")
    
    return len(conflicts) == 0


def verify_dependencies():
    """验证依赖关系"""
    config = load_config()
    tasks = config.get('tasks', [])
    
    print("\n" + "=" * 80)
    print("依赖关系检查")
    print("=" * 80)
    
    task_names = {t['name'] for t in tasks if t.get('enabled', True)}
    broken_deps = []
    
    for task in tasks:
        if not task.get('enabled', True):
            continue
        name = task.get('name', '')
        depends_on = task.get('depends_on', '')
        
        if depends_on and depends_on not in task_names:
            broken_deps.append({
                'task': name,
                'depends_on': depends_on
            })
    
    if broken_deps:
        print("\n❌ 无效的依赖:")
        for item in broken_deps:
            print(f"   - {item['task']} 依赖 {item['depends_on']} (不存在或已禁用)")
    else:
        print("\n✅ 所有依赖关系有效")
    
    return len(broken_deps) == 0


def main():
    print("开始验证配置...\n")
    
    scripts_ok = verify_scripts()
    schedule_ok = verify_schedule_conflicts()
    deps_ok = verify_dependencies()
    
    print("\n" + "=" * 80)
    print("最终验证结果")
    print("=" * 80)
    
    if scripts_ok and schedule_ok and deps_ok:
        print("✅ 所有验证通过！配置与脚本完全匹配。")
        return 0
    else:
        print("❌ 验证未通过，请修复上述问题。")
        return 1


if __name__ == '__main__':
    sys.exit(main())
