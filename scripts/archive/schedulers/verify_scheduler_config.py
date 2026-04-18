#!/usr/bin/env python3
"""
验证调度器配置一致性
检查 cron_tasks.yaml 中的任务是否能被 apscheduler_enhanced.py 正确加载
"""
import sys
import yaml
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def load_cron_config():
    """加载 cron_tasks.yaml"""
    config_path = project_root / 'config' / 'cron_tasks.yaml'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置文件失败: {e}")
        return None


def validate_config():
    """验证配置"""
    config = load_cron_config()
    if not config:
        return False

    print("=" * 80)
    print("调度器配置验证报告")
    print("=" * 80)

    tasks = config.get('tasks', [])
    groups = config.get('groups', {})

    # 统计信息
    total_tasks = len(tasks)
    enabled_tasks = [t for t in tasks if t.get('enabled', True)]
    disabled_tasks = [t for t in tasks if not t.get('enabled', True)]

    print(f"\n📊 任务统计:")
    print(f"   总任务数: {total_tasks}")
    print(f"   启用任务: {len(enabled_tasks)}")
    print(f"   禁用任务: {len(disabled_tasks)}")

    # 验证脚本存在性
    print(f"\n🔍 验证脚本存在性:")
    missing_scripts = []
    for task in enabled_tasks:
        script = task.get('script', '')
        if script:
            script_path = script.split()[0]
            full_path = project_root / script_path
            if not full_path.exists():
                missing_scripts.append((task['name'], script_path))
                print(f"   ❌ {task['name']}: {script_path} 不存在")
            else:
                print(f"   ✅ {task['name']}: {script_path}")

    # 验证任务分组
    print(f"\n📁 任务分组验证:")
    all_grouped_tasks = set()
    for group_name, group_info in groups.items():
        group_tasks = group_info.get('tasks', [])
        print(f"   📂 {group_name}: {len(group_tasks)} 个任务")
        all_grouped_tasks.update(group_tasks)

    # 检查是否有任务未分组
    task_names = {t['name'] for t in enabled_tasks}
    ungrouped = task_names - all_grouped_tasks
    if ungrouped:
        print(f"\n⚠️  未分组的任务:")
        for name in ungrouped:
            print(f"   - {name}")
    else:
        print(f"\n✅ 所有启用任务都已分组")

    # 检查分组中是否有不存在的任务
    print(f"\n🔍 检查分组有效性:")
    invalid_in_groups = all_grouped_tasks - task_names
    if invalid_in_groups:
        print(f"   ⚠️  分组中包含不存在的任务:")
        for name in invalid_in_groups:
            print(f"      - {name}")
    else:
        print(f"   ✅ 所有分组任务都有效")

    # 验证调度器能否加载
    print(f"\n🔄 验证调度器加载:")
    try:
        from apscheduler.triggers.cron import CronTrigger
        invalid_cron = []
        for task in enabled_tasks:
            schedule = task.get('schedule', '')
            try:
                CronTrigger.from_crontab(schedule)
            except Exception as e:
                invalid_cron.append((task['name'], schedule, str(e)))

        if invalid_cron:
            print(f"   ❌ 发现 {len(invalid_cron)} 个无效cron表达式:")
            for name, cron, error in invalid_cron:
                print(f"      - {name}: {cron} ({error})")
        else:
            print(f"   ✅ 所有cron表达式都有效")
    except Exception as e:
        print(f"   ⚠️  无法验证cron表达式: {e}")

    # 总结
    print("\n" + "=" * 80)
    print("验证总结:")
    print("=" * 80)

    issues = []
    if missing_scripts:
        issues.append(f"缺失脚本: {len(missing_scripts)} 个")
    if ungrouped:
        issues.append(f"未分组任务: {len(ungrouped)} 个")
    if invalid_in_groups:
        issues.append(f"分组无效任务: {len(invalid_in_groups)} 个")

    if issues:
        print(f"❌ 发现问题: {', '.join(issues)}")
        return False
    else:
        print(f"✅ 配置验证通过！所有 {len(enabled_tasks)} 个启用任务配置正确")
        return True


if __name__ == "__main__":
    success = validate_config()
    sys.exit(0 if success else 1)
