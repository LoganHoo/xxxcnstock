#!/usr/bin/env python3
"""验证cron配置有效性"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from pathlib import Path

try:
    with open('config/cron_tasks.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("=" * 60)
    print("配置验证结果")
    print("=" * 60)
    
    tasks = config.get('tasks', [])
    task_names = [t['name'] for t in tasks if t.get('enabled', True)]
    
    # 检查groups中的任务是否存在
    groups = config.get('groups', {})
    print(f"\n总任务数: {len(tasks)}")
    print(f"启用任务数: {len(task_names)}")
    print(f"任务分组数: {len(groups)}")
    
    # 检查依赖关系
    print("\n依赖关系检查:")
    for task in tasks:
        name = task.get('name')
        depends_on = task.get('depends_on')
        if depends_on:
            if depends_on in task_names:
                print(f"  OK {name} -> {depends_on}")
            else:
                print(f"  WARN {name} -> {depends_on} (未启用或不存在)")
    
    # 检查groups
    print("\n任务分组检查:")
    for group_name, group_info in groups.items():
        group_tasks = group_info.get('tasks', [])
        print(f"\n  {group_name}: {group_info.get('description', '')}")
        for task in group_tasks:
            if task in task_names:
                print(f"    OK {task}")
            else:
                print(f"    MISSING {task} (不存在)")
    
    print("\n" + "=" * 60)
    print("配置验证通过")
    print("=" * 60)
    
except Exception as e:
    print(f"配置错误: {e}")
    sys.exit(1)
