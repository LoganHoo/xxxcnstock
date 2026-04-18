#!/usr/bin/env python3
"""
分析 cron_tasks.yaml 中的任务重复和冲突
"""
import yaml
from pathlib import Path
from collections import defaultdict

# 读取配置文件
config_path = Path('config/cron_tasks.yaml')
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

tasks = config.get('tasks', [])

print("=" * 80)
print("Cron Tasks 分析报告")
print("=" * 80)

# 1. 检查重复的任务名称
print("\n【1. 任务名称重复检查】")
name_counts = defaultdict(list)
for i, task in enumerate(tasks):
    if 'name' in task:
        name_counts[task['name']].append(i)

duplicates = {name: indices for name, indices in name_counts.items() if len(indices) > 1}
if duplicates:
    print("❌ 发现重复的任务名称:")
    for name, indices in duplicates.items():
        print(f"   - {name}: 出现在第 {[i+1 for i in indices]} 个任务")
else:
    print("✅ 未发现重复的任务名称")

# 2. 检查相同脚本不同参数
print("\n【2. 相同脚本不同参数检查】")
script_tasks = defaultdict(list)
for i, task in enumerate(tasks):
    if 'script' in task:
        # 提取基础脚本名（去掉参数）
        script = task['script'].split()[0]
        script_tasks[script].append((i, task['name'], task['script']))

print("\n相同脚本的任务:")
for script, task_list in sorted(script_tasks.items(), key=lambda x: -len(x[1])):
    if len(task_list) > 1:
        print(f"\n   📄 {script} ({len(task_list)} 个任务):")
        for idx, name, full_script in task_list:
            print(f"      - {name}: {full_script}")

# 3. 检查时间冲突
print("\n【3. 时间冲突检查】")
schedule_tasks = defaultdict(list)
for i, task in enumerate(tasks):
    if 'schedule' in task:
        schedule_tasks[task['schedule']].append((i, task['name']))

conflicts = {sched: tasks for sched, tasks in schedule_tasks.items() if len(tasks) > 1}
if conflicts:
    print("❌ 发现相同执行时间的任务:")
    for sched, task_list in conflicts.items():
        print(f"\n   ⏰ {sched}:")
        for idx, name in task_list:
            print(f"      - {name}")
else:
    print("✅ 未发现时间冲突")

# 4. 检查依赖关系
print("\n【4. 依赖关系检查】")
dependencies = {}
for i, task in enumerate(tasks):
    if 'name' in task and 'depends_on' in task:
        dependencies[task['name']] = task['depends_on']

print(f"\n共有 {len(dependencies)} 个任务有依赖关系:")
for name, depends in list(dependencies.items())[:10]:
    print(f"   - {name} → 依赖: {depends}")
if len(dependencies) > 10:
    print(f"   ... 还有 {len(dependencies) - 10} 个")

# 5. 检查可能的功能重复
print("\n【5. 潜在功能重复检查】")

# 检查选股相关任务
pick_tasks = [(i, t['name'], t['script']) for i, t in enumerate(tasks) 
              if 'pick' in t.get('name', '') or 'select' in t.get('name', '')]
if pick_tasks:
    print("\n   选股相关任务:")
    for idx, name, script in pick_tasks:
        print(f"      - {name}: {script}")

# 检查新闻采集任务
news_tasks = [(i, t['name'], t['script']) for i, t in enumerate(tasks) 
              if 'news' in t.get('name', '') or 'cctv' in t.get('name', '')]
if news_tasks:
    print("\n   新闻采集任务:")
    for idx, name, script in news_tasks:
        print(f"      - {name}: {script}")

# 检查监控任务
monitor_tasks = [(i, t['name'], t['script']) for i, t in enumerate(tasks) 
                 if 'monitor' in t.get('name', '') or 'dashboard' in t.get('name', '')]
if monitor_tasks:
    print("\n   监控相关任务:")
    for idx, name, script in monitor_tasks:
        print(f"      - {name}: {script}")

# 6. 总结
print("\n" + "=" * 80)
print("【总结】")
print("=" * 80)
print(f"总任务数: {len(tasks)}")
print(f"重复名称: {len(duplicates)} 个")
print(f"时间冲突: {len(conflicts)} 个")
print(f"有依赖的任务: {len(dependencies)} 个")

print("\n" + "=" * 80)
