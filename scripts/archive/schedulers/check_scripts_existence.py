#!/usr/bin/env python3
"""验证所有脚本存在性"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from pathlib import Path

# 读取cron配置
with open('config/cron_tasks.yaml', 'r') as f:
    config = yaml.safe_load(f)

# 提取所有脚本路径
scripts = set()
for task in config.get('tasks', []):
    script = task.get('script', '')
    if script:
        parts = script.split()
        for part in parts:
            if part.endswith('.py'):
                scripts.add(part)
                break

print("=" * 70)
print("脚本存在性检查")
print("=" * 70)

missing = []
existing = []

for script in sorted(scripts):
    path = Path(script)
    if path.exists():
        existing.append(script)
        print(f"✅ {script}")
    else:
        missing.append(script)
        print(f"❌ {script}")

print("\n" + "=" * 70)
print(f"总计: {len(scripts)} 个脚本")
print(f"✅ 存在: {len(existing)} 个")
print(f"❌ 缺失: {len(missing)} 个")
print("=" * 70)

if missing:
    print("\n❌ 缺失脚本列表:")
    for m in missing:
        print(f"  - {m}")
    sys.exit(1)
else:
    print("\n✅ 所有脚本都存在！")
    sys.exit(0)
