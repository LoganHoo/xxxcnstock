#!/usr/bin/env python3
"""
查看错误记录

功能：
1. 查看所有错误记录
2. 按状态筛选错误
3. 查看修复历史
"""
import sys
import json
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

ERROR_LOG_FILE = project_root / "logs" / "error_records.json"


def load_error_records():
    """加载错误记录"""
    if ERROR_LOG_FILE.exists():
        try:
            with open(ERROR_LOG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载错误记录失败: {e}")
            return []
    return []


def view_errors(status_filter=None, task_filter=None):
    """查看错误记录"""
    records = load_error_records()

    if not records:
        print("没有错误记录")
        return

    # 筛选
    filtered = records
    if status_filter:
        filtered = [r for r in filtered if r.get('fix_status') == status_filter]
    if task_filter:
        filtered = [r for r in filtered if task_filter.lower() in r.get('task_id', '').lower()]

    # 按时间排序（最新的在前）
    filtered = sorted(filtered, key=lambda x: x.get('timestamp', ''), reverse=True)

    print("=" * 80)
    print("错误记录查看")
    print("=" * 80)
    print()

    # 统计
    status_counts = {}
    for r in records:
        status = r.get('fix_status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    print("📊 统计:")
    for status, count in sorted(status_counts.items()):
        status_icon = {
            'fixed': '✅',
            'pending': '⏳',
            'in_progress': '🔄',
            'failed': '❌',
            'manual_required': '👤'
        }.get(status, '❓')
        print(f"  {status_icon} {status}: {count}")
    print()

    # 显示记录
    if filtered:
        print(f"📋 显示 {len(filtered)} 条记录:")
        print("-" * 80)

        for i, record in enumerate(filtered[:20], 1):  # 只显示前20条
            status = record.get('fix_status', 'unknown')
            status_icon = {
                'fixed': '✅',
                'pending': '⏳',
                'in_progress': '🔄',
                'failed': '❌',
                'manual_required': '👤'
            }.get(status, '❓')

            print(f"\n{i}. {status_icon} {record.get('task_name', 'Unknown')}")
            print(f"   时间: {record.get('timestamp', 'N/A')}")
            print(f"   类型: {record.get('error_type', 'unknown')}")
            print(f"   状态: {status}")

            error_msg = record.get('error_message', '')
            if len(error_msg) > 100:
                error_msg = error_msg[:100] + "..."
            print(f"   错误: {error_msg}")

            if record.get('fix_result'):
                print(f"   修复结果: {record.get('fix_result')}")

            if record.get('fix_attempts', 0) > 0:
                print(f"   修复尝试: {record.get('fix_attempts')} 次")

            if record.get('fix_time'):
                print(f"   修复时间: {record.get('fix_time')}")

        if len(filtered) > 20:
            print(f"\n... 还有 {len(filtered) - 20} 条记录")
    else:
        print("没有符合条件的记录")

    print()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='查看错误记录')
    parser.add_argument('--status', '-s', help='按状态筛选 (fixed, pending, failed, manual_required)')
    parser.add_argument('--task', '-t', help='按任务名筛选')
    parser.add_argument('--stats', '-S', action='store_true', help='只显示统计信息')

    args = parser.parse_args()

    if args.stats:
        records = load_error_records()
        status_counts = {}
        type_counts = {}
        task_counts = {}

        for r in records:
            status = r.get('fix_status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1

            error_type = r.get('error_type', 'unknown')
            type_counts[error_type] = type_counts.get(error_type, 0) + 1

            task = r.get('task_id', 'unknown')
            task_counts[task] = task_counts.get(task, 0) + 1

        print("=" * 80)
        print("错误统计")
        print("=" * 80)
        print()

        print("按状态:")
        for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count}")
        print()

        print("按错误类型:")
        for error_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {error_type}: {count}")
        print()

        print("按任务:")
        for task, count in sorted(task_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {task}: {count}")
    else:
        view_errors(args.status, args.task)


if __name__ == "__main__":
    main()
