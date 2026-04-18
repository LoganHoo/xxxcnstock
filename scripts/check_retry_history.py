#!/usr/bin/env python3
"""
检查任务重试记录
"""
import sys
import json
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

STATE_FILE = project_root / 'logs' / 'task_states.json'


def load_states():
    """加载任务状态"""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载状态文件失败: {e}")
            return {}
    return {}


def check_retry_history():
    """检查重试记录"""
    states = load_states()

    if not states:
        print("没有任务状态记录")
        return

    print("=" * 80)
    print("任务重试记录检查")
    print("=" * 80)
    print()

    # 按日期分组
    today = datetime.now().strftime('%Y%m%d')
    today_tasks = {}
    other_tasks = {}

    for key, state in states.items():
        if '_' in key:
            job_id, date = key.rsplit('_', 1)
        else:
            job_id = key
            date = 'unknown'

        retry_count = state.get('retries', 0)
        retry_history = state.get('retry_history', [])
        status = state.get('status', 'unknown')
        last_run = state.get('last_run', 'N/A')
        result = state.get('result', '')

        task_info = {
            'job_id': job_id,
            'date': date,
            'status': status,
            'retries': retry_count,
            'retry_history': retry_history,
            'last_run': last_run,
            'result': result
        }

        if date == today:
            today_tasks[key] = task_info
        else:
            other_tasks[key] = task_info

    # 显示今日任务
    if today_tasks:
        print(f"📅 今日任务 ({today}):")
        print("-" * 80)

        # 按重试次数排序
        sorted_tasks = sorted(today_tasks.items(), key=lambda x: x[1]['retries'], reverse=True)

        for key, task in sorted_tasks:
            status_icon = "✅" if task['status'] == 'completed' else "❌" if task['status'] == 'failed' else "⏳"
            print(f"  {status_icon} {task['job_id']}")
            print(f"     状态: {task['status']}")
            print(f"     重试次数: {task['retries']}")
            print(f"     最后运行: {task['last_run']}")

            if task['retry_history']:
                print(f"     重试历史:")
                for retry in task['retry_history']:
                    print(f"       - {retry['time']} (第 {retry['retry_count']} 次)")

            if task['result'] and task['result'] not in ['成功', '修复成功']:
                print(f"     结果: {task['result']}")
            print()

    # 显示有重试的历史任务
    tasks_with_retries = {k: v for k, v in other_tasks.items() if v['retries'] > 0}
    if tasks_with_retries:
        print(f"📚 历史重试记录（有重试的任务）:")
        print("-" * 80)

        # 按日期和重试次数排序
        sorted_tasks = sorted(tasks_with_retries.items(), key=lambda x: (x[1]['date'], x[1]['retries']), reverse=True)

        for key, task in sorted_tasks[:10]:  # 只显示最近10个
            status_icon = "✅" if task['status'] == 'completed' else "❌" if task['status'] == 'failed' else "⏳"
            print(f"  {status_icon} {task['job_id']} ({task['date']})")
            print(f"     状态: {task['status']} | 重试: {task['retries']} 次 | 最后运行: {task['last_run']}")

            if task['retry_history']:
                print(f"     重试历史:")
                for retry in task['retry_history'][:3]:  # 只显示前3次
                    print(f"       - {retry['time']}")
            print()

    # 统计信息
    print("=" * 80)
    print("统计信息:")
    print("-" * 80)

    total_tasks = len(states)
    today_total = len(today_tasks)
    today_failed = sum(1 for t in today_tasks.values() if t['status'] == 'failed')
    today_retried = sum(1 for t in today_tasks.values() if t['retries'] > 0)
    total_retries_today = sum(t['retries'] for t in today_tasks.values())

    print(f"  总任务数: {total_tasks}")
    print(f"  今日任务数: {today_total}")
    print(f"  今日失败任务: {today_failed}")
    print(f"  今日有重试的任务: {today_retried}")
    print(f"  今日总重试次数: {total_retries_today}")
    print()

    # 显示重试机制配置
    print("=" * 80)
    print("重试机制配置:")
    print("-" * 80)
    print(f"  最大重试次数: 3")
    print(f"  重试延迟: 300 秒 (5分钟)")
    print(f"  任务锁超时: 600 秒 (10分钟)")
    print()


if __name__ == "__main__":
    check_retry_history()
