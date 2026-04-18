#!/usr/bin/env python3
"""
定时监控调度器

定期执行日志监控和自动修复
"""
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 监控间隔（秒）
MONITOR_INTERVAL = 300  # 5分钟


def run_monitor():
    """运行监控脚本"""
    print(f"\n{'='*80}")
    print(f"⏰ 定时监控启动: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    try:
        result = subprocess.run(
            [sys.executable, str(project_root / "scripts" / "task_log_monitor.py")],
            capture_output=True,
            text=True,
            timeout=300
        )
        print(result.stdout)
        if result.stderr:
            print("错误输出:", result.stderr)
    except Exception as e:
        print(f"监控执行失败: {e}")


def main():
    """主循环"""
    print("="*80)
    print("任务日志定时监控系统")
    print(f"监控间隔: {MONITOR_INTERVAL} 秒 ({MONITOR_INTERVAL//60} 分钟)")
    print("="*80)
    print()

    # 立即执行一次
    run_monitor()

    # 定时循环
    while True:
        time.sleep(MONITOR_INTERVAL)
        run_monitor()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n监控已停止")
