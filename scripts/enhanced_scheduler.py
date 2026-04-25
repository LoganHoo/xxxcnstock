#!/usr/bin/env python3
"""
XCNStock 增强版任务调度器
支持：依赖管理、失败重试、断点续传、状态监控
"""
import sys
import os
import yaml
import json
import time
import signal
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from enum import Enum

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger

logger = setup_logger("enhanced_scheduler")


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class TaskState:
    """任务状态"""
    name: str
    status: str
    last_run: Optional[str] = None
    last_result: Optional[bool] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


class EnhancedScheduler:
    """增强版任务调度器"""

    def __init__(self, config_path: Optional[str] = None):
        self.project_root = Path(__file__).parent.parent
        self.config_path = config_path or self.project_root / 'config' / 'cron_tasks.yaml'
        self.state_file = self.project_root / 'data' / 'scheduler_state.json'

        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        self.tasks: Dict[str, Dict] = {}
        self.task_states: Dict[str, TaskState] = {}
        self.dependency_graph: Dict[str, List[str]] = {}

        self._load_config()
        self._load_state()
        self._build_dependency_graph()

        # 信号处理
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _load_config(self):
        """加载配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self.global_config = config.get('global', {})
                self.tasks = {t['name']: t for t in config.get('tasks', []) if t.get('enabled', True)}
            logger.info(f"✅ 加载配置成功，共 {len(self.tasks)} 个任务")
        except Exception as e:
            logger.error(f"❌ 加载配置失败: {e}")
            raise

    def _load_state(self):
        """加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                    for name, data in state_data.items():
                        self.task_states[name] = TaskState(**data)
                logger.info(f"✅ 加载状态成功，共 {len(self.task_states)} 个任务状态")
            except Exception as e:
                logger.warning(f"⚠️ 加载状态失败: {e}")

    def _save_state(self):
        """保存状态"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            state_data = {name: asdict(state) for name, state in self.task_states.items()}
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ 保存状态失败: {e}")

    def _build_dependency_graph(self):
        """构建依赖图"""
        for name, task in self.tasks.items():
            self.dependency_graph[name] = task.get('depends_on', [])
            if name not in self.task_states:
                self.task_states[name] = TaskState(name=name, status=TaskStatus.PENDING.value)

    def _check_dependencies(self, task_name: str) -> bool:
        """检查依赖是否完成"""
        dependencies = self.dependency_graph.get(task_name, [])
        for dep in dependencies:
            dep_state = self.task_states.get(dep)
            if not dep_state or dep_state.status != TaskStatus.SUCCESS.value:
                logger.warning(f"⏸️ {task_name} 依赖 {dep} 未完成 (状态: {dep_state.status if dep_state else 'unknown'})")
                return False
        return True

    def _should_skip(self, task: Dict) -> bool:
        """检查是否应该跳过"""
        # 检查 skip_if_passed 条件
        skip_if_passed = task.get('skip_if_passed')
        if skip_if_passed:
            skip_task_state = self.task_states.get(skip_if_passed)
            if skip_task_state and skip_task_state.status == TaskStatus.SUCCESS.value:
                logger.info(f"⏭️ {task['name']} 跳过 (依赖任务 {skip_if_passed} 已成功)")
                return True
        return False

    def _should_retry(self, task: Dict, task_name: str) -> bool:
        """检查是否应该重试"""
        state = self.task_states[task_name]
        max_retries = task.get('max_retries', self.global_config.get('max_retries', 3))
        return state.retry_count < max_retries

    def _run_task(self, task: Dict) -> bool:
        """执行单个任务"""
        name = task['name']
        script = task['script']
        timeout = task.get('timeout', 600)

        script_path = self.project_root / script
        if not script_path.exists():
            logger.error(f"❌ 脚本不存在: {script_path}")
            return False

        logger.info(f"▶️ 开始执行: {name}")
        start_time = time.time()

        try:
            # 设置环境变量
            env = os.environ.copy()
            env.update(task.get('env', {}))

            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(self.project_root),
                env=env
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                logger.info(f"✅ {name} 完成 ({duration:.1f}s)")
                return True
            else:
                logger.error(f"❌ {name} 失败 ({duration:.1f}s)")
                logger.error(f"   错误: {result.stderr[:500]}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"⏱️ {name} 超时 ({timeout}s)")
            return False
        except Exception as e:
            logger.error(f"❌ {name} 异常: {e}")
            return False

    def _execute_task(self, task: Dict):
        """执行任务（带依赖检查和重试）"""
        name = task['name']
        state = self.task_states[name]

        # 检查依赖
        if not self._check_dependencies(name):
            state.status = TaskStatus.SKIPPED.value
            state.error_message = "依赖未完成"
            self._save_state()
            return

        # 检查跳过条件
        if self._should_skip(task):
            state.status = TaskStatus.SKIPPED.value
            self._save_state()
            return

        # 执行任务
        state.status = TaskStatus.RUNNING.value
        state.last_run = datetime.now().isoformat()
        self._save_state()

        result = self._run_task(task)

        # 更新状态
        state.last_result = result
        state.duration_seconds = time.time() - datetime.fromisoformat(state.last_run).timestamp()

        if result:
            state.status = TaskStatus.SUCCESS.value
            state.retry_count = 0
            state.error_message = None
        else:
            if self._should_retry(task, name):
                state.status = TaskStatus.RETRYING.value
                state.retry_count += 1
                retry_delay = task.get('retry_delay', self.global_config.get('retry_delay', 60))
                logger.info(f"🔄 {name} 将在 {retry_delay} 秒后重试 ({state.retry_count}/{task.get('max_retries', 3)})")

                # 调度重试
                self.scheduler.add_job(
                    self._execute_task,
                    'date',
                    run_date=datetime.now() + timedelta(seconds=retry_delay),
                    args=[task],
                    id=f"{name}_retry_{state.retry_count}",
                    replace_existing=True
                )
            else:
                state.status = TaskStatus.FAILED.value
                state.error_message = "重试次数用尽"

        self._save_state()

    def _signal_handler(self, signum, frame):
        """信号处理"""
        logger.info(f"📴 收到信号 {signum}，正在关闭调度器...")
        self._save_state()
        self.scheduler.shutdown(wait=False)
        sys.exit(0)

    def start(self):
        """启动调度器"""
        # 注册任务
        for name, task in self.tasks.items():
            schedule = task['schedule']
            try:
                trigger = CronTrigger.from_crontab(schedule)
                self.scheduler.add_job(
                    self._execute_task,
                    trigger=trigger,
                    args=[task],
                    id=name,
                    name=task.get('description', name),
                    replace_existing=True
                )
                logger.info(f"✅ 已注册: {name} ({schedule})")
            except Exception as e:
                logger.error(f"❌ 注册失败 {name}: {e}")

        # 添加状态监控任务
        self.scheduler.add_job(
            self._print_status,
            'interval',
            minutes=5,
            id='status_monitor'
        )

        logger.info(f"\n🚀 调度器已启动，共 {len(self.scheduler.get_jobs())} 个任务\n")

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self._signal_handler(0, None)

    def _print_status(self):
        """打印状态"""
        running = sum(1 for s in self.task_states.values() if s.status == TaskStatus.RUNNING.value)
        success = sum(1 for s in self.task_states.values() if s.status == TaskStatus.SUCCESS.value)
        failed = sum(1 for s in self.task_states.values() if s.status == TaskStatus.FAILED.value)

        logger.info(f"📊 状态: 运行中={running}, 成功={success}, 失败={failed}")


def main():
    """主函数"""
    scheduler = EnhancedScheduler()
    scheduler.start()


if __name__ == '__main__':
    main()
