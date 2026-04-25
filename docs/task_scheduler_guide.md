# XCNStock 任务调度自动执行指南

## 一、现有调度架构

### 1.1 定时任务配置

配置文件：`config/cron_tasks.yaml`

```yaml
# 任务类型分布
├── 盘后流水线 (16:00-20:35)
│   ├── 16:00 数据采集
│   ├── 16:30 断点续传
│   ├── 16:50 数据审计
│   ├── 17:30 复盘分析
│   └── 20:35 晚间分析
│
├── 晨间流水线 (08:25-08:45)
│   ├── 08:30 晨间数据
│   ├── 08:45 晨前哨报
│   └── 09:26 主力行为报告
│
└── 周末任务 (周六 10:00)
    └── 批量更新历史数据
```

### 1.2 任务依赖关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           任务依赖图                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  盘后流水线 (Pipeline)                                                      │
│  ═══════════════════════                                                    │
│                                                                             │
│  data_fetch ──▶ data_fetch_retry ──▶ data_audit_unified                    │
│     │               │                      │                                │
│     │               │                      ├─▶ calculate_cvd                │
│     │               │                      ├─▶ market_review                │
│     │               │                      │       │                        │
│     │               │                      │       ├─▶ picks_review         │
│     │               │                      │               │                │
│     │               │                      │               ├─▶ daily_selection_review
│     │               │                      │               ├─▶ drawdown_analysis
│     │               │                      │                       │        │
│     │               │                      │                       └─▶ review_report
│     │               │                      │                                │
│     │               │                      └─▶ precompute ──▶ night_analysis│
│     │               │                                                       │
│     └───────────────▶ dragon_tiger_fetch (可选，并行)                       │
│                                                                             │
│  晨间流水线 (Pipeline)                                                      │
│  ═══════════════════════                                                    │
│                                                                             │
│  morning_data ──▶ collect_macro ──▶ collect_oil_dollar                      │
│     │                                              │                        │
│     ├─▶ collect_commodities ──▶ collect_sentiment ─┤                        │
│     │                                              │                        │
│     ├─▶ collect_news ◀─────────────────────────────┤                        │
│     │                                              │                        │
│     └─▶ market_analysis ──▶ morning_report                                  │
│                                                                             │
│  09:26 核心任务链                                                           │
│  ═══════════════════════                                                    │
│                                                                             │
│  fund_behavior_resource_prepare ──▶ fund_behavior_resource_validate        │
│                                              │                              │
│                                              └─▶ fund_behavior_guardian_check
│                                                        │                    │
│                                                        └─▶ fund_behavior_report
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 二、自动执行方案

### 2.1 方案选择

| 方案 | 工具 | 适用场景 | 推荐度 |
|-----|------|---------|-------|
| **系统 Cron** | Linux/Mac Cron | 简单定时任务 | ⭐⭐⭐ |
| **APScheduler** | Python库 | 复杂依赖、动态调度 | ⭐⭐⭐⭐⭐ |
| **DolphinScheduler** | 分布式调度 | 大规模分布式 | ⭐⭐⭐⭐ |
| **Airflow** | 工作流编排 | 复杂数据管道 | ⭐⭐⭐⭐ |

**推荐：APScheduler + 自定义依赖管理**

原因：
- ✅ 纯Python，与项目技术栈一致
- ✅ 支持复杂依赖关系
- ✅ 支持失败重试
- ✅ 支持动态添加/删除任务
- ✅ 易于监控和日志记录

### 2.2 增强版调度器设计

```python
# scripts/enhanced_scheduler.py

class EnhancedScheduler:
    """增强版任务调度器"""
    
    def __init__(self):
        self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        self.task_states = {}  # 任务状态跟踪
        self.dependency_graph = {}  # 依赖图
        
    def register_task(self, task_config):
        """注册任务（带依赖检查）"""
        name = task_config['name']
        depends_on = task_config.get('depends_on', [])
        
        # 构建依赖图
        self.dependency_graph[name] = {
            'depends_on': depends_on,
            'status': 'pending',
            'last_run': None,
            'retry_count': 0
        }
        
        # 添加定时触发
        trigger = CronTrigger.from_crontab(task_config['schedule'])
        self.scheduler.add_job(
            self._execute_with_deps,
            trigger=trigger,
            args=[task_config],
            id=name,
            name=task_config.get('description', name)
        )
    
    def _execute_with_deps(self, task_config):
        """执行任务（带依赖检查）"""
        name = task_config['name']
        depends_on = task_config.get('depends_on', [])
        
        # 检查依赖是否完成
        for dep in depends_on:
            dep_status = self.dependency_graph.get(dep, {}).get('status')
            if dep_status != 'success':
                logger.warning(f"{name} 依赖 {dep} 未完成，跳过执行")
                return
        
        # 执行前检查（跳过条件）
        if self._should_skip(task_config):
            logger.info(f"{name} 满足跳过条件，不执行")
            return
        
        # 执行任务
        self.dependency_graph[name]['status'] = 'running'
        result = self._run_task(task_config)
        
        # 更新状态
        if result:
            self.dependency_graph[name]['status'] = 'success'
        else:
            self.dependency_graph[name]['status'] = 'failed'
            self.dependency_graph[name]['retry_count'] += 1
            
            # 自动重试
            if self._should_retry(task_config, name):
                self._schedule_retry(task_config)
```

## 三、实现步骤

### 3.1 步骤1：安装依赖

```bash
pip install apscheduler pytz
```

### 3.2 步骤2：创建增强调度器

文件：`scripts/enhanced_scheduler.py`

```python
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
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from enum import Enum

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

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
    status: TaskStatus
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
                self.task_states[name] = TaskState(name=name, status=TaskStatus.PENDING)
    
    def _check_dependencies(self, task_name: str) -> bool:
        """检查依赖是否完成"""
        dependencies = self.dependency_graph.get(task_name, [])
        for dep in dependencies:
            dep_state = self.task_states.get(dep)
            if not dep_state or dep_state.status != TaskStatus.SUCCESS:
                logger.warning(f"⏸️ {task_name} 依赖 {dep} 未完成 (状态: {dep_state.status if dep_state else 'unknown'})")
                return False
        return True
    
    def _should_skip(self, task: Dict) -> bool:
        """检查是否应该跳过"""
        # 检查 skip_if_passed 条件
        skip_if_passed = task.get('skip_if_passed')
        if skip_if_passed:
            skip_task_state = self.task_states.get(skip_if_passed)
            if skip_task_state and skip_task_state.status == TaskStatus.SUCCESS:
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
            state.status = TaskStatus.SKIPPED
            state.error_message = "依赖未完成"
            self._save_state()
            return
        
        # 检查跳过条件
        if self._should_skip(task):
            state.status = TaskStatus.SKIPPED
            self._save_state()
            return
        
        # 执行任务
        state.status = TaskStatus.RUNNING
        state.last_run = datetime.now().isoformat()
        self._save_state()
        
        result = self._run_task(task)
        
        # 更新状态
        state.last_result = result
        state.duration_seconds = time.time() - datetime.fromisoformat(state.last_run).timestamp()
        
        if result:
            state.status = TaskStatus.SUCCESS
            state.retry_count = 0
            state.error_message = None
        else:
            if self._should_retry(task, name):
                state.status = TaskStatus.RETRYING
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
                state.status = TaskStatus.FAILED
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
        running = sum(1 for s in self.task_states.values() if s.status == TaskStatus.RUNNING)
        success = sum(1 for s in self.task_states.values() if s.status == TaskStatus.SUCCESS)
        failed = sum(1 for s in self.task_states.values() if s.status == TaskStatus.FAILED)
        
        logger.info(f"📊 状态: 运行中={running}, 成功={success}, 失败={failed}")


def main():
    """主函数"""
    scheduler = EnhancedScheduler()
    scheduler.start()


if __name__ == '__main__':
    main()
```

### 3.3 步骤3：创建启动脚本

文件：`scripts/start_scheduler.sh`

```bash
#!/bin/bash
# XCNStock 调度器启动脚本

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$PROJECT_DIR/data/scheduler.pid"
LOG_FILE="$PROJECT_DIR/logs/scheduler.log"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/data"

# 检查是否已在运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "调度器已在运行 (PID: $PID)"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

echo "🚀 启动 XCNStock 任务调度器..."

# 启动调度器
cd "$PROJECT_DIR"
nohup python3 scripts/enhanced_scheduler.py >> "$LOG_FILE" 2>&1 &

# 保存PID
echo $! > "$PID_FILE"
echo "✅ 调度器已启动 (PID: $(cat $PID_FILE))"
echo "📋 日志文件: $LOG_FILE"
```

文件：`scripts/stop_scheduler.sh`

```bash
#!/bin/bash
# XCNStock 调度器停止脚本

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$PROJECT_DIR/data/scheduler.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo "📴 停止调度器 (PID: $PID)..."
    kill "$PID" 2>/dev/null
    rm -f "$PID_FILE"
    echo "✅ 调度器已停止"
else
    echo "调度器未运行"
fi
```

### 3.4 步骤4：创建 Systemd 服务（Linux）

文件：`/etc/systemd/system/xcnstock-scheduler.service`

```ini
[Unit]
Description=XCNStock Task Scheduler
After=network.target

[Service]
Type=simple
User=xcnstock
WorkingDirectory=/app/xcnstock
Environment=PYTHONPATH=/app/xcnstock
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/usr/bin/python3 /app/xcnstock/scripts/enhanced_scheduler.py
ExecStop=/bin/kill -TERM $MAINPID
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启用服务：

```bash
sudo systemctl enable xcnstock-scheduler
sudo systemctl start xcnstock-scheduler
sudo systemctl status xcnstock-scheduler
```

### 3.5 步骤5：创建 Launchd 服务（macOS）

文件：`~/Library/LaunchAgents/com.xcnstock.scheduler.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.xcnstock.scheduler</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/python3</string>
        <string>/Volumes/Xdata/workstation/xxxcnstock/scripts/enhanced_scheduler.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Volumes/Xdata/workstation/xxxcnstock</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONPATH</key>
        <string>/Volumes/Xdata/workstation/xxxcnstock</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Volumes/Xdata/workstation/xxxcnstock/logs/scheduler.log</string>
    <key>StandardErrorPath</key>
    <string>/Volumes/Xdata/workstation/xxxcnstock/logs/scheduler_error.log</string>
</dict>
</plist>
```

启用服务：

```bash
launchctl load ~/Library/LaunchAgents/com.xcnstock.scheduler.plist
launchctl start com.xcnstock.scheduler
launchctl list | grep xcnstock
```

### 3.6 步骤6：Docker 部署

文件：`Dockerfile.scheduler`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY . .

# 创建必要目录
RUN mkdir -p /app/logs /app/data /app/data/checkpoints

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 启动调度器
CMD ["python3", "scripts/enhanced_scheduler.py"]
```

文件：`docker-compose.scheduler.yml`

```yaml
version: '3.8'

services:
  scheduler:
    build:
      context: .
      dockerfile: Dockerfile.scheduler
    container_name: xcnstock-scheduler
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    environment:
      - TZ=Asia/Shanghai
      - PYTHONPATH=/app
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"
```

启动：

```bash
docker-compose -f docker-compose.scheduler.yml up -d
```

## 四、监控与管理

### 4.1 查看任务状态

```bash
# 查看调度器状态
tail -f logs/scheduler.log

# 查看任务状态
cat data/scheduler_state.json | python3 -m json.tool
```

### 4.2 手动触发任务

```bash
# 手动执行单个任务
python3 scripts/pipeline/data_collect.py

# 手动执行带重试
python3 scripts/pipeline/data_collect.py --retry
```

### 4.3 任务状态监控

调度器会自动：
- ✅ 每5分钟打印任务状态
- ✅ 保存任务执行历史到 `data/scheduler_state.json`
- ✅ 记录详细日志到 `logs/scheduler.log`

## 五、故障处理

### 5.1 常见问题

| 问题 | 原因 | 解决 |
|-----|------|------|
| 任务不执行 | 依赖未完成 | 检查上游任务状态 |
| 任务超时 | 网络问题 | 增加 timeout 配置 |
| 重复执行 | 调度器多实例 | 检查 PID 文件 |
| 状态丢失 | 未正常关闭 | 使用信号正常关闭 |

### 5.2 重启调度器

```bash
# 停止
./scripts/stop_scheduler.sh

# 或强制停止
pkill -f enhanced_scheduler

# 启动
./scripts/start_scheduler.sh
```

## 六、总结

### 6.1 自动执行流程

```
1. 配置任务 (config/cron_tasks.yaml)
      │
      ▼
2. 启动调度器 (scripts/enhanced_scheduler.py)
      │
      ├─▶ 加载配置
      ├─▶ 加载状态
      ├─▶ 注册任务
      └─▶ 启动调度
      │
      ▼
3. 定时触发
      │
      ├─▶ 检查依赖
      ├─▶ 检查跳过条件
      ├─▶ 执行任务
      ├─▶ 更新状态
      └─▶ 失败重试
      │
      ▼
4. 监控与日志
```

### 6.2 核心特性

- ✅ **依赖管理** - 自动检查上游任务完成状态
- ✅ **失败重试** - 自动重试，指数退避
- ✅ **断点续传** - 任务状态持久化
- ✅ **跳过条件** - 支持条件跳过
- ✅ **状态监控** - 实时状态跟踪
- ✅ **信号处理** - 优雅关闭
