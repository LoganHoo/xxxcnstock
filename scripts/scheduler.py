#!/usr/bin/env python3
"""
XCNStock 统一调度器 (APScheduler)
从 config/cron_tasks.yaml 读取配置执行任务
支持 7x24 不间断调度，本地记录日志



改进版特性：
- 分布式锁控制
- 任务依赖管理
- 自动重试机制
- 执行历史记录
- 健康检查端点
- 优雅关闭
- 日志轮转
"""
import sys
import os
import yaml
import json
import time
import signal
import sqlite3
import subprocess
import logging
import threading
import tempfile
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, JobExecutionEvent
from logging.handlers import RotatingFileHandler

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / '.env')
except ImportError:
    pass

from core.distributed_lock import DistributedLock  # noqa: E402
import redis  # noqa: E402

# ==================== 配置 ====================
LOG_DIR = project_root / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / 'scheduler.log'

# 任务执行历史数据库
HISTORY_DB = project_root / 'data' / 'scheduler_history.db'
HISTORY_DB.parent.mkdir(exist_ok=True)
TASK_STATE_DIR = project_root / 'data' / 'tasks'
TASK_STATE_DIR.mkdir(parents=True, exist_ok=True)
PROGRESS_STATE_DIR = TASK_STATE_DIR / 'progress'
PROGRESS_STATE_DIR.mkdir(parents=True, exist_ok=True)
CIRCUIT_BREAKER_STATE_FILE = TASK_STATE_DIR / 'circuit_breaker_state.json'

# 全局状态 - 使用线程安全的数据结构
shutdown_event = threading.Event()
scheduler_instance = None
redis_pool = None

# 线程安全的任务状态管理
completed_tasks_lock = threading.RLock()
completed_tasks: Dict[str, str] = {}  # 受锁保护

# SQLite 访问锁，避免 APScheduler 多线程写入时出现 database is locked
history_db_lock = threading.RLock()
circuit_breaker_lock = threading.RLock()


# ==================== 日志配置 ====================
def setup_logging():
    """配置日志，支持轮转"""
    handlers = [
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout)
    ]

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )
    return logging.getLogger('scheduler')


logger = setup_logging()


# ==================== 数据模型 ====================
@dataclass
class TaskExecution:
    """任务执行记录"""
    task_name: str
    start_time: str
    end_time: Optional[str] = None
    status: str = 'running'  # running, success, failed, timeout
    duration_ms: int = 0
    output: str = ''
    error: str = ''
    retry_count: int = 0


# ==================== 数据库操作 ====================
class HistoryDB:
    """任务执行历史数据库"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS task_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT NOT NULL,
                    duration_ms INTEGER DEFAULT 0,
                    output TEXT,
                    error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_task_name ON task_executions(task_name)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_start_time ON task_executions(start_time)
            ''')
            conn.commit()

    def record_start(self, task_name: str) -> int:
        """记录任务开始，返回记录ID"""
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            cursor = conn.execute(
                'INSERT INTO task_executions (task_name, start_time, status) VALUES (?, ?, ?)',
                (task_name, datetime.now().isoformat(), 'running')
            )
            conn.commit()
            return cursor.lastrowid

    def record_complete(self, record_id: int, status: str, duration_ms: int,
                        output: str = '', error: str = '', retry_count: int = 0):
        """记录任务完成"""
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.execute('''
                UPDATE task_executions
                SET end_time = ?, status = ?, duration_ms = ?, output = ?, error = ?, retry_count = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), status, duration_ms, output, error, retry_count, record_id))
            conn.commit()

    def get_last_execution(self, task_name: str) -> Optional[Dict]:
        """获取任务上次执行记录"""
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                'SELECT * FROM task_executions WHERE task_name = ? ORDER BY start_time DESC LIMIT 1',
                (task_name,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_task_stats(self, task_name: str, days: int = 7) -> Dict:
        """获取任务统计信息"""
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            cursor = conn.execute('''
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) as timeout,
                    AVG(duration_ms) as avg_duration
                FROM task_executions
                WHERE task_name = ? AND start_time > datetime('now', '-{} days')
            '''.format(days), (task_name,))
            row = cursor.fetchone()
            return {
                'total': row[0] or 0,
                'success': row[1] or 0,
                'failed': row[2] or 0,
                'timeout': row[3] or 0,
                'avg_duration_ms': round(row[4], 2) if row[4] else 0
            }

    def has_successful_run_on_date(self, task_name: str, target_date: str) -> bool:
        """检查任务在指定日期是否已经成功执行过。"""
        day_start = f"{target_date}T00:00:00"
        day_end = f"{target_date}T23:59:59.999999"
        with history_db_lock, sqlite3.connect(self.db_path, timeout=30) as conn:
            cursor = conn.execute(
                '''
                SELECT 1
                FROM task_executions
                WHERE task_name = ?
                  AND status = 'success'
                  AND start_time >= ?
                  AND start_time <= ?
                LIMIT 1
                ''',
                (task_name, day_start, day_end),
            )
            return cursor.fetchone() is not None


# 全局数据库实例 - 延迟初始化
history_db: Optional[HistoryDB] = None


def get_history_db() -> HistoryDB:
    """获取数据库实例（线程安全）"""
    global history_db
    with history_db_lock:
        if history_db is None:
            history_db = HistoryDB(HISTORY_DB)
    return history_db


# ==================== Redis 连接池 ====================
def get_redis_client():
    """获取 Redis 客户端（使用连接池）"""
    global redis_pool
    if redis_pool is None:
        redis_pool = redis.ConnectionPool(
            host=os.getenv('REDIS_HOST', '49.233.10.199'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            password=os.getenv('REDIS_PASSWORD', '100200'),
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=5,
            max_connections=10
        )
    return redis.Redis(connection_pool=redis_pool)


# ==================== 配置加载 ====================
class UniqueKeySafeLoader(yaml.SafeLoader):
    """严格 YAML Loader，禁止重复键静默覆盖。"""


def _construct_mapping_without_duplicates(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            line_no = key_node.start_mark.line + 1
            raise ValueError(f"YAML 重复键: {key} (行 {line_no})")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_without_duplicates,
)


def load_yaml_with_unique_keys(config_path: Path) -> Dict:
    """加载 YAML，并在重复键时直接失败。"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.load(f, Loader=UniqueKeySafeLoader)


def load_cron_tasks() -> List[Dict]:
    """从 cron_tasks.yaml 加载任务配置"""
    config_path = project_root / 'config' / 'cron_tasks.yaml'
    try:
        config = load_yaml_with_unique_keys(config_path)

        tasks = config.get('tasks', [])
        global_config = config.get('global', {})
        environment_config = config.get('environment', {})

        # 合并全局配置到每个任务
        for task in tasks:
            task['_global'] = global_config
            task['_env_defaults'] = environment_config

        logger.info(f"✅ 加载 {len(tasks)} 个任务配置")
        return tasks
    except Exception as e:
        logger.error(f"❌ 加载 cron_tasks.yaml 失败: {e}")
        raise


# ==================== 通知机制 ====================
def send_notification(task_name: str, status: str, error: str = '', duration_ms: int = 0):
    """发送任务失败通知"""
    # 这里可以实现邮件、钉钉、企业微信等通知
    # 暂时只记录日志
    if status == 'failed':
        logger.error(f"🔔 任务失败通知: {task_name}, 错误: {error[:200]}")
    elif status == 'timeout':
        logger.error(f"🔔 任务超时通知: {task_name}, 耗时: {duration_ms}ms")


# ==================== 分布式锁 ====================
class TaskDistributedLock:
    """任务分布式锁 - 使用上下文管理器确保锁在任务执行期间保持"""
    
    def __init__(self, lock_key: str, timeout: int = 300):
        self.lock_key = lock_key
        self.timeout = timeout
        self.lock = None
        self.acquired = False
        self.redis_client = None
    
    def __enter__(self):
        """获取锁"""
        try:
            self.redis_client = get_redis_client()
            self.lock = DistributedLock(
                redis_client=self.redis_client,
                lock_key=f"scheduler:{self.lock_key}",
                ttl_seconds=self.timeout,
                auto_renew=True  # 自动续期，防止任务执行时间长导致锁过期
            )
            
            self.acquired = self.lock.acquire(blocking=False)
            if self.acquired:
                logger.info(f"🔒 获取分布式锁 [{self.lock_key}]")
                return self
            else:
                logger.info(f"🔒 分布式锁被占用 [{self.lock_key}]，跳过执行")
                return None
                
        except redis.ConnectionError:
            logger.warning(f"⚠️ Redis 不可用，以降级模式执行 [{self.lock_key}]")
            self.acquired = True  # 降级模式视为获取成功
            return self
        except Exception as e:
            logger.warning(f"⚠️ 锁获取异常: {e}，允许执行 [{self.lock_key}]")
            self.acquired = True  # 异常时降级执行
            return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """释放锁"""
        if self.lock and self.acquired and not isinstance(exc_val, redis.ConnectionError):
            try:
                self.lock.release()
                logger.info(f"🔓 释放分布式锁 [{self.lock_key}]")
            except Exception as e:
                logger.warning(f"⚠️ 锁释放异常: {e}")
        return False  # 不吞掉异常


def check_distributed_lock(lock_key: str, timeout: int = 300) -> bool:
    """检查分布式锁状态（兼容旧接口，但标记为废弃）"""
    logger.warning("⚠️ check_distributed_lock 已废弃，请使用 TaskDistributedLock 上下文管理器")
    try:
        redis_client = get_redis_client()
        lock = DistributedLock(
            redis_client=redis_client,
            lock_key=f"scheduler:{lock_key}",
            ttl_seconds=timeout,
            auto_renew=False
        )

        acquired = lock.acquire(blocking=False)
        if acquired:
            lock.release()
            return True
        else:
            logger.info(f"🔒 分布式锁被占用 [{lock_key}]，跳过执行")
            return False

    except redis.ConnectionError:
        logger.warning("⚠️ Redis 不可用，以降级模式执行")
        return True
    except Exception as e:
        logger.warning(f"⚠️ 锁检查异常: {e}，允许执行")
        return True


# ==================== 依赖检查 ====================
def check_dependencies(task: Dict) -> bool:
    """检查任务依赖是否满足 - 使用线程安全的状态访问"""
    depends_on = task.get('depends_on')
    if not depends_on:
        return True

    # 获取依赖任务的最新状态（线程安全）
    with completed_tasks_lock:
        dep_status = completed_tasks.get(depends_on)
    if dep_status == 'success':
        return True

    # 检查数据库中的最近执行记录
    try:
        db = get_history_db()
        last_exec = db.get_last_execution(depends_on)
        if last_exec and last_exec['status'] == 'success':
            # 检查是否在合理时间范围内（24小时内）
            exec_time = datetime.fromisoformat(last_exec['start_time'])
            time_diff = datetime.now() - exec_time
            if time_diff.total_seconds() < 86400:  # 24小时
                return True
    except Exception as e:
        logger.warning(f"⚠️ 检查依赖状态时出错: {e}")

    logger.warning(f"⏳ 任务 [{task['name']}] 依赖 [{depends_on}] 未满足，跳过执行")
    return False


def should_run_today(task_config: Dict, now: Optional[datetime] = None) -> bool:
    """按 day_type 和节假日配置判断任务今天是否应该执行。"""
    now = now or datetime.now()
    day_type = task_config.get('day_type', 'daily')
    global_config = task_config.get('_global', {})
    calendar_config = global_config.get('market_calendar', {})
    calendar_enabled = calendar_config.get('enabled', False)
    is_weekend = now.weekday() >= 5
    today_str = now.strftime('%Y-%m-%d')
    special_holidays = set(calendar_config.get('special_holidays', []))
    is_holiday = calendar_enabled and today_str in special_holidays

    if day_type == 'daily':
        return True
    if day_type == 'weekday':
        return not is_weekend and not is_holiday
    if day_type == 'weekend':
        return is_weekend
    return True


def build_task_env(task_config: Dict) -> Dict[str, str]:
    """合并任务环境变量，保持本地运行和容器运行都可兼容。"""
    env = os.environ.copy()
    env.update({k: str(v) for k, v in task_config.get('_env_defaults', {}).items()})
    env.update({k: str(v) for k, v in task_config.get('env', {}).items()})

    task_path = task_config.get('_global', {}).get('path')
    if task_path:
        env['PATH'] = str(task_path)

    return env


def resolve_task_cwd(task_config: Dict) -> str:
    """优先使用配置中的工作目录，若不存在则回退到项目根目录。"""
    configured_cwd = task_config.get('_global', {}).get('working_dir')
    if configured_cwd:
        cwd_path = Path(configured_cwd)
        if cwd_path.exists():
            return str(cwd_path)
        logger.warning(f"⚠️ 配置工作目录不存在，回退到项目根目录: {configured_cwd}")
    return str(project_root)


def get_progress_file_path(task_name: str) -> Path:
    """获取任务进度文件路径。"""
    return PROGRESS_STATE_DIR / f'{task_name}.json'


def evaluate_task_progress(task_config: Dict, now: Optional[datetime] = None) -> Dict:
    """评估任务进度是否停滞。"""
    progress_config = task_config.get('progress_check', {})
    now = now or datetime.now()
    result = {
        'enabled': progress_config.get('enabled', False),
        'is_stalled': False,
        'progress': None,
        'reason': '',
    }
    if not result['enabled']:
        return result

    progress_file = get_progress_file_path(task_config['name'])
    interval = int(progress_config.get('interval', 600))
    min_progress = float(progress_config.get('min_progress', 0))

    if not progress_file.exists():
        result['is_stalled'] = True
        result['reason'] = f'进度停滞: 未找到进度文件 {progress_file.name}'
        return result

    try:
        payload = json.loads(progress_file.read_text(encoding='utf-8'))
        progress = float(payload.get('progress', 0))
        updated_at = payload.get('updated_at')
        if not updated_at:
            result['is_stalled'] = True
            result['reason'] = '进度停滞: 缺少 updated_at'
            result['progress'] = progress
            return result

        updated_time = datetime.fromisoformat(updated_at)
        seconds_since_update = (now - updated_time).total_seconds()
        result['progress'] = progress

        if seconds_since_update > interval and progress < min_progress:
            result['is_stalled'] = True
            result['reason'] = (
                f'进度停滞: {seconds_since_update:.0f}s 未更新，当前进度 {progress}% 低于要求 {min_progress}%'
            )
        return result
    except Exception as e:
        result['is_stalled'] = True
        result['reason'] = f'进度停滞: 进度文件解析失败: {e}'
        return result


def load_circuit_breaker_state() -> Dict[str, Dict]:
    """读取熔断状态。"""
    with circuit_breaker_lock:
        if not CIRCUIT_BREAKER_STATE_FILE.exists():
            return {}
        try:
            return json.loads(CIRCUIT_BREAKER_STATE_FILE.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning(f"⚠️ 读取熔断状态失败，按空状态处理: {e}")
            return {}


def save_circuit_breaker_state(state: Dict[str, Dict]) -> None:
    """保存熔断状态。"""
    with circuit_breaker_lock:
        CIRCUIT_BREAKER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CIRCUIT_BREAKER_STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )


def record_circuit_breaker_failure(
    task_config: Dict,
    error: str,
    now: Optional[datetime] = None,
) -> None:
    """在任务失败后更新熔断状态。"""
    breaker_config = task_config.get('circuit_breaker', {})
    if not breaker_config.get('enabled', False):
        return

    now = now or datetime.now()
    task_name = task_config['name']
    threshold = int(breaker_config.get('failure_threshold', 3))
    state = load_circuit_breaker_state()
    task_state = state.get(task_name, {
        'failure_count': 0,
        'state': 'closed',
        'opened_at': None,
        'last_error': '',
    })
    task_state['failure_count'] += 1
    task_state['last_error'] = error[:500]
    if task_state['failure_count'] >= threshold:
        task_state['state'] = 'open'
        task_state['opened_at'] = now.isoformat()
    state[task_name] = task_state
    save_circuit_breaker_state(state)


def reset_circuit_breaker(task_name: str) -> None:
    """任务成功后重置熔断状态。"""
    state = load_circuit_breaker_state()
    if task_name in state:
        state[task_name] = {
            'failure_count': 0,
            'state': 'closed',
            'opened_at': None,
            'last_error': '',
        }
        save_circuit_breaker_state(state)


def should_skip_by_circuit_breaker(
    task_config: Dict,
    now: Optional[datetime] = None,
) -> Tuple[bool, Optional[str]]:
    """判断任务是否处于熔断打开状态。"""
    breaker_config = task_config.get('circuit_breaker', {})
    if not breaker_config.get('enabled', False):
        return False, None

    now = now or datetime.now()
    recovery_timeout = int(breaker_config.get('recovery_timeout', 3600))
    task_state = load_circuit_breaker_state().get(task_config['name'])
    if not task_state or task_state.get('state') != 'open':
        return False, None

    opened_at = task_state.get('opened_at')
    if not opened_at:
        return False, None

    elapsed = (now - datetime.fromisoformat(opened_at)).total_seconds()
    if elapsed < recovery_timeout:
        return True, task_state.get('last_error')

    task_state['state'] = 'half_open'
    state = load_circuit_breaker_state()
    state[task_config['name']] = task_state
    save_circuit_breaker_state(state)
    return False, None


def has_successful_run_today(task_name: str, now: Optional[datetime] = None) -> bool:
    """检查任务今天是否已经成功执行过。"""
    now = now or datetime.now()
    return get_history_db().has_successful_run_on_date(task_name, now.date().isoformat())


def update_task_status(name: str, status: str) -> None:
    """线程安全地更新任务状态"""
    with completed_tasks_lock:
        completed_tasks[name] = status


def get_task_status(name: str) -> Optional[str]:
    """线程安全地获取任务状态"""
    with completed_tasks_lock:
        return completed_tasks.get(name)


# ==================== 子进程执行（带资源管理）====================
def run_subprocess_with_timeout(
    cmd: List[str],
    timeout: int,
    cwd: str,
    env: Optional[Dict[str, str]] = None,
    task_name: Optional[str] = None,
    progress_config: Optional[Dict] = None,
    max_output_size: int = 10 * 1024 * 1024  # 10MB
) -> tuple[int, str, str]:
    """
    执行子进程，带资源管理和输出限制
    
    Returns:
        (returncode, stdout, stderr)
    """
    process = None
    try:
        with tempfile.TemporaryFile(mode='w+t', encoding='utf-8') as stdout_file, tempfile.TemporaryFile(
            mode='w+t', encoding='utf-8'
        ) as stderr_file:
            # 使用临时文件避免子进程输出阻塞，同时允许轮询进度
            process = subprocess.Popen(
                cmd,
                stdout=stdout_file,
                stderr=stderr_file,
                cwd=cwd,
                env=env,
                text=True,
                bufsize=1
            )

            start_time = time.time()
            while True:
                if process.poll() is not None:
                    break

                elapsed = time.time() - start_time
                if elapsed > timeout:
                    raise subprocess.TimeoutExpired(cmd, timeout)

                if progress_config and progress_config.get('enabled') and task_name:
                    progress_status = evaluate_task_progress(
                        {'name': task_name, 'progress_check': progress_config},
                    )
                    if progress_status['is_stalled']:
                        raise RuntimeError(progress_status['reason'])

                time.sleep(1)

            stdout_file.seek(0)
            stderr_file.seek(0)
            stdout = stdout_file.read()
            stderr = stderr_file.read()
        
        # 限制输出大小
        if len(stdout) > max_output_size:
            stdout = stdout[:max_output_size] + f"\n... [截断，总大小: {len(stdout)} bytes]"
        if len(stderr) > max_output_size:
            stderr = stderr[:max_output_size] + f"\n... [截断，总大小: {len(stderr)} bytes]"
        
        return process.returncode, stdout, stderr
        
    except subprocess.TimeoutExpired:
        # 超时终止进程
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise
    except Exception:
        # 确保进程被终止
        if process:
            try:
                process.kill()
                process.wait()
            except Exception:
                pass
        raise


# ==================== 任务执行 ====================
def run_task(task_config: Dict) -> bool:
    """
    执行单个任务（带重试、锁、依赖检查）
    
    修复点：
    1. 使用 TaskDistributedLock 上下文管理器确保锁在任务执行期间保持
    2. 使用线程安全的 completed_tasks 访问
    3. 改进异常处理，区分可重试和致命错误
    4. 子进程资源管理
    5. 指数退避上限
    """
    name = task_config['name']
    script = task_config['script']
    args = task_config.get('args', [])
    timeout = task_config.get('timeout', 600)
    max_retries = task_config.get('_global', {}).get('max_retries', 3)
    retry_delay = task_config.get('_global', {}).get('retry_delay', 60)
    retry_backoff = task_config.get('_global', {}).get('retry_backoff', 2)
    max_backoff = task_config.get('_global', {}).get('max_backoff', 600)  # 最大退避时间
    alert_on_failure = task_config.get('alert_on_failure', False)
    global_config = task_config.get('_global', {})
    use_lock = global_config.get('use_redis_lock', True)
    lock_timeout = int(global_config.get('redis_lock_timeout', timeout) or timeout)
    lock_timeout = max(1, min(lock_timeout, timeout))
    progress_config = task_config.get('progress_check', {})

    # 检查是否正在关闭
    if shutdown_event.is_set():
        logger.info(f"⏹️ 系统正在关闭，跳过任务: {name}")
        return False

    if not should_run_today(task_config):
        logger.info(f"📅 今日不满足执行日历，跳过任务: {name}")
        return False

    # 检查依赖
    if not check_dependencies(task_config):
        return False

    if task_config.get('run_once') and has_successful_run_today(name):
        logger.info(f"🔁 任务 [{name}] 已在今日成功执行过一次，按 run_once 语义跳过")
        return False

    should_skip, breaker_error = should_skip_by_circuit_breaker(task_config)
    if should_skip:
        logger.warning(f"⛔ 任务 [{name}] 处于熔断状态，跳过执行")
        fallback_script = task_config.get('circuit_breaker', {}).get('fallback_script')
        if fallback_script:
            logger.info(f"🪂 执行熔断兜底脚本: {fallback_script}")
            fallback_task = dict(task_config)
            fallback_task['script'] = fallback_script
            fallback_task['args'] = []
            fallback_task['circuit_breaker'] = {}
            fallback_task['run_once'] = False
            fallback_task['progress_check'] = {}
            fallback_task['name'] = f"{name}__fallback"
            fallback_task['_global'] = dict(global_config, use_redis_lock=False)
            return run_task(fallback_task)
        if breaker_error:
            logger.warning(f"⛔ 熔断原因: {breaker_error}")
        return False

    lock_context = TaskDistributedLock(name, lock_timeout) if use_lock else nullcontext(True)

    # 使用新的分布式锁上下文管理器
    with lock_context as lock:
        if lock is None:
            # 锁被占用，跳过执行
            return False
        
        # 记录开始（使用数据库队列）
        record_id = None
        try:
            db = get_history_db()
            record_id = db.record_start(name)
        except Exception as e:
            logger.warning(f"⚠️ 记录任务开始失败: {e}")
        
        start_time = time.time()
        retry_count = 0
        last_error = ''
        last_output = ''
        
        logger.info(f"▶️ 开始执行: {name}")

        # 重试循环
        while retry_count <= max_retries:
            if retry_count > 0:
                # 计算退避时间（带上限）
                backoff = min(retry_delay * (retry_backoff ** (retry_count - 1)), max_backoff)
                logger.info(f"🔄 第 {retry_count}/{max_retries} 次重试: {name} (等待 {backoff}s)")
                time.sleep(backoff)

            try:
                script_path = project_root / script
                if not script_path.exists():
                    raise FileNotFoundError(f"脚本不存在: {script}")

                # 构建命令
                if script.endswith('.sh'):
                    cmd = ['/bin/bash', str(script_path)]
                elif script.endswith('.py'):
                    cmd = [sys.executable, str(script_path)]
                else:
                    cmd = [str(script_path)]

                if args:
                    cmd.extend(args)

                # 执行（带资源管理）
                returncode, stdout, stderr = run_subprocess_with_timeout(
                    cmd,
                    timeout,
                    resolve_task_cwd(task_config),
                    env=build_task_env(task_config),
                    task_name=name,
                    progress_config=progress_config,
                )

                duration_ms = int((time.time() - start_time) * 1000)
                last_output = stdout
                last_error = stderr

                # 记录输出到日志（限制长度）
                if stdout:
                    logger.info(f"📤 [{name}] 输出:\n{stdout[:2000]}")
                if stderr:
                    logger.warning(f"⚠️ [{name}] 错误输出:\n{stderr[:1000]}")

                if returncode == 0:
                    # 成功
                    if record_id:
                        try:
                            db.record_complete(
                                record_id, 'success', duration_ms,
                                output=last_output[:5000],
                                error=last_error[:2000],
                                retry_count=retry_count
                            )
                        except Exception as e:
                            logger.warning(f"⚠️ 记录任务完成失败: {e}")
                    
                    update_task_status(name, 'success')
                    reset_circuit_breaker(name)
                    logger.info(f"✅ 完成: {name} (耗时: {duration_ms}ms)")
                    return True
                else:
                    # 失败，准备重试
                    last_error = f"Exit code: {returncode}, stderr: {stderr[:500]}"
                    logger.error(f"❌ 失败: {name} - {last_error}")
                    retry_count += 1

            except subprocess.TimeoutExpired:
                duration_ms = int((time.time() - start_time) * 1000)
                last_error = f"Timeout after {timeout}s"
                logger.error(f"⏱️ 超时: {name} (>{timeout}s)")

                if record_id:
                    try:
                        db.record_complete(
                            record_id, 'timeout', duration_ms,
                            output=last_output[:5000],
                            error=last_error,
                            retry_count=retry_count
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ 记录任务超时失败: {e}")

                if alert_on_failure:
                    send_notification(name, 'timeout', duration_ms=duration_ms)

                record_circuit_breaker_failure(task_config, last_error)
                update_task_status(name, 'timeout')
                return False

            except (FileNotFoundError, PermissionError) as e:
                # 致命错误，不重试
                last_error = str(e)
                logger.error(f"❌ 致命错误: {name} - {e}")
                break

            except Exception as e:
                # 可重试错误
                last_error = str(e)
                logger.error(f"❌ 异常: {name} - {e}")
                retry_count += 1

        # 重试耗尽，最终失败
        duration_ms = int((time.time() - start_time) * 1000)
        
        if record_id:
            try:
                db.record_complete(
                    record_id, 'failed', duration_ms,
                    output=last_output[:5000],
                    error=last_error,
                    retry_count=max(0, retry_count - 1)
                )
            except Exception as e:
                logger.warning(f"⚠️ 记录任务失败状态失败: {e}")

        if alert_on_failure:
            send_notification(name, 'failed', error=last_error)

        record_circuit_breaker_failure(task_config, last_error)
        update_task_status(name, 'failed')
        return False


# ==================== 事件监听 ====================
def job_executed_listener(event: JobExecutionEvent):
    """任务执行完成监听"""
    if event.exception:
        logger.error(f"❌ 任务 {event.job_id} 执行异常: {event.exception}")
    else:
        logger.debug(f"✅ 任务 {event.job_id} 执行完成")


def job_error_listener(event: JobExecutionEvent):
    """任务执行错误监听"""
    logger.error(f"🔥 任务 {event.job_id} 执行错误: {event.exception}")


# ==================== 健康检查 HTTP 服务 ====================
class HealthHandler(BaseHTTPRequestHandler):
    """健康检查处理器"""

    def log_message(self, format, *args):
        # 静默日志
        pass

    def do_GET(self):
        if self.path == '/health':
            self._send_health_response()
        elif self.path == '/tasks':
            self._send_tasks_response()
        elif self.path == '/stats':
            self._send_stats_response()
        else:
            self.send_error(404)

    def _send_health_response(self):
        """健康状态 - 线程安全地访问共享状态"""
        with completed_tasks_lock:
            task_count = len(completed_tasks)
        
        status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'scheduler_running': scheduler_instance is not None and scheduler_instance.running,
            'completed_tasks_count': task_count
        }
        self._send_json(status)

    def _send_tasks_response(self):
        """任务列表"""
        tasks = load_cron_tasks()
        enabled_tasks = [t for t in tasks if t.get('enabled', True)]
        self._send_json({
            'total': len(tasks),
            'enabled': len(enabled_tasks),
            'tasks': [{'name': t['name'], 'schedule': t.get('schedule'), 'enabled': t.get('enabled', True)}
                      for t in enabled_tasks[:20]]  # 只显示前20个
        })

    def _send_stats_response(self):
        """统计信息"""
        tasks = load_cron_tasks()
        db = get_history_db()
        stats = {}
        for task in tasks[:10]:  # 只显示前10个任务的统计
            task_stats = db.get_task_stats(task['name'], days=7)
            stats[task['name']] = task_stats
        self._send_json(stats)

    def _send_json(self, data: dict):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())


def start_health_server(port: int = 8080):
    """启动健康检查服务"""
    try:
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"✅ 健康检查服务启动: http://0.0.0.0:{port}/health")
        return server
    except Exception as e:
        logger.warning(f"⚠️ 健康检查服务启动失败: {e}")
        return None


# ==================== 信号处理 ====================
def signal_handler(signum, _frame):
    """
    信号处理 - 优雅关闭
    
    修复点：
    1. 不在信号处理中调用 sys.exit()
    2. 使用全局标志通知主循环退出
    3. 异步关闭调度器
    """
    sig_name = signal.Signals(signum).name
    logger.info(f"\n🛑 收到信号 {sig_name}，开始优雅关闭...")
    shutdown_event.set()

    # 异步关闭调度器（避免在信号处理中阻塞）
    def shutdown_scheduler():
        if scheduler_instance:
            try:
                scheduler_instance.shutdown(wait=True)
                logger.info("✅ 调度器已关闭")
            except Exception as e:
                logger.error(f"❌ 调度器关闭异常: {e}")
    
    # 在单独线程中执行关闭
    shutdown_thread = threading.Thread(target=shutdown_scheduler, daemon=True)
    shutdown_thread.start()
    
    # 给关闭操作一些时间
    shutdown_thread.join(timeout=30)
    
    # 不在这里调用 sys.exit()，让程序自然退出


# ==================== 主函数 ====================
def main():
    """主函数"""
    global scheduler_instance

    # 注册信号处理
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("=" * 60)
    logger.info("XCNStock 调度器启动 (改进版 - 已修复线程安全和分布式锁问题)")
    logger.info(f"日志文件: {LOG_FILE}")
    logger.info(f"历史数据库: {HISTORY_DB}")
    logger.info("=" * 60)

    # 启动健康检查服务
    health_server = start_health_server(port=8080)

    # 加载任务
    tasks = load_cron_tasks()
    if not tasks:
        logger.error("⚠️ 没有加载到任何任务")
        return 1

    # 创建调度器
    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    scheduler_instance = scheduler

    # 添加事件监听
    scheduler.add_listener(job_executed_listener, EVENT_JOB_EXECUTED)
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

    registered_count = 0

    for task in tasks:
        if not task.get('enabled', True):
            continue

        name = task.get('name')
        schedule = task.get('schedule')

        if not name or not schedule:
            logger.warning(f"⚠️ 任务配置不完整: {task}")
            continue

        try:
            trigger = CronTrigger.from_crontab(schedule)
            scheduler.add_job(
                run_task,
                trigger=trigger,
                args=[task],
                id=name,
                name=task.get('description', name),
                replace_existing=True,
                max_instances=1  # 同一任务同时只能执行一个实例
            )
            logger.info(f"✅ 已注册任务: {name} ({schedule})")
            registered_count += 1
        except Exception as e:
            logger.error(f"❌ 注册任务失败 {name}: {e}")

    logger.info(f"\n调度器已启动，共 {registered_count} 个任务")
    logger.info("健康检查: http://localhost:8080/health")
    logger.info("任务列表: http://localhost:8080/tasks")
    logger.info("统计信息: http://localhost:8080/stats")
    logger.info("按 Ctrl+C 停止\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，开始关闭...")
        shutdown_event.set()
    except Exception as e:
        logger.error(f"调度器异常: {e}")
        return 1
    finally:
        # 确保资源被清理
        logger.info("正在清理资源...")
        
        # 关闭健康检查服务
        if health_server:
            try:
                health_server.shutdown()
                logger.info("✅ 健康检查服务已停止")
            except Exception as e:
                logger.warning(f"⚠️ 健康检查服务关闭异常: {e}")
        
        logger.info("✅ 调度器已完全关闭")

    return 0


if __name__ == '__main__':
    sys.exit(main())
