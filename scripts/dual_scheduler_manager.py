#!/usr/bin/env python3
"""
XCNStock 双调度器主备管理器
Kestra + APScheduler 主备架构

架构设计:
- Kestra: 主调度器 (Primary)
- APScheduler: 备调度器 (Backup)
- 两者互备，状态同步，自动切换

主备判定逻辑:
1. 健康检查: 每30秒检查一次双方状态
2. 主备切换: 当主调度器连续3次检查失败，备调度器接管
3. 任务执行: 只有主调度器执行任务，备调度器处于待命状态
4. 状态同步: 通过Redis共享任务执行状态
"""

import sys
import os
import json
import time
import redis
import yaml
import requests
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from threading import Thread, Lock
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SchedulerRole(Enum):
    """调度器角色"""
    PRIMARY = "primary"      # 主调度器
    BACKUP = "backup"        # 备调度器
    UNKNOWN = "unknown"      # 未知


class SchedulerStatus(Enum):
    """调度器状态"""
    HEALTHY = "healthy"      # 健康
    DEGRADED = "degraded"    # 降级
    DOWN = "down"            # 宕机
    UNKNOWN = "unknown"      # 未知


@dataclass
class SchedulerState:
    """调度器状态"""
    name: str                                    # 调度器名称
    role: str                                    # 当前角色
    status: str                                  # 健康状态
    last_heartbeat: str                          # 最后心跳时间
    active_tasks: int                            # 活跃任务数
    failed_checks: int                           # 连续失败检查次数
    is_leader: bool                              # 是否是Leader
    version: str = "1.0.0"                       # 版本
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchedulerState':
        return cls(**data)


class DualSchedulerManager:
    """双调度器管理器"""
    
    # Redis键前缀
    REDIS_KEY_PREFIX = "xcnstock:scheduler:"
    HEARTBEAT_KEY = f"{REDIS_KEY_PREFIX}heartbeat"
    STATE_KEY = f"{REDIS_KEY_PREFIX}state"
    LEADER_KEY = f"{REDIS_KEY_PREFIX}leader"
    LOCK_KEY = f"{REDIS_KEY_PREFIX}lock"
    TASK_STATUS_KEY = f"{REDIS_KEY_PREFIX}task_status"
    
    # 配置
    HEARTBEAT_INTERVAL = 30      # 心跳间隔(秒)
    HEALTH_CHECK_INTERVAL = 30   # 健康检查间隔(秒)
    FAILOVER_THRESHOLD = 3       # 故障转移阈值(连续失败次数)
    LEADER_LOCK_TTL = 60         # Leader锁过期时间(秒)
    
    def __init__(self, scheduler_type: str = "kestra"):
        """
        初始化双调度器管理器
        
        Args:
            scheduler_type: 当前调度器类型 ("kestra" 或 "apscheduler")
        """
        self.scheduler_type = scheduler_type
        self.scheduler_name = f"{scheduler_type}_{os.getpid()}"
        self.role = SchedulerRole.UNKNOWN
        self.status = SchedulerStatus.UNKNOWN
        self.is_running = False
        self.lock = Lock()
        
        # 加载配置
        self.config = self._load_config()
        
        # 初始化Redis连接
        self.redis_client = self._init_redis()
        
        # 连续失败计数
        self.failed_checks = 0
        
        logger.info(f"[{self.scheduler_name}] 双调度器管理器初始化完成")
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        config_path = project_root / 'config' / 'dual_scheduler.yaml'
        default_config = {
            'kestra': {
                'api_url': 'http://localhost:8082/api/v1',
                'health_endpoint': '/health',
                'timeout': 5
            },
            'apscheduler': {
                'heartbeat_file': '/app/logs/scheduler_heartbeat',
                'state_file': '/app/logs/task_states.json',
                'timeout': 60
            },
            'redis': {
                'host': 'localhost',
                'port': 6379,
                'db': 0,
                'password': None
            },
            'failover': {
                'enabled': True,
                'threshold': 3,
                'auto_recover': True
            }
        }
        
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                logger.info(f"✅ 加载配置文件: {config_path}")
                return config
            except Exception as e:
                logger.error(f"❌ 加载配置失败: {e}，使用默认配置")
        
        return default_config
    
    def _init_redis(self) -> Optional[redis.Redis]:
        """初始化Redis连接"""
        try:
            redis_config = self.config.get('redis', {})
            client = redis.Redis(
                host=redis_config.get('host', 'localhost'),
                port=redis_config.get('port', 6379),
                db=redis_config.get('db', 0),
                password=redis_config.get('password'),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            client.ping()
            logger.info("✅ Redis连接成功")
            return client
        except Exception as e:
            logger.error(f"❌ Redis连接失败: {e}")
            return None
    
    def _acquire_leader_lock(self) -> bool:
        """尝试获取Leader锁"""
        if not self.redis_client:
            return False
        
        try:
            # 使用Redis SET NX EX 实现分布式锁
            acquired = self.redis_client.set(
                self.LEADER_KEY,
                self.scheduler_name,
                nx=True,  # 仅当key不存在时才设置
                ex=self.LEADER_LOCK_TTL  # 设置过期时间
            )
            
            if acquired:
                logger.info(f"[{self.scheduler_name}] ✅ 成功获取Leader锁")
                return True
            else:
                # 检查当前Leader是否是自己
                current_leader = self.redis_client.get(self.LEADER_KEY)
                if current_leader == self.scheduler_name:
                    # 续期
                    self.redis_client.expire(self.LEADER_KEY, self.LEADER_LOCK_TTL)
                    return True
                return False
        except Exception as e:
            logger.error(f"获取Leader锁失败: {e}")
            return False
    
    def _release_leader_lock(self):
        """释放Leader锁"""
        if not self.redis_client:
            return
        
        try:
            current_leader = self.redis_client.get(self.LEADER_KEY)
            if current_leader == self.scheduler_name:
                self.redis_client.delete(self.LEADER_KEY)
                logger.info(f"[{self.scheduler_name}] ✅ 已释放Leader锁")
        except Exception as e:
            logger.error(f"释放Leader锁失败: {e}")
    
    def _check_kestra_health(self) -> SchedulerStatus:
        """检查Kestra健康状态"""
        try:
            kestra_config = self.config.get('kestra', {})
            api_url = kestra_config.get('api_url', 'http://localhost:8082/api/v1')
            timeout = kestra_config.get('timeout', 5)
            
            # 检查API健康
            response = requests.get(
                f"{api_url}/health",
                timeout=timeout
            )
            
            if response.status_code == 200:
                return SchedulerStatus.HEALTHY
            else:
                return SchedulerStatus.DEGRADED
        except requests.exceptions.ConnectionError:
            return SchedulerStatus.DOWN
        except Exception as e:
            logger.warning(f"Kestra健康检查异常: {e}")
            return SchedulerStatus.UNKNOWN
    
    def _check_apscheduler_health(self) -> SchedulerStatus:
        """检查APScheduler健康状态"""
        try:
            aps_config = self.config.get('apscheduler', {})
            heartbeat_file = Path(aps_config.get('heartbeat_file', '/app/logs/scheduler_heartbeat'))
            timeout = aps_config.get('timeout', 60)
            
            if not heartbeat_file.exists():
                return SchedulerStatus.DOWN
            
            # 读取心跳时间
            with open(heartbeat_file, 'r') as f:
                heartbeat_str = f.read().strip()
            
            heartbeat_time = datetime.strptime(heartbeat_str, '%Y-%m-%d %H:%M:%S')
            time_diff = (datetime.now() - heartbeat_time).total_seconds()
            
            if time_diff > timeout:
                return SchedulerStatus.DOWN
            elif time_diff > timeout / 2:
                return SchedulerStatus.DEGRADED
            else:
                return SchedulerStatus.HEALTHY
        except Exception as e:
            logger.warning(f"APScheduler健康检查异常: {e}")
            return SchedulerStatus.UNKNOWN
    
    def _check_peer_health(self) -> SchedulerStatus:
        """检查对方调度器健康状态"""
        if self.scheduler_type == "kestra":
            return self._check_apscheduler_health()
        else:
            return self._check_kestra_health()
    
    def _update_heartbeat(self):
        """更新心跳"""
        if not self.redis_client:
            return
        
        try:
            state = SchedulerState(
                name=self.scheduler_name,
                role=self.role.value,
                status=self.status.value,
                last_heartbeat=datetime.now().isoformat(),
                active_tasks=0,  # 可从实际调度器获取
                failed_checks=self.failed_checks,
                is_leader=self.role == SchedulerRole.PRIMARY
            )
            
            # 保存到Redis
            self.redis_client.hset(
                self.HEARTBEAT_KEY,
                self.scheduler_name,
                json.dumps(state.to_dict())
            )
            
            # 设置过期时间
            self.redis_client.expire(self.HEARTBEAT_KEY, self.LEADER_LOCK_TTL * 2)
        except Exception as e:
            logger.error(f"更新心跳失败: {e}")
    
    def _get_peer_state(self) -> Optional[SchedulerState]:
        """获取对方调度器状态"""
        if not self.redis_client:
            return None
        
        try:
            peer_type = "apscheduler" if self.scheduler_type == "kestra" else "kestra"
            
            # 获取所有心跳记录
            all_states = self.redis_client.hgetall(self.HEARTBEAT_KEY)
            
            for name, state_json in all_states.items():
                if peer_type in name:
                    return SchedulerState.from_dict(json.loads(state_json))
            
            return None
        except Exception as e:
            logger.error(f"获取对方状态失败: {e}")
            return None
    
    def _should_take_over(self) -> bool:
        """判断是否应该接管主调度器角色"""
        peer_state = self._get_peer_state()
        
        if not peer_state:
            # 对方无状态，可能是首次启动
            logger.info("对方调度器无状态记录，尝试接管")
            return True
        
        # 检查对方状态
        if peer_state.status == SchedulerStatus.DOWN.value:
            self.failed_checks += 1
            logger.warning(f"对方调度器宕机，连续失败次数: {self.failed_checks}")
        elif peer_state.status == SchedulerStatus.UNKNOWN.value:
            self.failed_checks += 1
            logger.warning(f"对方调度器状态未知，连续失败次数: {self.failed_checks}")
        else:
            # 对方健康，重置失败计数
            if self.failed_checks > 0:
                logger.info("对方调度器恢复健康，重置失败计数")
            self.failed_checks = 0
        
        # 超过阈值，应该接管
        return self.failed_checks >= self.FAILOVER_THRESHOLD
    
    def _determine_role(self):
        """确定当前角色 - Kestra优先策略"""
        with self.lock:
            # Kestra优先策略：
            # 1. Kestra健康时，始终是主调度器
            # 2. Kestra宕机时，APScheduler接管
            # 3. Kestra恢复后，自动切回Kestra
            
            if self.scheduler_type == "kestra":
                # Kestra逻辑：只要健康就是主调度器
                if self.status == SchedulerStatus.HEALTHY:
                    if self._acquire_leader_lock():
                        self.role = SchedulerRole.PRIMARY
                        logger.info("✅ Kestra健康，作为主调度器运行")
                    else:
                        # 锁被占用，检查是否是APScheduler
                        current_leader = self.redis_client.get(self.LEADER_KEY)
                        if current_leader and "apscheduler" in str(current_leader):
                            # APScheduler是Leader，强制夺回
                            logger.warning("🔄 Kestra已恢复，从APScheduler夺回Leader角色")
                            self.redis_client.delete(self.LEADER_KEY)
                            if self._acquire_leader_lock():
                                self.role = SchedulerRole.PRIMARY
                else:
                    # Kestra不健康，降级为备
                    self.role = SchedulerRole.BACKUP
                    self._release_leader_lock()
                    logger.warning(f"⚠️ Kestra状态异常({self.status.value})，降级为备调度器")
            
            else:
                # APScheduler逻辑：只有Kestra宕机时才接管
                kestra_state = self._get_peer_state()
                
                if kestra_state:
                    if kestra_state.status == SchedulerStatus.HEALTHY.value:
                        # Kestra健康，APScheduler永远是备
                        self.role = SchedulerRole.BACKUP
                        self._release_leader_lock()
                        if self.role != SchedulerRole.BACKUP:
                            logger.info("Kestra健康运行中，APScheduler保持备状态")
                        return
                    elif kestra_state.status in [SchedulerStatus.DOWN.value, SchedulerStatus.UNKNOWN.value]:
                        # Kestra宕机，检查是否应该接管
                        if self._should_take_over():
                            logger.warning("🚨 Kestra连续失败，APScheduler接管Leader角色")
                            self.redis_client.delete(self.LEADER_KEY)
                            if self._acquire_leader_lock():
                                self.role = SchedulerRole.PRIMARY
                                self.failed_checks = 0
                        else:
                            self.role = SchedulerRole.BACKUP
                    else:
                        # Kestra降级状态，保持观察
                        self.role = SchedulerRole.BACKUP
                else:
                    # 没有Kestra状态，可能是首次启动
                    # APScheduler不主动争抢，等待Kestra
                    logger.info("等待Kestra启动...")
                    self.role = SchedulerRole.BACKUP
    
    def _health_check_loop(self):
        """健康检查循环"""
        while self.is_running:
            try:
                # 检查自身健康
                if self.scheduler_type == "kestra":
                    self.status = self._check_kestra_health()
                else:
                    self.status = self._check_apscheduler_health()
                
                # 确定角色
                self._determine_role()
                
                # 更新心跳
                self._update_heartbeat()
                
                logger.debug(f"健康检查完成 - 角色: {self.role.value}, 状态: {self.status.value}")
            except Exception as e:
                logger.error(f"健康检查异常: {e}")
            
            time.sleep(self.HEALTH_CHECK_INTERVAL)
    
    def is_leader(self) -> bool:
        """检查当前是否是Leader"""
        return self.role == SchedulerRole.PRIMARY
    
    def can_execute_task(self, task_name: str) -> bool:
        """
        检查是否可以执行任务
        
        只有主调度器(Leader)才能执行任务
        备调度器返回False，任务会被跳过
        """
        # 首先检查自身角色
        if not self.is_leader():
            return False
        
        # 检查任务是否已在对方执行
        if self.redis_client:
            try:
                task_key = f"{self.TASK_STATUS_KEY}:{task_name}:{datetime.now().strftime('%Y%m%d')}"
                task_status = self.redis_client.get(task_key)
                
                if task_status:
                    status_data = json.loads(task_status)
                    if status_data.get('status') == 'completed':
                        logger.info(f"任务 {task_name} 已在对方执行完成，跳过")
                        return False
            except Exception as e:
                logger.error(f"检查任务状态失败: {e}")
        
        return True
    
    def mark_task_executed(self, task_name: str, result: Dict[str, Any]):
        """标记任务已执行"""
        if not self.redis_client:
            return
        
        try:
            task_key = f"{self.TASK_STATUS_KEY}:{task_name}:{datetime.now().strftime('%Y%m%d')}"
            task_data = {
                'status': 'completed',
                'scheduler': self.scheduler_name,
                'executed_at': datetime.now().isoformat(),
                'result': result
            }
            
            self.redis_client.setex(
                task_key,
                86400 * 7,  # 保留7天
                json.dumps(task_data)
            )
        except Exception as e:
            logger.error(f"标记任务状态失败: {e}")
    
    def start(self):
        """启动双调度器管理器"""
        self.is_running = True
        
        # 启动健康检查线程
        health_thread = Thread(target=self._health_check_loop, daemon=True)
        health_thread.start()
        
        logger.info(f"[{self.scheduler_name}] 双调度器管理器已启动")
        
        # 如果是APScheduler，启动调度器
        if self.scheduler_type == "apscheduler":
            self._start_apscheduler()
    
    def _start_apscheduler(self):
        """启动APScheduler调度器"""
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
        
        scheduler = BlockingScheduler(timezone='Asia/Shanghai')
        
        # 加载任务配置
        cron_config = self._load_cron_config()
        if not cron_config:
            logger.error("无法加载cron配置")
            return
        
        for task in cron_config.get('tasks', []):
            if not task.get('enabled', True):
                continue
            
            name = task['name']
            schedule = task['schedule']
            script = task['script']
            
            def run_task(task_name=name, task_script=script, task_config=task):
                # 检查是否可以执行
                if not self.can_execute_task(task_name):
                    logger.info(f"[{self.scheduler_name}] 跳过任务 {task_name} (非Leader)")
                    return
                
                logger.info(f"[{self.scheduler_name}] ▶️ 执行任务: {task_name}")
                
                try:
                    script_path = project_root / task_script
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True,
                        text=True,
                        timeout=task_config.get('timeout', 600),
                        cwd=str(project_root)
                    )
                    
                    success = result.returncode == 0
                    
                    # 标记任务已执行
                    self.mark_task_executed(task_name, {
                        'success': success,
                        'returncode': result.returncode,
                        'stdout': result.stdout[:1000] if success else None,
                        'stderr': result.stderr[:500] if not success else None
                    })
                    
                    if success:
                        logger.info(f"[{self.scheduler_name}] ✅ 任务完成: {task_name}")
                    else:
                        logger.error(f"[{self.scheduler_name}] ❌ 任务失败: {task_name}")
                except Exception as e:
                    logger.error(f"[{self.scheduler_name}] ❌ 任务异常: {task_name} - {e}")
            
            try:
                trigger = CronTrigger.from_crontab(schedule)
                scheduler.add_job(
                    run_task,
                    trigger=trigger,
                    id=name,
                    name=task.get('description', name),
                    replace_existing=True
                )
                logger.info(f"✅ 已注册任务: {name} ({schedule})")
            except Exception as e:
                logger.error(f"❌ 注册任务失败 {name}: {e}")
        
        logger.info(f"APScheduler已启动，共 {len(scheduler.get_jobs())} 个任务")
        scheduler.start()
    
    def _load_cron_config(self):
        """加载cron配置"""
        import yaml
        config_path = project_root / 'config' / 'cron_tasks.yaml'
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载cron配置失败: {e}")
            return None
    
    def stop(self):
        """停止双调度器管理器"""
        self.is_running = False
        self._release_leader_lock()
        logger.info(f"[{self.scheduler_name}] 双调度器管理器已停止")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='XCNStock 双调度器管理器')
    parser.add_argument(
        '--type',
        choices=['kestra', 'apscheduler'],
        required=True,
        help='调度器类型'
    )
    
    args = parser.parse_args()
    
    manager = DualSchedulerManager(scheduler_type=args.type)
    
    try:
        manager.start()
        
        # 保持运行
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        manager.stop()


if __name__ == '__main__':
    sys.exit(main())
