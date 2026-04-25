"""
流水线监控模块

提供任务执行监控、性能指标收集和告警功能。
集成 Prometheus 指标暴露。
"""

import time
import functools
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import contextmanager

from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from loguru import logger


# Prometheus 指标定义
TASK_DURATION = Histogram(
    'xcnstock_task_duration_seconds',
    'Task execution duration in seconds',
    ['task_name', 'status']
)

TASK_SUCCESS_RATE = Gauge(
    'xcnstock_task_success_rate',
    'Task success rate (0-1)',
    ['task_name']
)

TASK_COUNT = Counter(
    'xcnstock_task_count',
    'Total number of task executions',
    ['task_name', 'status']
)

CACHE_HIT_RATIO = Gauge(
    'xcnstock_cache_hit_ratio',
    'Cache hit ratio (0-1)',
    ['cache_level']
)

DATA_COLLECTION_DURATION = Histogram(
    'xcnstock_data_collection_duration_seconds',
    'Data collection duration',
    ['data_type']
)

DATA_COLLECTION_RECORDS = Counter(
    'xcnstock_data_collection_records_total',
    'Total number of records collected',
    ['data_type', 'status']
)

PIPELINE_STAGE_DURATION = Histogram(
    'xcnstock_pipeline_stage_duration_seconds',
    'Pipeline stage duration',
    ['stage_name']
)

ACTIVE_TASKS = Gauge(
    'xcnstock_active_tasks',
    'Number of currently active tasks',
    ['task_type']
)

SYSTEM_INFO = Info(
    'xcnstock_system',
    'System information'
)


@dataclass
class TaskMetrics:
    """任务指标"""
    task_name: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    status: str = "running"
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, status: str = "success", error: str = None):
        """完成任务"""
        self.end_time = datetime.now()
        self.status = status
        self.error_message = error
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'task_name': self.task_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'status': self.status,
            'error_message': self.error_message,
            'metadata': self.metadata
        }


class TaskMetricsCollector:
    """
    任务指标收集器
    
    收集任务执行指标并暴露给 Prometheus。
    """
    
    def __init__(self):
        self.active_tasks: Dict[str, TaskMetrics] = {}
        self.task_history: List[TaskMetrics] = []
        self.max_history = 1000
    
    def start_task(self, task_name: str, metadata: Dict[str, Any] = None) -> TaskMetrics:
        """
        开始记录任务
        
        Args:
            task_name: 任务名称
            metadata: 任务元数据
            
        Returns:
            TaskMetrics: 任务指标对象
        """
        metrics = TaskMetrics(
            task_name=task_name,
            metadata=metadata or {}
        )
        
        self.active_tasks[task_name] = metrics
        ACTIVE_TASKS.labels(task_type=task_name).inc()
        
        logger.info(f"Task started: {task_name}")
        return metrics
    
    def end_task(
        self,
        task_name: str,
        status: str = "success",
        error: str = None
    ) -> TaskMetrics:
        """
        结束记录任务
        
        Args:
            task_name: 任务名称
            status: 任务状态 (success, failed)
            error: 错误信息
            
        Returns:
            TaskMetrics: 任务指标对象
        """
        metrics = self.active_tasks.pop(task_name, None)
        
        if metrics:
            metrics.complete(status, error)
            
            # 记录 Prometheus 指标
            TASK_DURATION.labels(
                task_name=task_name,
                status=status
            ).observe(metrics.duration_seconds)
            
            TASK_COUNT.labels(
                task_name=task_name,
                status=status
            ).inc()
            
            ACTIVE_TASKS.labels(task_type=task_name).dec()
            
            # 保存到历史
            self.task_history.append(metrics)
            if len(self.task_history) > self.max_history:
                self.task_history.pop(0)
            
            # 更新成功率
            self._update_success_rate(task_name)
            
            logger.info(
                f"Task completed: {task_name} "
                f"(status={status}, duration={metrics.duration_seconds:.2f}s)"
            )
        
        return metrics
    
    def _update_success_rate(self, task_name: str):
        """更新任务成功率"""
        # 计算最近 100 次任务的成功率
        recent_tasks = [
            t for t in self.task_history
            if t.task_name == task_name
        ][-100:]
        
        if recent_tasks:
            success_count = sum(1 for t in recent_tasks if t.status == "success")
            rate = success_count / len(recent_tasks)
            TASK_SUCCESS_RATE.labels(task_name=task_name).set(rate)
    
    def get_task_stats(self, task_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取任务统计
        
        Args:
            task_name: 任务名称，None 表示所有任务
            
        Returns:
            Dict: 任务统计信息
        """
        tasks = self.task_history
        if task_name:
            tasks = [t for t in tasks if t.task_name == task_name]
        
        if not tasks:
            return {}
        
        total = len(tasks)
        success = sum(1 for t in tasks if t.status == "success")
        failed = total - success
        
        durations = [t.duration_seconds for t in tasks]
        
        return {
            'total': total,
            'success': success,
            'failed': failed,
            'success_rate': success / total if total > 0 else 0,
            'avg_duration': sum(durations) / len(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
        }
    
    @contextmanager
    def measure_task(self, task_name: str, metadata: Dict[str, Any] = None):
        """
        任务测量上下文管理器
        
        使用示例：
            with collector.measure_task("data_collection"):
                collect_data()
        """
        metrics = self.start_task(task_name, metadata)
        try:
            yield metrics
            self.end_task(task_name, status="success")
        except Exception as e:
            self.end_task(task_name, status="failed", error=str(e))
            raise


class PipelineStageMonitor:
    """
    流水线阶段监控器
    
    监控流水线各阶段的执行时间。
    """
    
    def __init__(self):
        self.stages: Dict[str, Dict[str, Any]] = {}
    
    @contextmanager
    def measure_stage(self, stage_name: str):
        """
        测量阶段执行时间
        
        使用示例：
            with monitor.measure_stage("data_fetch"):
                fetch_data()
        """
        start_time = time.time()
        logger.info(f"Pipeline stage started: {stage_name}")
        
        try:
            yield
            
            duration = time.time() - start_time
            PIPELINE_STAGE_DURATION.labels(stage_name=stage_name).observe(duration)
            
            self.stages[stage_name] = {
                'duration': duration,
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Pipeline stage completed: {stage_name} ({duration:.2f}s)")
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.stages[stage_name] = {
                'duration': duration,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.error(f"Pipeline stage failed: {stage_name} - {e}")
            raise
    
    def get_stage_summary(self) -> Dict[str, Any]:
        """获取阶段摘要"""
        return self.stages.copy()


class AlertManager:
    """
    告警管理器
    
    管理告警规则和通知。
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.rules: List[Dict[str, Any]] = []
        self.alert_history: List[Dict[str, Any]] = []
        self.webhook_url: Optional[str] = self.config.get('webhook_url')
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[], bool],
        message: str,
        severity: str = "warning"
    ):
        """
        添加告警规则
        
        Args:
            name: 规则名称
            condition: 条件函数，返回 True 时触发告警
            message: 告警消息
            severity: 严重程度 (critical, warning, info)
        """
        self.rules.append({
            'name': name,
            'condition': condition,
            'message': message,
            'severity': severity,
            'last_triggered': None,
            'trigger_count': 0
        })
    
    def check_rules(self):
        """检查所有告警规则"""
        for rule in self.rules:
            try:
                if rule['condition']():
                    self._trigger_alert(rule)
            except Exception as e:
                logger.error(f"Error checking alert rule {rule['name']}: {e}")
    
    def _trigger_alert(self, rule: Dict[str, Any]):
        """触发告警"""
        now = datetime.now()
        
        # 告警抑制（5分钟内不重复触发）
        if rule['last_triggered']:
            elapsed = (now - rule['last_triggered']).total_seconds()
            if elapsed < 300:  # 5分钟
                return
        
        rule['last_triggered'] = now
        rule['trigger_count'] += 1
        
        alert = {
            'name': rule['name'],
            'message': rule['message'],
            'severity': rule['severity'],
            'timestamp': now.isoformat(),
            'trigger_count': rule['trigger_count']
        }
        
        self.alert_history.append(alert)
        
        # 记录日志
        log_func = logger.warning if rule['severity'] == 'warning' else logger.error
        log_func(f"ALERT: {rule['name']} - {rule['message']}")
        
        # 发送 Webhook 通知
        if self.webhook_url:
            self._send_webhook(alert)
    
    def _send_webhook(self, alert: Dict[str, Any]):
        """发送 Webhook 通知"""
        try:
            import requests
            requests.post(
                self.webhook_url,
                json=alert,
                timeout=10
            )
        except Exception as e:
            logger.error(f"Failed to send webhook: {e}")
    
    def get_alerts(self, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取告警历史"""
        if severity:
            return [a for a in self.alert_history if a['severity'] == severity]
        return self.alert_history.copy()


def monitor_task(task_name: str):
    """
    任务监控装饰器
    
    使用示例：
        @monitor_task("data_collection")
        def collect_data():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            collector = TaskMetricsCollector()
            
            with collector.measure_task(task_name):
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


def get_prometheus_metrics() -> bytes:
    """获取 Prometheus 格式的指标数据"""
    return generate_latest()


def update_cache_hit_ratio(level: str, hits: int, misses: int):
    """
    更新缓存命中率
    
    Args:
        level: 缓存层级 (l1, l2)
        hits: 命中次数
        misses: 未命中次数
    """
    total = hits + misses
    if total > 0:
        ratio = hits / total
        CACHE_HIT_RATIO.labels(cache_level=level).set(ratio)


def record_data_collection(data_type: str, record_count: int, success: bool = True):
    """
    记录数据采集指标
    
    Args:
        data_type: 数据类型
        record_count: 记录数
        success: 是否成功
    """
    status = "success" if success else "failed"
    DATA_COLLECTION_RECORDS.labels(
        data_type=data_type,
        status=status
    ).inc(record_count)


# 全局实例
task_collector = TaskMetricsCollector()
stage_monitor = PipelineStageMonitor()


def init_system_info(version: str = "1.0.0", environment: str = "production"):
    """初始化系统信息"""
    SYSTEM_INFO.info({
        'version': version,
        'environment': environment
    })
