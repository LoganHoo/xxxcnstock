#!/usr/bin/env python3
"""
性能指标监控模块

用于监控策略性能、系统健康度等
"""
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """指标数据点"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        self.metrics: Dict[str, List[MetricPoint]] = defaultdict(list)
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        
    def record(self, name: str, value: float, labels: Optional[Dict] = None):
        """记录指标"""
        point = MetricPoint(
            timestamp=datetime.now(),
            value=value,
            labels=labels or {}
        )
        self.metrics[name].append(point)
        
        # 限制历史数据长度
        if len(self.metrics[name]) > 10000:
            self.metrics[name] = self.metrics[name][-5000:]
    
    def increment(self, name: str, value: float = 1.0):
        """增加计数器"""
        self.counters[name] += value
    
    def set_gauge(self, name: str, value: float):
        """设置仪表盘值"""
        self.gauges[name] = value
    
    def get_metric(self, name: str, duration_minutes: int = 60) -> List[MetricPoint]:
        """获取最近指标"""
        if name not in self.metrics:
            return []
        
        cutoff = datetime.now() - timedelta(minutes=duration_minutes)
        return [p for p in self.metrics[name] if p.timestamp > cutoff]
    
    def get_summary(self, name: str) -> Dict[str, float]:
        """获取指标摘要"""
        points = self.metrics.get(name, [])
        if not points:
            return {}
        
        values = [p.value for p in points]
        return {
            'count': len(values),
            'sum': sum(values),
            'mean': sum(values) / len(values),
            'min': min(values),
            'max': max(values),
            'last': values[-1] if values else 0
        }
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """获取所有指标"""
        return {
            'counters': dict(self.counters),
            'gauges': dict(self.gauges),
            'summaries': {name: self.get_summary(name) for name in self.metrics.keys()}
        }


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.collector = MetricsCollector()
        self.start_times: Dict[str, float] = {}
        
    def start_timer(self, name: str):
        """开始计时"""
        self.start_times[name] = time.time()
    
    def end_timer(self, name: str, labels: Optional[Dict] = None):
        """结束计时并记录"""
        if name in self.start_times:
            duration = time.time() - self.start_times[name]
            self.collector.record(f'{name}_duration', duration, labels)
            del self.start_times[name]
            return duration
        return 0
    
    def record_trade(self, trade_result: Dict):
        """记录交易结果"""
        self.collector.increment('trades_total')
        
        if trade_result.get('pnl', 0) > 0:
            self.collector.increment('trades_wins')
            self.collector.record('trade_profit', trade_result['pnl'])
        else:
            self.collector.increment('trades_losses')
            self.collector.record('trade_loss', abs(trade_result['pnl']))
    
    def record_signal(self, strategy: str, signal_count: int):
        """记录信号生成"""
        self.collector.record('signals_generated', signal_count, {'strategy': strategy})
        self.collector.increment(f'signals_{strategy}', signal_count)
    
    def get_win_rate(self) -> float:
        """获取胜率"""
        wins = self.collector.counters.get('trades_wins', 0)
        total = self.collector.counters.get('trades_total', 0)
        return wins / total if total > 0 else 0
    
    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        return {
            'win_rate': self.get_win_rate(),
            'total_trades': self.collector.counters.get('trades_total', 0),
            'winning_trades': self.collector.counters.get('trades_wins', 0),
            'losing_trades': self.collector.counters.get('trades_losses', 0),
            'metrics': self.collector.get_all_metrics()
        }


class SystemHealthMonitor:
    """系统健康监控"""
    
    def __init__(self):
        self.collector = MetricsCollector()
        self.checks: Dict[str, callable] = {}
        
    def register_check(self, name: str, check_func: callable):
        """注册健康检查"""
        self.checks[name] = check_func
    
    def run_health_checks(self) -> Dict[str, Any]:
        """运行健康检查"""
        results = {}
        overall_status = 'healthy'
        
        for name, check_func in self.checks.items():
            try:
                result = check_func()
                results[name] = {
                    'status': 'pass' if result else 'fail',
                    'message': 'OK' if result else 'Check failed'
                }
                if not result:
                    overall_status = 'unhealthy'
            except Exception as e:
                results[name] = {
                    'status': 'error',
                    'message': str(e)
                }
                overall_status = 'unhealthy'
        
        return {
            'status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'checks': results
        }
    
    def check_data_source(self, source_name: str, check_func: callable) -> bool:
        """检查数据源健康"""
        try:
            healthy = check_func()
            self.collector.set_gauge(f'datasource_{source_name}_healthy', 1 if healthy else 0)
            return healthy
        except Exception as e:
            logger.error(f"数据源 {source_name} 检查失败: {e}")
            self.collector.set_gauge(f'datasource_{source_name}_healthy', 0)
            return False


# 全局监控实例
performance_monitor = PerformanceMonitor()
system_monitor = SystemHealthMonitor()
metrics_collector = MetricsCollector()


def get_monitoring_dashboard_data() -> Dict[str, Any]:
    """获取监控面板数据"""
    return {
        'performance': performance_monitor.get_performance_report(),
        'health': system_monitor.run_health_checks(),
        'metrics': metrics_collector.get_all_metrics(),
        'timestamp': datetime.now().isoformat()
    }


if __name__ == '__main__':
    # 测试
    monitor = PerformanceMonitor()
    
    monitor.start_timer('test_operation')
    time.sleep(0.1)
    monitor.end_timer('test_operation')
    
    monitor.record_trade({'pnl': 100})
    monitor.record_trade({'pnl': -50})
    monitor.record_signal('endstock_pick', 5)
    
    print(monitor.get_performance_report())
