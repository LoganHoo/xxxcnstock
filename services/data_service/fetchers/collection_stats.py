#!/usr/bin/env python3
"""
采集统计管理器 (v3.0)

职责：
1. 集中管理所有采集统计数据
2. 事件驱动的统计更新
3. 线程安全的计数器
4. 实时统计和报告

设计原则：
- 单一职责：只负责统计管理
- 数据一致性：避免双重计数
- 可观测性：支持实时监控和报告

使用方式：
    stats = CollectionStatsManager()
    stats.record_success('000001', 'baostock', 120)
    stats.record_failure('000002', 'network_timeout')
    report = stats.get_report()
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field
from collections import defaultdict
import threading
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class StockResult:
    """单只股票的采集结果"""
    code: str
    success: bool
    source: str = ''
    rows: int = 0
    status: str = ''
    error: str = ''
    error_type: str = ''
    elapsed_ms: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class SourceStats:
    """单个数据源的统计信息"""
    tasks: int = 0
    success: int = 0
    failed: int = 0
    total_rows: int = 0
    total_elapsed_ms: float = 0.0
    
    @property
    def success_rate(self) -> float:
        if self.tasks == 0:
            return 0.0
        return round(self.success / self.tasks * 100, 1)
    
    @property
    def avg_elapsed_ms(self) -> float:
        if self.success == 0:
            return 0.0
        return round(self.total_elapsed_ms / self.success, 1)


class CollectionStatsManager:
    """
    集中式采集统计管理器
    
    特性：
    - 事件驱动更新，避免分散式计数
    - 线程安全，支持并发访问
    - 实时聚合，避免重复计算
    - 详细的历史记录用于调试
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        
        self._total_stocks = 0
        self._start_time: Optional[float] = None
        
        # 按数据源分类的统计
        self._source_stats: Dict[str, SourceStats] = {
            'baostock': SourceStats(),
            'tencent': SourceStats(),
        }
        
        # 全局计数器（避免双重计数的唯一数据源）
        self._global_counts = {
            'cache_hits': 0,
            'delisted_detected': 0,
            'fallback_success': 0,
            'validation_issues': 0,
        }
        
        # 失败原因分类统计
        self._failure_reasons: Dict[str, int] = defaultdict(int)
        
        # 连续失败跟踪（用于黑名单决策）
        self._consecutive_failures: Dict[str, int] = defaultdict(int)
        
        # 详细结果记录（可选，用于调试）
        self._results_history: List[StockResult] = []
        self._max_history_size = 1000
    
    def start_timing(self):
        """开始计时"""
        self._start_time = time.time()
        logger.info("统计计时开始")
    
    @property
    def elapsed_seconds(self) -> float:
        """获取已用时间（秒）"""
        if self._start_time is None:
            return 0.0
        return round(time.time() - self._start_time, 1)
    
    def set_total_stocks(self, count: int):
        """设置总股票数"""
        with self._lock:
            self._total_stocks = count
    
    def record_success(
        self,
        code: str,
        source: str,
        rows: int,
        elapsed_ms: float = 0.0,
        status: str = ''
    ):
        """
        记录成功采集
        
        Args:
            code: 股票代码
            source: 数据源名称
            rows: 获取的行数
            elapsed_ms: 耗时（毫秒）
            status: 状态描述
        """
        with self._lock:
            source_stats = self._source_stats.get(source)
            if source_stats:
                source_stats.tasks += 1
                source_stats.success += 1
                source_stats.total_rows += rows
                source_stats.total_elapsed_ms += elapsed_ms
            
            # 重置连续失败计数
            if code in self._consecutive_failures:
                del self._consecutive_failures[code]
            
            # 记录历史
            self._add_to_history(StockResult(
                code=code,
                success=True,
                source=source,
                rows=rows,
                status=status,
                elapsed_ms=elapsed_ms
            ))
    
    def record_failure(
        self,
        code: str,
        source: str,
        error: str,
        error_type: str = 'unknown'
    ):
        """
        记录失败采集
        
        Args:
            code: 股票代码
            source: 数据源名称
            error: 错误信息
            error_type: 错误类型
        """
        with self._lock:
            source_stats = self._source_stats.get(source)
            if source_stats:
                source_stats.tasks += 1
                source_stats.failed += 1
            
            # 更新失败原因统计
            self._failure_reasons[error_type] += 1
            
            # 更新连续失败计数
            self._consecutive_failures[code] += 1
            
            # 记录历史
            self._add_to_history(StockResult(
                code=code,
                success=False,
                source=source,
                error=error[:100],
                error_type=error_type
            ))
    
    def record_cache_hit(self, code: str):
        """记录缓存命中"""
        with self._lock:
            self._global_counts['cache_hits'] += 1
    
    def record_delisted(self, code: str, reason: str = ''):
        """记录检测到退市股票"""
        with self._lock:
            self._global_counts['delisted_detected'] += 1
            logger.debug(f"记录退市检测: {code} (原因: {reason})")
    
    def record_fallback_success(
        self,
        code: str,
        fallback_source: str,
        rows: int
    ):
        """记录降级成功"""
        with self._lock:
            self._global_counts['fallback_success'] += 1
            
            # 同时更新备用源的统计
            source_stats = self._source_stats.get(fallback_source)
            if source_stats:
                source_stats.tasks += 1
                source_stats.success += 1
                source_stats.total_rows += rows
    
    def record_validation_issue(self, code: str, issue_type: str):
        """记录验证问题"""
        with self._lock:
            self._global_counts['validation_issues'] += 1
    
    def get_consecutive_failures(self, code: str) -> int:
        """获取股票的连续失败次数"""
        with self._lock:
            return self._consecutive_failures.get(code, 0)
    
    def should_blacklist(self, code: str, threshold: int = 3) -> bool:
        """
        判断是否应该将股票加入黑名单
        
        Args:
            code: 股票代码
            threshold: 连续失败阈值
            
        Returns:
            是否应该加入黑名单
        """
        with self._lock:
            failures = self._consecutive_failures.get(code, 0)
            return failures >= threshold
    
    def _add_to_history(self, result: StockResult):
        """添加到历史记录（自动限制大小）"""
        self._results_history.append(result)
        
        if len(self._results_history) > self._max_history_size:
            self._results_history = self._results_history[-self._max_history_size // 2:]
    
    def get_aggregated_stats(self) -> Dict[str, Any]:
        """
        聚合统计信息（唯一权威来源，避免双重计数）
        
        Returns:
            包含所有统计信息的字典
        """
        with self._lock:
            bs = self._source_stats['baostock']
            tc = self._source_stats['tencent']
            
            total_tasks = bs.tasks + tc.tasks
            total_success = bs.success + tc.success
            total_failed = bs.failed + tc.failed
            
            return {
                # 总体统计
                'total': self._total_stocks,
                'success': total_success,
                'failed': total_failed,
                'skipped': self._global_counts['cache_hits'],
                'delisted': self._global_counts['delisted_detected'],
                'pending': max(0, self._total_stocks - total_success - 
                               self._global_counts['cache_hits'] - total_failed),
                
                # 成功率
                'success_rate': round(total_success / total_tasks * 100, 1) if total_tasks > 0 else 0,
                
                # 按数据源统计
                'baostock': {
                    'tasks': bs.tasks,
                    'success': bs.success,
                    'failed': bs.failed,
                    'success_rate': bs.success_rate,
                    'avg_elapsed_ms': bs.avg_elapsed_ms,
                    'total_rows': bs.total_rows,
                },
                'tencent': {
                    'tasks': tc.tasks,
                    'success': tc.success,
                    'failed': tc.failed,
                    'success_rate': tc.success_rate,
                    'avg_elapsed_ms': tc.avg_elapsed_ms,
                    'total_rows': tc.total_rows,
                },
                
                # 特殊统计
                'fallback_success': self._global_counts['fallback_success'],
                'cache_hits': self._global_counts['cache_hits'],
                'delisted_detected': self._global_counts['delisted_detected'],
                'validation_issues': self._global_counts['validation_issues'],
                
                # 时间统计
                'elapsed_seconds': self.elapsed_seconds,
                
                # 失败原因分布
                'failure_reasons': dict(self._failure_reasons),
            }
    
    def get_source_stats(self, source: str) -> Optional[SourceStats]:
        """获取指定数据源的统计"""
        with self._lock:
            return self._source_stats.get(source)
    
    def get_recent_results(self, limit: int = 50) -> List[StockResult]:
        """获取最近的结果记录"""
        with self._lock:
            return list(self._results_history[-limit:])
    
    def get_failed_codes(self) -> List[str]:
        """获取所有失败的代码列表"""
        with self._lock:
            return [
                r.code for r in self._results_history
                if not r.success
            ]
    
    def reset(self):
        """重置所有统计"""
        with self._lock:
            self._total_stocks = 0
            self._start_time = None
            
            for key in self._source_stats:
                self._source_stats[key] = SourceStats()
            
            for key in self._global_counts:
                self._global_counts[key] = 0
            
            self._failure_reasons.clear()
            self._consecutive_failures.clear()
            self._results_history.clear()
            
            logger.info("统计已重置")
    
    def get_progress_bar(self, width: int = 20) -> str:
        """生成进度条字符串"""
        stats = self.get_aggregated_stats()
        
        total = stats['total']
        if total == 0:
            return '[' + '░' * width + '] 0.0%'
        
        done = stats['success'] + stats['skipped'] + stats['delisted']
        pct = done / total * 100
        
        filled = int(width * pct / 100)
        bar = '█' * filled + '░' * (width - filled)
        
        return f'[{bar}] {pct:5.1f}%'
    
    def print_summary(self):
        """打印统计摘要到日志"""
        stats = self.get_aggregated_stats()
        
        logger.info("=" * 60)
        logger.info("采集统计摘要:")
        logger.info(f"  总计: {stats['total']} 只股票")
        logger.info(f"  成功: {stats['success']} 只 ({stats['success_rate']:.1f}%)")
        logger.info(f"  跳过(已最新): {stats['skipped']} 只")
        logger.info(f"  退市/停牌: {stats['delisted']} 只")
        logger.info(f"  失败: {stats['failed']} 只")
        logger.info(f"  待处理: {stats['pending']} 只")
        logger.info("")
        logger.info(f"  Baostock: ✓{stats['baostock']['success']}/{stats['baostock']['tasks']} "
                   f"({stats['baostock']['success_rate']}%) "
                   f"平均{stats['baostock']['avg_elapsed_ms']}ms")
        logger.info(f"  Tencent: ✓{stats['tencent']['success']}/{stats['tencent']['tasks']} "
                   f"({stats['tencent']['success_rate']}%) "
                   f"平均{stats['tencent']['avg_elapsed_ms']}ms")
        logger.info(f"  降级成功: {stats['fallback_success']} 次")
        logger.info(f"  检测到退市: {stats['delisted_detected']} 只")
        logger.info(f"  耗时: {stats['elapsed_seconds']:.0f}s "
                   f"({stats['elapsed_seconds']/60:.1f}min)")
        
        if stats['failure_reasons']:
            logger.info("")
            logger.info("  失败原因分布:")
            for reason, count in sorted(stats['failure_reasons'].items(), 
                                        key=lambda x: x[1], reverse=True)[:5]:
                logger.info(f"    - {reason}: {count}")
        
        logger.info("=" * 60)
