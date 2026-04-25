#!/usr/bin/env python3
"""
数据质量监控系统

功能：
- 定期数据质量检查
- 异常告警通知
- 数据新鲜度监控
- 数据完整性监控
"""
import logging
import json
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
from enum import Enum

import pandas as pd
import polars as pl

try:
    from .gx_validator import (
        GreatExpectationsValidator,
        ValidationSuiteResult,
        KlineDataQualitySuite,
        StockListQualitySuite,
        validate_kline_data,
        generate_quality_report
    )
except ImportError:
    from gx_validator import (
        GreatExpectationsValidator,
        ValidationSuiteResult,
        KlineDataQualitySuite,
        StockListQualitySuite,
        validate_kline_data,
        generate_quality_report
    )

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """告警信息"""
    level: AlertLevel
    title: str
    message: str
    timestamp: datetime
    details: Dict = None
    
    def to_dict(self) -> Dict:
        return {
            'level': self.level.value,
            'title': self.title,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'details': self.details or {}
        }


@dataclass
class DataFreshnessMetric:
    """数据新鲜度指标"""
    data_source: str
    latest_date: Optional[str]
    days_behind: int
    status: str  # 'fresh', 'stale', 'expired'
    threshold_days: int = 3
    
    @property
    def is_fresh(self) -> bool:
        return self.days_behind <= self.threshold_days


class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(
        self,
        data_dir: Path = None,
        alert_handlers: List[Callable[[Alert], None]] = None
    ):
        self.data_dir = data_dir or Path("data")
        self.kline_dir = self.data_dir / "kline"
        self.alert_handlers = alert_handlers or []
        self.alerts: List[Alert] = []
        
    def add_alert_handler(self, handler: Callable[[Alert], None]):
        """添加告警处理器"""
        self.alert_handlers.append(handler)
    
    def _send_alert(self, alert: Alert):
        """发送告警"""
        self.alerts.append(alert)
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"告警处理器失败: {e}")
    
    def check_data_freshness(self) -> List[DataFreshnessMetric]:
        """检查数据新鲜度"""
        metrics = []
        today = datetime.now().date()
        
        # 检查K线数据新鲜度（抽样）
        if self.kline_dir.exists():
            sample_files = list(self.kline_dir.glob("*.parquet"))[:10]
            
            latest_dates = []
            for file_path in sample_files:
                try:
                    df = pl.read_parquet(file_path)
                    if 'trade_date' in df.columns:
                        latest_date = df['trade_date'].max()
                        latest_dates.append(latest_date)
                except Exception as e:
                    logger.warning(f"读取 {file_path} 失败: {e}")
            
            if latest_dates:
                overall_latest = max(latest_dates)
                latest_date_obj = datetime.strptime(str(overall_latest), "%Y-%m-%d").date()
                days_behind = (today - latest_date_obj).days
                
                if days_behind <= 1:
                    status = 'fresh'
                elif days_behind <= 3:
                    status = 'stale'
                else:
                    status = 'expired'
                
                metric = DataFreshnessMetric(
                    data_source='kline',
                    latest_date=str(overall_latest),
                    days_behind=days_behind,
                    status=status,
                    threshold_days=3
                )
                metrics.append(metric)
                
                # 发送告警
                if status == 'stale':
                    self._send_alert(Alert(
                        level=AlertLevel.WARNING,
                        title="数据新鲜度警告",
                        message=f"K线数据已滞后 {days_behind} 天",
                        timestamp=datetime.now(),
                        details={'latest_date': str(overall_latest)}
                    ))
                elif status == 'expired':
                    self._send_alert(Alert(
                        level=AlertLevel.ERROR,
                        title="数据新鲜度严重滞后",
                        message=f"K线数据已滞后 {days_behind} 天，需要立即更新",
                        timestamp=datetime.now(),
                        details={'latest_date': str(overall_latest)}
                    ))
        
        return metrics
    
    def check_data_completeness(self) -> Dict[str, any]:
        """检查数据完整性"""
        results = {
            'stock_list_exists': False,
            'kline_count': 0,
            'expected_kline_count': 5000,  # 期望的K线文件数量
            'completeness_rate': 0.0
        }
        
        # 检查股票列表
        stock_list_file = self.data_dir / "stock_list.parquet"
        results['stock_list_exists'] = stock_list_file.exists()
        
        if not results['stock_list_exists']:
            self._send_alert(Alert(
                level=AlertLevel.ERROR,
                title="股票列表缺失",
                message="股票列表文件不存在，需要重新采集",
                timestamp=datetime.now()
            ))
        
        # 检查K线数据
        if self.kline_dir.exists():
            results['kline_count'] = len(list(self.kline_dir.glob("*.parquet")))
            results['completeness_rate'] = min(
                results['kline_count'] / results['expected_kline_count'],
                1.0
            )
            
            if results['kline_count'] < results['expected_kline_count'] * 0.8:
                self._send_alert(Alert(
                    level=AlertLevel.WARNING,
                    title="K线数据不完整",
                    message=f"K线数据覆盖率仅 {results['completeness_rate']:.1%} ({results['kline_count']}/{results['expected_kline_count']})",
                    timestamp=datetime.now(),
                    details={'kline_count': results['kline_count']}
                ))
        
        return results
    
    def check_data_quality(self, sample_size: int = 10) -> Dict[str, ValidationSuiteResult]:
        """检查数据质量"""
        if not self.kline_dir.exists():
            logger.warning("K线数据目录不存在")
            return {}
        
        results = {}
        parquet_files = list(self.kline_dir.glob("*.parquet"))
        
        if len(parquet_files) > sample_size:
            import random
            random.seed(42)
            parquet_files = random.sample(parquet_files, sample_size)
        
        logger.info(f"质量检查: 抽样 {len(parquet_files)} 个文件")
        
        for file_path in parquet_files:
            try:
                result = validate_kline_data(file_path)
                results[file_path.stem] = result
                
                # 质量差的告警
                if result.success_rate < 0.9:
                    self._send_alert(Alert(
                        level=AlertLevel.WARNING,
                        title=f"数据质量问题: {file_path.stem}",
                        message=f"成功率仅 {result.success_rate:.1%}",
                        timestamp=datetime.now(),
                        details={
                            'file': file_path.name,
                            'success_rate': result.success_rate,
                            'failed_count': len(result.failed_expectations)
                        }
                    ))
            except Exception as e:
                logger.error(f"验证 {file_path} 失败: {e}")
                self._send_alert(Alert(
                    level=AlertLevel.ERROR,
                    title=f"数据验证失败: {file_path.stem}",
                    message=str(e),
                    timestamp=datetime.now()
                ))
        
        return results
    
    def run_full_check(self) -> Dict:
        """运行完整检查"""
        logger.info("=" * 70)
        logger.info("🔍 开始数据质量全面检查")
        logger.info("=" * 70)
        
        report = {
            'check_time': datetime.now().isoformat(),
            'freshness': [],
            'completeness': {},
            'quality': {},
            'alerts': []
        }
        
        # 1. 数据新鲜度
        logger.info("\n1️⃣ 检查数据新鲜度...")
        freshness_metrics = self.check_data_freshness()
        report['freshness'] = [
            {
                'data_source': m.data_source,
                'latest_date': m.latest_date,
                'days_behind': m.days_behind,
                'status': m.status,
                'is_fresh': m.is_fresh
            }
            for m in freshness_metrics
        ]
        
        for metric in freshness_metrics:
            status_icon = "✅" if metric.is_fresh else "⚠️"
            logger.info(f"   {status_icon} {metric.data_source}: 最新 {metric.latest_date} (滞后 {metric.days_behind} 天)")
        
        # 2. 数据完整性
        logger.info("\n2️⃣ 检查数据完整性...")
        completeness = self.check_data_completeness()
        report['completeness'] = completeness
        
        logger.info(f"   股票列表: {'✅ 存在' if completeness['stock_list_exists'] else '❌ 缺失'}")
        logger.info(f"   K线文件数: {completeness['kline_count']}")
        logger.info(f"   完整度: {completeness['completeness_rate']:.1%}")
        
        # 3. 数据质量
        logger.info("\n3️⃣ 检查数据质量...")
        quality_results = self.check_data_quality(sample_size=20)
        
        passed = sum(1 for r in quality_results.values() if r.success)
        total = len(quality_results)
        avg_success_rate = sum(r.success_rate for r in quality_results.values()) / total if total else 0
        
        report['quality'] = {
            'sample_size': total,
            'passed': passed,
            'failed': total - passed,
            'avg_success_rate': avg_success_rate
        }
        
        logger.info(f"   抽样检查: {total} 个文件")
        logger.info(f"   通过: {passed} | 失败: {total - passed}")
        logger.info(f"   平均成功率: {avg_success_rate:.1%}")
        
        # 4. 告警汇总
        report['alerts'] = [alert.to_dict() for alert in self.alerts]
        
        logger.info("\n4️⃣ 告警汇总...")
        if self.alerts:
            for alert in self.alerts:
                icon = {"info": "ℹ️", "warning": "⚠️", "error": "❌", "critical": "🚨"}.get(alert.level.value, "•")
                logger.info(f"   {icon} [{alert.level.value.upper()}] {alert.title}")
        else:
            logger.info("   ✅ 无告警")
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 数据质量检查完成")
        logger.info("=" * 70)
        
        return report
    
    def save_report(self, report: Dict, output_path: Path = None):
        """保存检查报告"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = self.data_dir / f"quality_check_{timestamp}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"报告已保存: {output_path}")
        return output_path


class ConsoleAlertHandler:
    """控制台告警处理器"""
    
    def __call__(self, alert: Alert):
        icon = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨"
        }.get(alert.level, "•")
        
        print(f"\n{icon} [{alert.level.value.upper()}] {alert.title}")
        print(f"   {alert.message}")
        if alert.details:
            print(f"   详情: {alert.details}")


class FileAlertHandler:
    """文件告警处理器"""
    
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
    
    def __call__(self, alert: Alert):
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"{alert.timestamp.isoformat()} | {alert.level.value.upper()} | {alert.title} | {alert.message}\n")


def run_monitor_check():
    """运行监控检查（命令行入口）"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    # 创建监控器
    monitor = DataQualityMonitor()
    
    # 添加告警处理器
    monitor.add_alert_handler(ConsoleAlertHandler())
    monitor.add_alert_handler(FileAlertHandler(Path("logs/data_quality_alerts.log")))
    
    # 运行检查
    report = monitor.run_full_check()
    
    # 保存报告
    report_path = monitor.save_report(report)
    
    print(f"\n📄 详细报告: {report_path}")
    
    # 返回状态码
    critical_alerts = sum(1 for a in monitor.alerts if a.level == AlertLevel.CRITICAL)
    error_alerts = sum(1 for a in monitor.alerts if a.level == AlertLevel.ERROR)
    
    if critical_alerts > 0:
        return 2
    elif error_alerts > 0:
        return 1
    else:
        return 0


if __name__ == "__main__":
    exit_code = run_monitor_check()
    exit(exit_code)
