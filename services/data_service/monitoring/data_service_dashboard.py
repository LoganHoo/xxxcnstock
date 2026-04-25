#!/usr/bin/env python3
"""
数据服务监控面板

提供数据服务的实时监控和统计:
- 数据质量监控
- 更新任务状态
- 缓存命中率
- 存储使用情况
- API调用统计

使用示例:
    dashboard = DataServiceDashboard()
    report = dashboard.generate_full_report()
    dashboard.print_report(report)
"""
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.quality.data_quality_monitor import DataQualityMonitor
from services.data_service.storage.optimized_financial_storage import (
    OptimizedFinancialStorageManager
)
from services.data_service.tasks.incremental_update_task import IncrementalUpdateTask
from services.data_service.tasks.data_preheat_task import DataPreheatingTask

logger = setup_logger("data_service_dashboard", log_file="system/data_service_dashboard.log")


@dataclass
class DashboardMetrics:
    """监控指标"""
    timestamp: str
    
    # 数据质量
    quality_score: float = 0.0
    quality_issues: int = 0
    
    # 存储统计
    storage_stats: Dict = field(default_factory=dict)
    
    # 缓存统计
    cache_stats: Dict = field(default_factory=dict)
    
    # 任务状态
    task_status: Dict = field(default_factory=dict)
    
    # API统计
    api_stats: Dict = field(default_factory=dict)


class DataServiceDashboard:
    """数据服务监控面板"""
    
    def __init__(self):
        self.logger = logger
        
        # 组件
        self.quality_monitor = DataQualityMonitor()
        self.storage = OptimizedFinancialStorageManager()
        self.update_task = IncrementalUpdateTask()
        self.preheat_task = DataPreheatingTask()
        
        # 报告目录
        self.report_dir = get_data_path() / "dashboard_reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def collect_metrics(self) -> DashboardMetrics:
        """收集监控指标"""
        metrics = DashboardMetrics(
            timestamp=datetime.now().isoformat()
        )
        
        # 1. 数据质量
        try:
            quality_result = self.quality_monitor.check_data_freshness()
            metrics.quality_score = quality_result.score
            metrics.quality_issues = len(quality_result.issues)
        except Exception as e:
            self.logger.error(f"收集质量指标失败: {e}")
        
        # 2. 存储统计
        try:
            metrics.storage_stats = self.storage.get_storage_stats()
        except Exception as e:
            self.logger.error(f"收集存储统计失败: {e}")
        
        # 3. 缓存统计
        try:
            metrics.cache_stats = self.storage.get_cache_stats()
        except Exception as e:
            self.logger.error(f"收集缓存统计失败: {e}")
        
        # 4. 任务状态
        try:
            metrics.task_status = {
                'update_summary': self.update_task.get_update_summary(),
                'preheat_status': self.preheat_task.get_preheat_status(),
            }
        except Exception as e:
            self.logger.error(f"收集任务状态失败: {e}")
        
        return metrics
    
    def generate_full_report(self) -> Dict[str, Any]:
        """生成完整报告"""
        self.logger.info("生成数据服务监控报告...")
        
        metrics = self.collect_metrics()
        
        report = {
            'generated_at': metrics.timestamp,
            'overview': {
                'quality_score': metrics.quality_score,
                'quality_status': '良好' if metrics.quality_score >= 90 else '一般' if metrics.quality_score >= 70 else '差',
                'quality_issues': metrics.quality_issues,
            },
            'storage': metrics.storage_stats,
            'cache': metrics.cache_stats,
            'tasks': metrics.task_status,
            'health_status': self._calculate_health_status(metrics),
        }
        
        return report
    
    def _calculate_health_status(self, metrics: DashboardMetrics) -> str:
        """计算健康状态"""
        score = 100
        
        # 数据质量扣分
        if metrics.quality_score < 90:
            score -= (90 - metrics.quality_score)
        
        # 问题数量扣分
        score -= min(metrics.quality_issues * 2, 20)
        
        # 缓存状态
        cache_stats = metrics.cache_stats
        if cache_stats.get('size', 0) == 0:
            score -= 10
        
        if score >= 90:
            return '健康'
        elif score >= 70:
            return '一般'
        elif score >= 50:
            return '警告'
        else:
            return '严重'
    
    def print_report(self, report: Dict = None):
        """打印报告到控制台"""
        if report is None:
            report = self.generate_full_report()
        
        print("\n" + "=" * 70)
        print("数据服务监控报告".center(70))
        print("=" * 70)
        
        # 概览
        print(f"\n📊 生成时间: {report['generated_at']}")
        print(f"🏥 健康状态: {report['health_status']}")
        
        # 数据质量
        overview = report['overview']
        print(f"\n📈 数据质量:")
        print(f"   质量评分: {overview['quality_score']:.1f}/100")
        print(f"   质量状态: {overview['quality_status']}")
        print(f"   问题数量: {overview['quality_issues']}")
        
        # 存储统计
        storage = report.get('storage', {})
        print(f"\n💾 存储统计:")
        print(f"   资产负债表: {storage.get('balance_sheet_count', 0)} 只股票")
        print(f"   利润表: {storage.get('income_statement_count', 0)} 只股票")
        print(f"   现金流量表: {storage.get('cash_flow_count', 0)} 只股票")
        print(f"   财务指标: {storage.get('indicators_count', 0)} 只股票")
        
        # 缓存统计
        cache = report.get('cache', {})
        print(f"\n⚡ 缓存统计:")
        if cache.get('enabled', True):
            print(f"   缓存大小: {cache.get('size', 0)}/{cache.get('max_size', 0)}")
            print(f"   缓存TTL: {cache.get('ttl', 0)}秒")
        else:
            print(f"   缓存: 已禁用")
        
        # 任务状态
        tasks = report.get('tasks', {})
        print(f"\n🔄 任务状态:")
        
        update_summary = tasks.get('update_summary', {})
        print(f"   最后更新: {update_summary.get('last_update', '无')}")
        print(f"   已完成: {update_summary.get('completed_count', 0)} 只股票")
        print(f"   失败: {update_summary.get('failed_count', 0)} 只股票")
        
        preheat_status = tasks.get('preheat_status', {})
        print(f"   最后预热: {preheat_status.get('last_preheat', '无')}")
        if preheat_status.get('preheat_time'):
            print(f"   预热耗时: {preheat_status['preheat_time']:.2f}秒")
        
        print("\n" + "=" * 70)
    
    def save_report(self, report: Dict = None, filename: str = None) -> str:
        """保存报告到文件"""
        if report is None:
            report = self.generate_full_report()
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"dashboard_report_{timestamp}.json"
        
        filepath = self.report_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"报告已保存: {filepath}")
            return str(filepath)
            
        except Exception as e:
            self.logger.error(f"保存报告失败: {e}")
            return ""
    
    def get_historical_reports(self, days: int = 7) -> List[Dict]:
        """获取历史报告"""
        reports = []
        
        try:
            files = sorted(self.report_dir.glob("dashboard_report_*.json"), reverse=True)
            
            for filepath in files[:days]:
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                        reports.append({
                            'timestamp': report.get('generated_at'),
                            'quality_score': report.get('overview', {}).get('quality_score'),
                            'health_status': report.get('health_status'),
                        })
                except Exception as e:
                    self.logger.debug(f"读取历史报告失败: {e}")
                    
        except Exception as e:
            self.logger.error(f"获取历史报告失败: {e}")
        
        return reports
    
    def check_alerts(self) -> List[Dict]:
        """检查告警条件"""
        alerts = []
        
        report = self.generate_full_report()
        
        # 数据质量告警
        quality_score = report['overview']['quality_score']
        if quality_score < 70:
            alerts.append({
                'level': 'error',
                'message': f'数据质量评分过低: {quality_score:.1f}',
                'timestamp': datetime.now().isoformat(),
            })
        elif quality_score < 90:
            alerts.append({
                'level': 'warning',
                'message': f'数据质量评分一般: {quality_score:.1f}',
                'timestamp': datetime.now().isoformat(),
            })
        
        # 问题数量告警
        quality_issues = report['overview']['quality_issues']
        if quality_issues > 10:
            alerts.append({
                'level': 'warning',
                'message': f'数据质量问题较多: {quality_issues} 个',
                'timestamp': datetime.now().isoformat(),
            })
        
        # 缓存告警
        cache = report.get('cache', {})
        if not cache.get('enabled', True):
            alerts.append({
                'level': 'warning',
                'message': '缓存已禁用，可能影响性能',
                'timestamp': datetime.now().isoformat(),
            })
        
        # 更新任务告警
        tasks = report.get('tasks', {})
        update_summary = tasks.get('update_summary', {})
        last_update = update_summary.get('last_update')
        
        if last_update:
            last_update_time = datetime.fromisoformat(last_update)
            elapsed = (datetime.now() - last_update_time).total_seconds() / 3600
            
            if elapsed > 48:  # 超过48小时未更新
                alerts.append({
                    'level': 'error',
                    'message': f'数据更新延迟: {elapsed:.1f}小时未更新',
                    'timestamp': datetime.now().isoformat(),
                })
            elif elapsed > 24:
                alerts.append({
                    'level': 'warning',
                    'message': f'数据更新延迟: {elapsed:.1f}小时未更新',
                    'timestamp': datetime.now().isoformat(),
                })
        
        return alerts
    
    def print_alerts(self):
        """打印告警信息"""
        alerts = self.check_alerts()
        
        if not alerts:
            print("\n✅ 无告警")
            return
        
        print("\n" + "=" * 70)
        print("告警信息".center(70))
        print("=" * 70)
        
        for alert in alerts:
            level_icon = "🔴" if alert['level'] == 'error' else "🟡"
            print(f"\n{level_icon} [{alert['level'].upper()}] {alert['message']}")
            print(f"   时间: {alert['timestamp']}")
        
        print("\n" + "=" * 70)


def run_dashboard_cli():
    """运行监控面板CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据服务监控面板')
    parser.add_argument('--report', action='store_true', help='生成并显示报告')
    parser.add_argument('--alerts', action='store_true', help='显示告警信息')
    parser.add_argument('--save', action='store_true', help='保存报告到文件')
    parser.add_argument('--history', type=int, default=0, help='显示最近N天的历史报告')
    
    args = parser.parse_args()
    
    dashboard = DataServiceDashboard()
    
    if args.report or (not args.alerts and args.history == 0):
        report = dashboard.generate_full_report()
        dashboard.print_report(report)
        
        if args.save:
            filepath = dashboard.save_report(report)
            print(f"\n报告已保存: {filepath}")
    
    if args.alerts:
        dashboard.print_alerts()
    
    if args.history > 0:
        reports = dashboard.get_historical_reports(args.history)
        print(f"\n📊 最近 {len(reports)} 天历史报告:")
        for r in reports:
            print(f"   {r['timestamp']}: 评分={r['quality_score']:.1f}, 状态={r['health_status']}")


if __name__ == "__main__":
    # 测试
    print("=" * 70)
    print("测试: 数据服务监控面板")
    print("=" * 70)
    
    dashboard = DataServiceDashboard()
    
    # 测试收集指标
    print("\n1. 测试收集指标:")
    metrics = dashboard.collect_metrics()
    print(f"指标时间戳: {metrics.timestamp}")
    print(f"质量评分: {metrics.quality_score}")
    
    # 测试生成报告
    print("\n2. 测试生成报告:")
    report = dashboard.generate_full_report()
    print(f"健康状态: {report['health_status']}")
    
    # 测试打印报告
    print("\n3. 测试打印报告:")
    dashboard.print_report(report)
    
    # 测试告警检查
    print("\n4. 测试告警检查:")
    alerts = dashboard.check_alerts()
    print(f"告警数量: {len(alerts)}")
    for alert in alerts:
        print(f"   [{alert['level']}] {alert['message']}")
