#!/usr/bin/env python3
"""
日常运营工作流

实现日常运营业务流:
- 每日数据更新
- 数据质量检查
- 系统健康检查
- 生成运营报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json

from core.logger import setup_logger
from core.paths import get_data_path
from core.market_guardian import is_market_closed
from workflows.data_collection_workflow import DataCollectionWorkflow, CollectionType
from services.data_service.quality.data_quality_monitor import DataQualityMonitor
from services.data_service.audit.audit_reporter import get_audit_reporter, ReportType


class OperationTask(Enum):
    """运营任务"""
    DATA_UPDATE = "data_update"             # 数据更新
    QUALITY_CHECK = "quality_check"         # 质量检查
    HEALTH_CHECK = "health_check"           # 健康检查
    AUDIT_REPORT = "audit_report"           # 审计报告
    CLEANUP = "cleanup"                     # 数据清理
    ALL = "all"                             # 全部任务


@dataclass
class OperationResult:
    """运营结果"""
    task_name: str
    status: str  # success, partial, failed, skipped
    start_time: str
    end_time: str
    duration_seconds: float
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'task_name': self.task_name,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'details': self.details,
            'errors': self.errors
        }


@dataclass
class DailyReport:
    """日常运营报告"""
    date: str
    generated_at: str
    overall_status: str
    tasks_completed: int
    tasks_failed: int
    tasks_skipped: int
    results: Dict[str, OperationResult]
    summary: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'date': self.date,
            'generated_at': self.generated_at,
            'overall_status': self.overall_status,
            'tasks_completed': self.tasks_completed,
            'tasks_failed': self.tasks_failed,
            'tasks_skipped': self.tasks_skipped,
            'results': {k: v.to_dict() for k, v in self.results.items()},
            'summary': self.summary
        }


class DailyOperationWorkflow:
    """日常运营工作流"""
    
    def __init__(self):
        """初始化日常运营工作流"""
        self.logger = setup_logger("daily_operation_workflow")
        
        self.collection_workflow = DataCollectionWorkflow()
        self.quality_monitor = DataQualityMonitor()
        self.audit_reporter = get_audit_reporter()
        
        self.results_dir = get_data_path() / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self,
            tasks: List[OperationTask] = None,
            date: Optional[str] = None,
            skip_market_check: bool = False) -> DailyReport:
        """
        运行日常运营工作流
        
        Args:
            tasks: 要执行的任务列表，默认为全部
            date: 运营日期，默认为今天
            skip_market_check: 是否跳过市场状态检查
        
        Returns:
            日常运营报告
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        if tasks is None:
            tasks = [OperationTask.ALL]
        
        self.logger.info(f"开始日常运营工作流: {date}")
        start_time = datetime.now()
        
        # 检查市场状态
        if not skip_market_check and OperationTask.DATA_UPDATE in tasks or OperationTask.ALL in tasks:
            try:
                is_closed = is_market_closed()
                if not is_closed:
                    self.logger.warning("市场未收盘，跳过数据更新任务")
                    # 移除数据更新任务
                    tasks = [t for t in tasks if t != OperationTask.DATA_UPDATE]
            except:
                pass
        
        results = {}
        
        # 执行数据更新
        if OperationTask.DATA_UPDATE in tasks or OperationTask.ALL in tasks:
            results['data_update'] = self._run_data_update(date)
        
        # 执行质量检查
        if OperationTask.QUALITY_CHECK in tasks or OperationTask.ALL in tasks:
            results['quality_check'] = self._run_quality_check(date)
        
        # 执行健康检查
        if OperationTask.HEALTH_CHECK in tasks or OperationTask.ALL in tasks:
            results['health_check'] = self._run_health_check(date)
        
        # 生成审计报告
        if OperationTask.AUDIT_REPORT in tasks or OperationTask.ALL in tasks:
            results['audit_report'] = self._run_audit_report(date)
        
        # 执行数据清理
        if OperationTask.CLEANUP in tasks or OperationTask.ALL in tasks:
            results['cleanup'] = self._run_cleanup(date)
        
        end_time = datetime.now()
        
        # 生成报告
        report = self._generate_report(date, results, start_time, end_time)
        
        # 保存报告
        self._save_report(report)
        
        self.logger.info(f"日常运营工作流完成: {report.overall_status}")
        
        return report
    
    def _run_data_update(self, date: str) -> OperationResult:
        """运行数据更新"""
        self.logger.info("执行任务: 数据更新")
        start_time = datetime.now()
        
        try:
            # 运行数据采集工作流
            collection_results = self.collection_workflow.run(
                collection_type=CollectionType.ALL,
                date=date,
                validate=True
            )
            
            # 统计结果
            total_collected = sum(r.records_collected for r in collection_results.values())
            failed_collections = sum(1 for r in collection_results.values() if r.status == "failed")
            
            status = "success" if failed_collections == 0 else "partial"
            
            details = {
                'collections': {k: v.to_dict() for k, v in collection_results.items()},
                'total_collected': total_collected
            }
            errors = []
            
        except Exception as e:
            self.logger.error(f"数据更新失败: {e}")
            status = "failed"
            details = {}
            errors = [str(e)]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return OperationResult(
            task_name="data_update",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            details=details,
            errors=errors
        )
    
    def _run_quality_check(self, date: str) -> OperationResult:
        """运行质量检查"""
        self.logger.info("执行任务: 质量检查")
        start_time = datetime.now()
        
        try:
            # 运行数据质量检查
            quality_results = {}
            
            for data_type in ['kline', 'financial', 'market_behavior', 'announcement']:
                try:
                    report = self.quality_monitor.run_quality_check(data_type)
                    quality_results[data_type] = report
                except Exception as e:
                    quality_results[data_type] = {'error': str(e)}
            
            # 计算平均质量分数
            scores = [r.get('overall_score', 0) for r in quality_results.values() if 'overall_score' in r]
            avg_score = sum(scores) / len(scores) if scores else 0
            
            status = "success" if avg_score >= 80 else "partial" if avg_score >= 60 else "failed"
            
            details = {
                'quality_results': quality_results,
                'average_score': avg_score
            }
            errors = []
            
        except Exception as e:
            self.logger.error(f"质量检查失败: {e}")
            status = "failed"
            details = {}
            errors = [str(e)]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return OperationResult(
            task_name="quality_check",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            details=details,
            errors=errors
        )
    
    def _run_health_check(self, date: str) -> OperationResult:
        """运行健康检查"""
        self.logger.info("执行任务: 健康检查")
        start_time = datetime.now()
        
        checks = {}
        errors = []
        
        # 检查1: 磁盘空间
        try:
            import shutil
            stat = shutil.disk_usage(get_data_path())
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            usage_percent = (stat.total - stat.free) / stat.total * 100
            
            checks['disk_space'] = {
                'status': 'ok' if usage_percent < 90 else 'warning',
                'free_gb': round(free_gb, 2),
                'total_gb': round(total_gb, 2),
                'usage_percent': round(usage_percent, 2)
            }
        except Exception as e:
            errors.append(f"磁盘空间检查失败: {e}")
            checks['disk_space'] = {'status': 'error', 'error': str(e)}
        
        # 检查2: 数据文件完整性
        try:
            data_dir = get_data_path()
            parquet_files = list(data_dir.rglob("*.parquet"))
            checks['data_files'] = {
                'status': 'ok',
                'parquet_files': len(parquet_files)
            }
        except Exception as e:
            errors.append(f"数据文件检查失败: {e}")
            checks['data_files'] = {'status': 'error', 'error': str(e)}
        
        # 检查3: 内存使用
        try:
            import psutil
            memory = psutil.virtual_memory()
            checks['memory'] = {
                'status': 'ok' if memory.percent < 90 else 'warning',
                'used_percent': memory.percent,
                'available_gb': round(memory.available / (1024**3), 2)
            }
        except Exception as e:
            errors.append(f"内存检查失败: {e}")
            checks['memory'] = {'status': 'error', 'error': str(e)}
        
        # 确定总体状态
        failed_checks = sum(1 for c in checks.values() if c.get('status') == 'error')
        warning_checks = sum(1 for c in checks.values() if c.get('status') == 'warning')
        
        if failed_checks > 0:
            status = "failed"
        elif warning_checks > 0:
            status = "partial"
        else:
            status = "success"
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return OperationResult(
            task_name="health_check",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            details={'checks': checks},
            errors=errors
        )
    
    def _run_audit_report(self, date: str) -> OperationResult:
        """生成审计报告"""
        self.logger.info("执行任务: 审计报告")
        start_time = datetime.now()
        
        try:
            # 生成日报
            report = self.audit_reporter.generate_daily_report()
            
            # 导出HTML
            html_path = self.audit_reporter.export_report_to_html(report)
            
            status = "success"
            details = {
                'report_id': report.report_id,
                'report_type': report.report_type,
                'html_path': str(html_path),
                'summary': report.summary
            }
            errors = []
            
        except Exception as e:
            self.logger.error(f"审计报告生成失败: {e}")
            status = "failed"
            details = {}
            errors = [str(e)]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return OperationResult(
            task_name="audit_report",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            details=details,
            errors=errors
        )
    
    def _run_cleanup(self, date: str) -> OperationResult:
        """运行数据清理"""
        self.logger.info("执行任务: 数据清理")
        start_time = datetime.now()
        
        cleaned = []
        errors = []
        
        # 清理1: 旧日志文件
        try:
            log_dir = get_data_path() / "logs"
            if log_dir.exists():
                cutoff = datetime.now() - timedelta(days=30)
                removed = 0
                for log_file in log_dir.glob("*.log"):
                    if datetime.fromtimestamp(log_file.stat().st_mtime) < cutoff:
                        log_file.unlink()
                        removed += 1
                cleaned.append(f"日志文件: {removed} 个")
        except Exception as e:
            errors.append(f"日志清理失败: {e}")
        
        # 清理2: 旧审计快照
        try:
            from services.data_service.audit.change_audit import get_change_auditor
            change_auditor = get_change_auditor()
            change_auditor.cleanup_old_snapshots(days=30)
            cleaned.append("审计快照: 已清理")
        except Exception as e:
            errors.append(f"审计快照清理失败: {e}")
        
        # 清理3: 临时文件
        try:
            temp_dir = get_data_path() / "temp"
            if temp_dir.exists():
                removed = 0
                for temp_file in temp_dir.glob("*"):
                    if temp_file.is_file():
                        temp_file.unlink()
                        removed += 1
                cleaned.append(f"临时文件: {removed} 个")
        except Exception as e:
            errors.append(f"临时文件清理失败: {e}")
        
        status = "success" if not errors else "partial"
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return OperationResult(
            task_name="cleanup",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            details={'cleaned_items': cleaned},
            errors=errors
        )
    
    def _generate_report(self,
                        date: str,
                        results: Dict[str, OperationResult],
                        start_time: datetime,
                        end_time: datetime) -> DailyReport:
        """生成运营报告"""
        # 统计任务状态
        tasks_completed = sum(1 for r in results.values() if r.status == "success")
        tasks_failed = sum(1 for r in results.values() if r.status == "failed")
        tasks_skipped = sum(1 for r in results.values() if r.status == "skipped")
        
        # 确定总体状态
        if tasks_failed > 0:
            overall_status = "failed"
        elif tasks_completed == len(results):
            overall_status = "success"
        else:
            overall_status = "partial"
        
        # 生成摘要
        summary = {
            'total_tasks': len(results),
            'tasks_completed': tasks_completed,
            'tasks_failed': tasks_failed,
            'tasks_skipped': tasks_skipped,
            'total_duration': (end_time - start_time).total_seconds(),
            'data_quality_score': 0,
            'system_health': 'unknown'
        }
        
        # 提取数据质量分数
        if 'quality_check' in results and results['quality_check'].status != "failed":
            summary['data_quality_score'] = results['quality_check'].details.get('average_score', 0)
        
        # 提取系统健康状态
        if 'health_check' in results and results['health_check'].status != "failed":
            summary['system_health'] = results['health_check'].status
        
        return DailyReport(
            date=date,
            generated_at=end_time.isoformat(),
            overall_status=overall_status,
            tasks_completed=tasks_completed,
            tasks_failed=tasks_failed,
            tasks_skipped=tasks_skipped,
            results=results,
            summary=summary
        )
    
    def _save_report(self, report: DailyReport):
        """保存运营报告"""
        report_file = self.results_dir / f"daily_operation_{report.date}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"运营报告已保存: {report_file}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='日常运营工作流')
    parser.add_argument('--tasks', nargs='+',
                       choices=['data_update', 'quality_check', 'health_check', 'audit_report', 'cleanup', 'all'],
                       default=['all'], help='要执行的任务')
    parser.add_argument('--date', help='运营日期 (YYYY-MM-DD)')
    parser.add_argument('--skip-market-check', action='store_true', help='跳过市场状态检查')
    
    args = parser.parse_args()
    
    # 创建工作流
    workflow = DailyOperationWorkflow()
    
    # 解析任务
    task_map = {
        'data_update': OperationTask.DATA_UPDATE,
        'quality_check': OperationTask.QUALITY_CHECK,
        'health_check': OperationTask.HEALTH_CHECK,
        'audit_report': OperationTask.AUDIT_REPORT,
        'cleanup': OperationTask.CLEANUP,
        'all': OperationTask.ALL
    }
    tasks = [task_map[t] for t in args.tasks]
    
    # 运行工作流
    report = workflow.run(
        tasks=tasks,
        date=args.date,
        skip_market_check=args.skip_market_check
    )
    
    # 输出结果
    print("\n" + "="*60)
    print(f"日常运营工作流报告: {report.date}")
    print("="*60)
    
    status_icon = "✅" if report.overall_status == "success" else "⚠️" if report.overall_status == "partial" else "❌"
    print(f"\n{status_icon} 总体状态: {report.overall_status}")
    print(f"📊 任务统计: 完成 {report.tasks_completed}, 失败 {report.tasks_failed}, 跳过 {report.tasks_skipped}")
    
    print(f"\n📋 任务详情:")
    for task_name, result in report.results.items():
        task_icon = "✅" if result.status == "success" else "⚠️" if result.status == "partial" else "❌"
        print(f"   {task_icon} {task_name}: {result.status} ({result.duration_seconds:.1f}s)")
    
    print(f"\n📈 摘要:")
    print(f"   数据质量评分: {report.summary.get('data_quality_score', 0):.1f}")
    print(f"   系统健康: {report.summary.get('system_health', 'unknown')}")
    print(f"   总耗时: {report.summary.get('total_duration', 0):.1f}秒")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
