#!/usr/bin/env python3
"""
数据审计脚本

执行全面的数据审计:
- 数据质量审计
- 数据血缘审计
- 操作审计
- 生成审计报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.audit import (
    AuditLogger, DataLineageTracker, OperationAuditor,
    ChangeAuditor, AuditReporter
)
from services.data_service.audit.audit_logger import get_audit_logger, AuditType, AuditLevel
from services.data_service.audit.operation_audit import get_operation_auditor, OperationType, OperationStatus
from services.data_service.audit.change_audit import get_change_auditor, ChangeType
from services.data_service.audit.data_lineage import get_lineage_tracker, DataSourceType, TransformationType
from services.data_service.audit.audit_reporter import get_audit_reporter, ReportType


class DataAuditor:
    """数据审计器"""
    
    def __init__(self):
        """初始化数据审计器"""
        self.logger = setup_logger("data_auditor")
        
        # 各审计模块
        self.audit_logger = get_audit_logger()
        self.operation_auditor = get_operation_auditor()
        self.change_auditor = get_change_auditor()
        self.lineage_tracker = get_lineage_tracker()
        self.reporter = get_audit_reporter()
        
        self.data_dir = get_data_path()
    
    def audit_data_quality(self, data_type: str = "all") -> Dict[str, Any]:
        """
        审计数据质量
        
        Args:
            data_type: 数据类型 (kline, financial, market_behavior, announcement, all)
        
        Returns:
            审计结果
        """
        self.logger.info(f"开始数据质量审计: {data_type}")
        
        results = {
            'audit_time': datetime.now().isoformat(),
            'data_type': data_type,
            'checks': []
        }
        
        if data_type in ['kline', 'all']:
            results['checks'].append(self._audit_kline_data())
        
        if data_type in ['financial', 'all']:
            results['checks'].append(self._audit_financial_data())
        
        if data_type in ['market_behavior', 'all']:
            results['checks'].append(self._audit_market_behavior_data())
        
        if data_type in ['announcement', 'all']:
            results['checks'].append(self._audit_announcement_data())
        
        # 计算总体评分
        total_checks = len(results['checks'])
        passed_checks = sum(1 for check in results['checks'] if check['passed'])
        results['overall_score'] = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        results['passed_checks'] = passed_checks
        results['total_checks'] = total_checks
        
        self.logger.info(f"数据质量审计完成: 评分 {results['overall_score']:.1f}%")
        
        return results
    
    def _audit_kline_data(self) -> Dict[str, Any]:
        """审计K线数据"""
        check = {
            'name': 'K线数据质量检查',
            'passed': True,
            'issues': []
        }
        
        kline_dir = self.data_dir / "kline"
        
        if not kline_dir.exists():
            check['passed'] = False
            check['issues'].append("K线数据目录不存在")
            return check
        
        parquet_files = list(kline_dir.glob("*.parquet"))
        
        if len(parquet_files) == 0:
            check['passed'] = False
            check['issues'].append("未找到K线数据文件")
            return check
        
        # 抽样检查
        sample_size = min(100, len(parquet_files))
        sample_files = parquet_files[:sample_size]
        
        issues_found = 0
        
        for file in sample_files:
            try:
                df = pd.read_parquet(file)
                
                # 检查必要列
                required_cols = ['trade_date', 'open', 'close', 'high', 'low', 'volume']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    check['issues'].append(f"{file.stem}: 缺少列 {missing_cols}")
                    issues_found += 1
                    continue
                
                # 检查价格合理性
                if (df['close'] <= 0).any():
                    check['issues'].append(f"{file.stem}: 收盘价包含非正值")
                    issues_found += 1
                
                # 检查成交量
                if (df['volume'] < 0).any():
                    check['issues'].append(f"{file.stem}: 成交量包含负值")
                    issues_found += 1
                
                # 检查OHLC关系
                invalid_ohlc = df[
                    (df['high'] < df['low']) |
                    (df['high'] < df['open']) |
                    (df['high'] < df['close']) |
                    (df['low'] > df['open']) |
                    (df['low'] > df['close'])
                ]
                
                if len(invalid_ohlc) > 0:
                    check['issues'].append(f"{file.stem}: 发现{len(invalid_ohlc)}条OHLC关系异常")
                    issues_found += 1
                    
            except Exception as e:
                check['issues'].append(f"{file.stem}: 读取失败 - {str(e)}")
                issues_found += 1
        
        if issues_found > sample_size * 0.1:  # 超过10%样本有问题
            check['passed'] = False
        
        check['sampled_files'] = sample_size
        check['issues_found'] = issues_found
        
        return check
    
    def _audit_financial_data(self) -> Dict[str, Any]:
        """审计财务数据"""
        check = {
            'name': '财务数据质量检查',
            'passed': True,
            'issues': []
        }
        
        financial_dir = self.data_dir / "financial"
        
        if not financial_dir.exists():
            check['passed'] = False
            check['issues'].append("财务数据目录不存在")
            return check
        
        # 检查资产负债表
        balance_sheet_dir = financial_dir / "balance_sheet"
        if balance_sheet_dir.exists():
            files = list(balance_sheet_dir.glob("*.parquet"))
            check['balance_sheet_files'] = len(files)
        
        # 检查利润表
        income_dir = financial_dir / "income_statement"
        if income_dir.exists():
            files = list(income_dir.glob("*.parquet"))
            check['income_statement_files'] = len(files)
        
        # 检查现金流量表
        cashflow_dir = financial_dir / "cash_flow"
        if cashflow_dir.exists():
            files = list(cashflow_dir.glob("*.parquet"))
            check['cash_flow_files'] = len(files)
        
        return check
    
    def _audit_market_behavior_data(self) -> Dict[str, Any]:
        """审计市场行为数据"""
        check = {
            'name': '市场行为数据质量检查',
            'passed': True,
            'issues': []
        }
        
        behavior_dir = self.data_dir / "market_behavior"
        
        if not behavior_dir.exists():
            check['passed'] = False
            check['issues'].append("市场行为数据目录不存在")
            return check
        
        # 检查龙虎榜数据
        dragon_tiger_dir = behavior_dir / "dragon_tiger"
        if dragon_tiger_dir.exists():
            files = list(dragon_tiger_dir.glob("*.parquet"))
            check['dragon_tiger_files'] = len(files)
        
        # 检查资金流向数据
        money_flow_dir = behavior_dir / "money_flow"
        if money_flow_dir.exists():
            files = list(money_flow_dir.glob("*.parquet"))
            check['money_flow_files'] = len(files)
        
        return check
    
    def _audit_announcement_data(self) -> Dict[str, Any]:
        """审计公告数据"""
        check = {
            'name': '公告数据质量检查',
            'passed': True,
            'issues': []
        }
        
        announcement_dir = self.data_dir / "announcements"
        
        if not announcement_dir.exists():
            check['passed'] = False
            check['issues'].append("公告数据目录不存在")
            return check
        
        files = list(announcement_dir.glob("*.parquet"))
        check['announcement_files'] = len(files)
        
        return check
    
    def audit_data_lineage(self) -> Dict[str, Any]:
        """
        审计数据血缘
        
        Returns:
            审计结果
        """
        self.logger.info("开始数据血缘审计")
        
        stats = self.lineage_tracker.get_statistics()
        
        result = {
            'audit_time': datetime.now().isoformat(),
            'lineage_stats': stats,
            'checks': []
        }
        
        # 检查是否有孤立节点
        if stats['total_nodes'] > 0:
            # 获取图谱
            graph = self.lineage_tracker._graph
            
            # 检查孤立节点
            isolated = [node for node in graph.nodes() if graph.degree(node) == 0]
            
            check = {
                'name': '孤立节点检查',
                'passed': len(isolated) < stats['total_nodes'] * 0.1,  # 允许10%孤立
                'isolated_nodes': len(isolated),
                'total_nodes': stats['total_nodes']
            }
            result['checks'].append(check)
        
        # 检查是否有环
        check = {
            'name': '血缘环路检查',
            'passed': stats.get('is_dag', True),
            'is_dag': stats.get('is_dag', True)
        }
        result['checks'].append(check)
        
        self.logger.info("数据血缘审计完成")
        
        return result
    
    def audit_operations(self, days: int = 7) -> Dict[str, Any]:
        """
        审计操作记录
        
        Args:
            days: 审计天数
        
        Returns:
            审计结果
        """
        self.logger.info(f"开始操作审计: 最近{days}天")
        
        stats = self.operation_auditor.get_operation_statistics(days=days)
        
        result = {
            'audit_time': datetime.now().isoformat(),
            'period_days': days,
            'operation_stats': stats,
            'checks': []
        }
        
        # 检查错误率
        total_ops = stats.get('total_operations', 0)
        error_count = stats.get('error_count', 0)
        error_rate = (error_count / total_ops * 100) if total_ops > 0 else 0
        
        check = {
            'name': '操作错误率检查',
            'passed': error_rate < 5,  # 错误率应低于5%
            'error_rate': f"{error_rate:.2f}%",
            'error_count': error_count,
            'total_operations': total_ops
        }
        result['checks'].append(check)
        
        # 检查异常率
        anomaly_count = stats.get('anomaly_count', 0)
        anomaly_rate = (anomaly_count / total_ops * 100) if total_ops > 0 else 0
        
        check = {
            'name': '异常操作率检查',
            'passed': anomaly_rate < 1,  # 异常率应低于1%
            'anomaly_rate': f"{anomaly_rate:.2f}%",
            'anomaly_count': anomaly_count
        }
        result['checks'].append(check)
        
        self.logger.info("操作审计完成")
        
        return result
    
    def audit_changes(self, days: int = 7) -> Dict[str, Any]:
        """
        审计数据变更
        
        Args:
            days: 审计天数
        
        Returns:
            审计结果
        """
        self.logger.info(f"开始变更审计: 最近{days}天")
        
        stats = self.change_auditor.get_change_statistics(days=days)
        
        result = {
            'audit_time': datetime.now().isoformat(),
            'period_days': days,
            'change_stats': stats,
            'checks': []
        }
        
        # 检查删除操作比例
        total_changes = stats.get('total_changes', 0)
        delete_count = stats.get('delete_count', 0)
        delete_rate = (delete_count / total_changes * 100) if total_changes > 0 else 0
        
        check = {
            'name': '数据删除比例检查',
            'passed': delete_rate < 10,  # 删除比例应低于10%
            'delete_rate': f"{delete_rate:.2f}%",
            'delete_count': delete_count,
            'total_changes': total_changes
        }
        result['checks'].append(check)
        
        # 检查存储大小
        storage_size = stats.get('storage_size_mb', 0)
        
        check = {
            'name': '审计存储大小检查',
            'passed': storage_size < 1000,  # 应小于1GB
            'storage_size_mb': storage_size
        }
        result['checks'].append(check)
        
        self.logger.info("变更审计完成")
        
        return result
    
    def generate_audit_report(self, report_type: str = "daily") -> Path:
        """
        生成审计报告
        
        Args:
            report_type: 报告类型 (daily, weekly, compliance, security)
        
        Returns:
            报告文件路径
        """
        self.logger.info(f"生成审计报告: {report_type}")
        
        if report_type == "daily":
            report = self.reporter.generate_daily_report()
        elif report_type == "weekly":
            report = self.reporter.generate_weekly_report()
        elif report_type == "compliance":
            report = self.reporter.generate_compliance_report()
        elif report_type == "security":
            report = self.reporter.generate_security_report()
        else:
            raise ValueError(f"未知的报告类型: {report_type}")
        
        # 导出为HTML
        html_path = self.reporter.export_report_to_html(report)
        
        self.logger.info(f"审计报告已生成: {html_path}")
        
        return html_path
    
    def run_full_audit(self) -> Dict[str, Any]:
        """
        执行完整审计
        
        Returns:
            完整审计结果
        """
        self.logger.info("开始完整数据审计")
        
        results = {
            'audit_time': datetime.now().isoformat(),
            'data_quality': self.audit_data_quality(),
            'data_lineage': self.audit_data_lineage(),
            'operations': self.audit_operations(),
            'changes': self.audit_changes()
        }
        
        # 计算总体健康度
        checks = []
        checks.extend(results['data_quality'].get('checks', []))
        checks.extend(results['data_lineage'].get('checks', []))
        checks.extend(results['operations'].get('checks', []))
        checks.extend(results['changes'].get('checks', []))
        
        total_checks = len(checks)
        passed_checks = sum(1 for check in checks if check.get('passed', False))
        
        results['overall_health'] = {
            'score': (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            'passed_checks': passed_checks,
            'total_checks': total_checks,
            'status': 'HEALTHY' if passed_checks == total_checks else 'WARNING' if passed_checks >= total_checks * 0.8 else 'CRITICAL'
        }
        
        self.logger.info(f"完整审计完成: 健康度 {results['overall_health']['score']:.1f}%")
        
        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据审计工具')
    parser.add_argument('--type', choices=['quality', 'lineage', 'operations', 'changes', 'full', 'report'],
                       default='full', help='审计类型')
    parser.add_argument('--data-type', default='all',
                       choices=['all', 'kline', 'financial', 'market_behavior', 'announcement'],
                       help='数据类型(仅用于quality审计)')
    parser.add_argument('--days', type=int, default=7, help='审计天数')
    parser.add_argument('--report-type', default='daily',
                       choices=['daily', 'weekly', 'compliance', 'security'],
                       help='报告类型')
    parser.add_argument('--output', help='输出文件路径')
    parser.add_argument('--format', choices=['json', 'console'], default='console',
                       help='输出格式')
    
    args = parser.parse_args()
    
    # 创建审计器
    auditor = DataAuditor()
    
    # 执行审计
    if args.type == 'quality':
        results = auditor.audit_data_quality(args.data_type)
    elif args.type == 'lineage':
        results = auditor.audit_data_lineage()
    elif args.type == 'operations':
        results = auditor.audit_operations(args.days)
    elif args.type == 'changes':
        results = auditor.audit_changes(args.days)
    elif args.type == 'report':
        report_path = auditor.generate_audit_report(args.report_type)
        print(f"\n✅ 审计报告已生成: {report_path}")
        return
    else:  # full
        results = auditor.run_full_audit()
    
    # 输出结果
    if args.format == 'json':
        output = json.dumps(results, ensure_ascii=False, indent=2)
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output)
            print(f"\n✅ 审计结果已保存: {args.output}")
        else:
            print(output)
    else:
        # 控制台输出
        print("\n" + "="*60)
        print("数据审计报告")
        print("="*60)
        
        if 'overall_health' in results:
            health = results['overall_health']
            print(f"\n📊 总体健康度: {health['score']:.1f}%")
            print(f"   状态: {health['status']}")
            print(f"   通过检查: {health['passed_checks']}/{health['total_checks']}")
        
        if 'data_quality' in results:
            print("\n📈 数据质量审计:")
            dq = results['data_quality']
            print(f"   评分: {dq.get('overall_score', 0):.1f}%")
            for check in dq.get('checks', []):
                status = "✅" if check['passed'] else "❌"
                print(f"   {status} {check['name']}")
                if check.get('issues'):
                    for issue in check['issues'][:5]:  # 最多显示5个问题
                        print(f"      - {issue}")
        
        if 'data_lineage' in results:
            print("\n🔗 数据血缘审计:")
            lineage = results['data_lineage']
            stats = lineage.get('lineage_stats', {})
            print(f"   节点数: {stats.get('total_nodes', 0)}")
            print(f"   边数: {stats.get('total_edges', 0)}")
            for check in lineage.get('checks', []):
                status = "✅" if check['passed'] else "❌"
                print(f"   {status} {check['name']}")
        
        if 'operations' in results:
            print("\n👤 操作审计:")
            ops = results['operations']
            stats = ops.get('operation_stats', {})
            print(f"   总操作数: {stats.get('total_operations', 0)}")
            print(f"   错误数: {stats.get('error_count', 0)}")
            print(f"   异常数: {stats.get('anomaly_count', 0)}")
            for check in ops.get('checks', []):
                status = "✅" if check['passed'] else "❌"
                print(f"   {status} {check['name']}: {check.get('error_rate', check.get('anomaly_rate', 'N/A'))}")
        
        if 'changes' in results:
            print("\n📝 变更审计:")
            changes = results['changes']
            stats = changes.get('change_stats', {})
            print(f"   总变更数: {stats.get('total_changes', 0)}")
            print(f"   存储大小: {stats.get('storage_size_mb', 0):.2f} MB")
            for check in changes.get('checks', []):
                status = "✅" if check['passed'] else "❌"
                print(f"   {status} {check['name']}")
        
        print("\n" + "="*60)
        
        # 保存到文件
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"✅ 详细结果已保存: {args.output}")


if __name__ == "__main__":
    main()
