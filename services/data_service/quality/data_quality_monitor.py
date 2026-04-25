#!/usr/bin/env python3
"""
数据质量监控模块

监控所有数据的质量:
- 财务数据质量: 会计恒等式、数据完整性
- 市场行为数据质量: 数据及时性、准确性
- 公告数据质量: 数据完整性、分类准确性

功能:
- 实时监控数据质量
- 生成质量报告
- 异常告警
"""
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from pathlib import Path
import json

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.quality.financial import FinancialDataValidator
from services.data_service.storage.financial_storage import FinancialStorageManager

logger = setup_logger("data_quality_monitor", log_file="system/data_quality_monitor.log")


@dataclass
class QualityCheckResult:
    """质量检查结果"""
    data_type: str                    # 数据类型
    check_time: str                   # 检查时间
    is_passed: bool                   # 是否通过
    score: float                      # 质量评分(0-100)
    issues: List[Dict] = field(default_factory=list)  # 问题列表
    stats: Dict = field(default_factory=dict)         # 统计信息


class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(self):
        self.logger = logger
        self.financial_validator = FinancialDataValidator()
        self.financial_storage = FinancialStorageManager()
        
        # 报告目录
        self.report_dir = get_data_path() / "quality_reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def check_financial_data_quality(
        self,
        sample_size: int = 100
    ) -> QualityCheckResult:
        """
        检查财务数据质量
        
        Args:
            sample_size: 抽样检查数量
        
        Returns:
            质量检查结果
        """
        self.logger.info(f"开始财务数据质量检查,抽样{sample_size}只股票")
        
        # 获取已存储的股票代码
        all_codes = self.financial_storage.get_available_codes('balance_sheet')
        
        # 抽样
        import random
        sample_codes = random.sample(all_codes, min(sample_size, len(all_codes)))
        
        issues = []
        total_checks = 0
        passed_checks = 0
        
        for code in sample_codes:
            try:
                # 加载数据
                bs = self.financial_storage.load_balance_sheet(code)
                inc = self.financial_storage.load_income_statement(code)
                cf = self.financial_storage.load_cash_flow(code)
                
                if bs.empty:
                    continue
                
                # 检查最新一期数据
                latest_date = bs.iloc[0]['report_date']
                
                # 执行验证
                bs_row = bs[bs['report_date'] == latest_date]
                inc_row = inc[inc['report_date'] == latest_date] if not inc.empty else pd.DataFrame()
                cf_row = cf[cf['report_date'] == latest_date] if not cf.empty else pd.DataFrame()
                
                # 验证资产负债表
                if not bs_row.empty:
                    results = self.financial_validator.validate_balance_sheet(bs_row, code, latest_date)
                    for r in results:
                        total_checks += 1
                        if r.is_passed:
                            passed_checks += 1
                        else:
                            issues.append({
                                'code': code,
                                'date': latest_date,
                                'type': 'balance_sheet',
                                'rule': r.rule_name,
                                'message': r.message,
                                'level': r.level.name
                            })
                
                # 验证利润表
                if not inc_row.empty:
                    results = self.financial_validator.validate_income_statement(inc_row, code, latest_date)
                    for r in results:
                        total_checks += 1
                        if r.is_passed:
                            passed_checks += 1
                        else:
                            issues.append({
                                'code': code,
                                'date': latest_date,
                                'type': 'income_statement',
                                'rule': r.rule_name,
                                'message': r.message,
                                'level': r.level.name
                            })
                
            except Exception as e:
                self.logger.error(f"{code} 质量检查失败: {e}")
        
        # 计算评分
        score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        result = QualityCheckResult(
            data_type='financial',
            check_time=datetime.now().isoformat(),
            is_passed=score >= 90,  # 90分以上为通过
            score=round(score, 2),
            issues=issues[:50],  # 只保留前50个问题
            stats={
                'sample_size': len(sample_codes),
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'failed_checks': total_checks - passed_checks,
                'error_count': len([i for i in issues if i['level'] == 'ERROR']),
                'warning_count': len([i for i in issues if i['level'] == 'WARNING']),
            }
        )
        
        self.logger.info(f"财务数据质量检查完成: 评分{result.score}")
        return result
    
    def check_data_freshness(self) -> QualityCheckResult:
        """
        检查数据新鲜度
        
        检查各类数据的最后更新时间
        """
        self.logger.info("开始数据新鲜度检查")
        
        issues = []
        now = datetime.now()
        
        # 检查财务数据
        storage_stats = self.financial_storage.get_storage_stats()
        
        # 检查市场行为数据
        market_behavior_dir = get_data_path() / "market_behavior"
        if market_behavior_dir.exists():
            dragon_tiger_files = list((market_behavior_dir / "dragon_tiger").glob("*.parquet"))
            if dragon_tiger_files:
                latest_file = max(dragon_tiger_files, key=lambda p: p.stat().st_mtime)
                latest_date = datetime.fromtimestamp(latest_file.stat().st_mtime)
                days_since_update = (now - latest_date).days
                
                if days_since_update > 1:
                    issues.append({
                        'type': 'dragon_tiger',
                        'message': f'龙虎榜数据已{days_since_update}天未更新',
                        'level': 'WARNING'
                    })
        
        # 检查公告数据
        announcement_dir = get_data_path() / "announcements"
        if announcement_dir.exists():
            major_events_files = list((announcement_dir / "major_events").glob("*.parquet"))
            if major_events_files:
                latest_file = max(major_events_files, key=lambda p: p.stat().st_mtime)
                latest_date = datetime.fromtimestamp(latest_file.stat().st_mtime)
                days_since_update = (now - latest_date).days
                
                if days_since_update > 1:
                    issues.append({
                        'type': 'announcements',
                        'message': f'公告数据已{days_since_update}天未更新',
                        'level': 'WARNING'
                    })
        
        # 计算评分
        score = 100 - len(issues) * 10
        score = max(0, score)
        
        result = QualityCheckResult(
            data_type='freshness',
            check_time=datetime.now().isoformat(),
            is_passed=score >= 90,
            score=score,
            issues=issues,
            stats={
                'financial_storage': storage_stats,
                'issues_count': len(issues)
            }
        )
        
        self.logger.info(f"数据新鲜度检查完成: 评分{result.score}")
        return result
    
    def check_data_completeness(self) -> QualityCheckResult:
        """
        检查数据完整性
        
        检查是否有缺失的数据字段
        """
        self.logger.info("开始数据完整性检查")
        
        issues = []
        
        # 获取已存储的股票代码
        codes = self.financial_storage.get_available_codes('balance_sheet')
        
        # 抽样检查
        import random
        sample_codes = random.sample(codes, min(50, len(codes)))
        
        incomplete_count = 0
        
        for code in sample_codes:
            try:
                bs = self.financial_storage.load_balance_sheet(code)
                inc = self.financial_storage.load_income_statement(code)
                cf = self.financial_storage.load_cash_flow(code)
                
                # 检查关键字段
                if not bs.empty:
                    required_fields = ['total_assets', 'total_liabilities', 'total_equity']
                    missing = [f for f in required_fields if f not in bs.columns]
                    if missing:
                        incomplete_count += 1
                        issues.append({
                            'code': code,
                            'type': 'balance_sheet',
                            'missing_fields': missing,
                            'level': 'ERROR'
                        })
                
                if not inc.empty:
                    required_fields = ['operating_revenue', 'net_profit']
                    missing = [f for f in required_fields if f not in inc.columns]
                    if missing:
                        incomplete_count += 1
                        issues.append({
                            'code': code,
                            'type': 'income_statement',
                            'missing_fields': missing,
                            'level': 'ERROR'
                        })
                
            except Exception as e:
                self.logger.error(f"{code} 完整性检查失败: {e}")
        
        # 计算评分
        score = ((len(sample_codes) - incomplete_count) / len(sample_codes) * 100) if sample_codes else 0
        
        result = QualityCheckResult(
            data_type='completeness',
            check_time=datetime.now().isoformat(),
            is_passed=score >= 95,
            score=round(score, 2),
            issues=issues[:20],
            stats={
                'sample_size': len(sample_codes),
                'incomplete_count': incomplete_count,
                'complete_count': len(sample_codes) - incomplete_count
            }
        )
        
        self.logger.info(f"数据完整性检查完成: 评分{result.score}")
        return result
    
    def run_full_quality_check(self) -> Dict[str, QualityCheckResult]:
        """
        执行完整质量检查
        
        Returns:
            各类数据的质量检查结果
        """
        self.logger.info("=" * 60)
        self.logger.info("开始完整数据质量检查")
        self.logger.info("=" * 60)
        
        results = {
            'financial': self.check_financial_data_quality(),
            'freshness': self.check_data_freshness(),
            'completeness': self.check_data_completeness(),
        }
        
        # 生成报告
        self.generate_quality_report(results)
        
        self.logger.info("完整数据质量检查完成")
        return results
    
    def generate_quality_report(
        self,
        results: Dict[str, QualityCheckResult],
        output_file: Optional[str] = None
    ) -> str:
        """
        生成质量报告
        
        Args:
            results: 质量检查结果
            output_file: 输出文件路径
        
        Returns:
            报告文件路径
        """
        if output_file is None:
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = self.report_dir / f"quality_report_{date_str}.json"
        
        # 转换结果为字典
        report = {
            'generated_at': datetime.now().isoformat(),
            'overall_score': sum(r.score for r in results.values()) / len(results) if results else 0,
            'checks': {}
        }
        
        for check_type, result in results.items():
            report['checks'][check_type] = {
                'is_passed': result.is_passed,
                'score': result.score,
                'issues_count': len(result.issues),
                'issues': result.issues[:10],  # 只保留前10个问题
                'stats': result.stats
            }
        
        # 保存报告
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"质量报告已保存: {output_file}")
        return str(output_file)
    
    def get_latest_report(self) -> Optional[Dict]:
        """获取最新的质量报告"""
        try:
            files = sorted(self.report_dir.glob("quality_report_*.json"), reverse=True)
            if files:
                with open(files[0], 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"读取最新报告失败: {e}")
        return None
    
    def get_quality_trend(self, days: int = 30) -> List[Dict]:
        """
        获取质量趋势
        
        Args:
            days: 最近几天
        
        Returns:
            质量趋势数据
        """
        try:
            files = sorted(self.report_dir.glob("quality_report_*.json"))
            
            trend = []
            for file_path in files[-days:]:
                with open(file_path, 'r', encoding='utf-8') as f:
                    report = json.load(f)
                    trend.append({
                        'date': report.get('generated_at', ''),
                        'overall_score': report.get('overall_score', 0),
                        'checks': {
                            k: v.get('score', 0) 
                            for k, v in report.get('checks', {}).items()
                        }
                    })
            
            return trend
            
        except Exception as e:
            self.logger.error(f"获取质量趋势失败: {e}")
            return []


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 数据质量监控")
    print("=" * 60)
    
    monitor = DataQualityMonitor()
    
    # 测试财务数据质量
    print("\n1. 测试财务数据质量检查:")
    result = monitor.check_financial_data_quality(sample_size=10)
    print(f"评分: {result.score}")
    print(f"通过: {result.is_passed}")
    print(f"问题数: {len(result.issues)}")
    
    # 测试数据新鲜度
    print("\n2. 测试数据新鲜度检查:")
    result = monitor.check_data_freshness()
    print(f"评分: {result.score}")
    print(f"问题数: {len(result.issues)}")
    
    # 测试完整性
    print("\n3. 测试数据完整性检查:")
    result = monitor.check_data_completeness()
    print(f"评分: {result.score}")
    
    # 测试生成报告
    print("\n4. 测试生成质量报告:")
    results = monitor.run_full_quality_check()
    print(f"报告已生成")
