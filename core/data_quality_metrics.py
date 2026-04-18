#!/usr/bin/env python3
"""
数据质量标准化指标模块
功能：
1. 计算数据质量标准化指标（采集率、完整性、新鲜度、一致性）
2. 生成数据质量评分卡
3. 为复盘报告提供标准化数据质量指标
"""
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict


@dataclass
class DataQualityMetrics:
    """数据质量标准化指标"""
    # 基础指标
    collection_rate: float      # 采集率 (0-100%)
    completeness_rate: float    # 完整性 (0-100%)
    freshness_score: float      # 新鲜度评分 (0-100)
    consistency_score: float    # 一致性评分 (0-100)

    # 综合评分
    overall_score: float        # 综合质量评分 (0-100)
    quality_level: str          # 质量等级: excellent/good/fair/poor

    # 详细统计
    total_stocks: int           # 应采集股票总数
    collected_stocks: int       # 实际采集股票数
    valid_stocks: int           # 有效数据股票数
    invalid_stocks: int         # 无效数据股票数
    missing_fields_count: int   # 缺失字段数
    data_freshness_days: int    # 数据新鲜度（天数）

    # 时间戳
    report_date: str
    generated_at: str


class DataQualityMetricsCalculator:
    """数据质量指标计算器"""

    def __init__(self, project_root: Path = None):
        if project_root is None:
            self.project_root = Path(__file__).parent.parent
        else:
            self.project_root = project_root

        self.kline_dir = self.project_root / "data" / "kline"
        self.audit_dir = self.project_root / "data" / "audit"

    def _log(self, message: str):
        """输出日志"""
        print(f"[DataQualityMetrics] {message}")

    def load_audit_result(self, date: str = None) -> Optional[Dict]:
        """加载审计结果"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        # 尝试加载智能审计结果
        audit_file = self.audit_dir / f"{date}_audit_result.json"
        if audit_file.exists():
            with open(audit_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # 尝试加载统一审计结果
        unified_file = self.audit_dir / f"{date}_unified_audit.json"
        if unified_file.exists():
            with open(unified_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        return None

    def load_dq_report(self, date: str = None) -> Optional[Dict]:
        """加载数据质量报告"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        dq_file = self.project_root / "data" / "reports" / f"{date}_dq_close.json"
        if dq_file.exists():
            with open(dq_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        return None

    def calculate_collection_rate(self, audit_result: Dict) -> float:
        """计算采集率"""
        total = audit_result.get('total_stocks', 0)
        collected = audit_result.get('collected_stocks', 0)

        if total == 0:
            return 0.0

        return round((collected / total) * 100, 2)

    def calculate_completeness_rate(self, audit_result: Dict) -> float:
        """计算完整性"""
        # 尝试从checks.completeness获取
        checks = audit_result.get('checks', {})
        if 'completeness' in checks:
            completeness = checks['completeness']
            total = completeness.get('total', 0)
            valid = completeness.get('valid', 0)
            if total > 0:
                return round((valid / total) * 100, 2)

        # 尝试从顶层completeness获取
        completeness = audit_result.get('completeness', {})
        total = completeness.get('total', 0)
        valid = completeness.get('valid', 0)

        if total == 0:
            return 0.0

        return round((valid / total) * 100, 2)

    def calculate_freshness_score(self, audit_result: Dict) -> float:
        """计算新鲜度评分"""
        # 尝试从checks.freshness获取
        checks = audit_result.get('checks', {})
        if 'freshness' in checks:
            freshness = checks['freshness']
            freshness_rate = freshness.get('freshness_rate', 0)
            return round(freshness_rate * 100, 2)

        # 尝试从顶层freshness获取
        freshness = audit_result.get('freshness', {})
        freshness_rate = freshness.get('freshness_rate', 0)

        # 转换为百分制
        return round(freshness_rate * 100, 2)

    def calculate_consistency_score(self, audit_result: Dict) -> float:
        """计算一致性评分"""
        # 检查是否有异常数据
        issues = audit_result.get('issues', [])

        if not issues:
            return 100.0

        # 根据问题数量扣分
        deduction = min(len(issues) * 5, 50)  # 最多扣50分
        return round(100 - deduction, 2)

    def calculate_overall_score(self, metrics: Dict) -> float:
        """计算综合质量评分"""
        weights = {
            'collection_rate': 0.25,
            'completeness_rate': 0.30,
            'freshness_score': 0.25,
            'consistency_score': 0.20
        }

        overall = sum(
            metrics.get(key, 0) * weight
            for key, weight in weights.items()
        )

        return round(overall, 2)

    def determine_quality_level(self, overall_score: float) -> str:
        """确定质量等级"""
        if overall_score >= 95:
            return 'excellent'
        elif overall_score >= 85:
            return 'good'
        elif overall_score >= 70:
            return 'fair'
        else:
            return 'poor'

    def calculate_metrics(self, date: str = None) -> DataQualityMetrics:
        """计算完整的数据质量指标"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        self._log(f"计算 {date} 的数据质量指标...")

        # 加载审计结果
        audit_result = self.load_audit_result(date) or {}
        dq_report = self.load_dq_report(date) or {}

        # 计算各项指标
        collection_rate = self.calculate_collection_rate(audit_result)
        completeness_rate = self.calculate_completeness_rate(audit_result)
        freshness_score = self.calculate_freshness_score(audit_result)
        consistency_score = self.calculate_consistency_score(audit_result)

        # 计算综合评分
        metrics_dict = {
            'collection_rate': collection_rate,
            'completeness_rate': completeness_rate,
            'freshness_score': freshness_score,
            'consistency_score': consistency_score
        }
        overall_score = self.calculate_overall_score(metrics_dict)
        quality_level = self.determine_quality_level(overall_score)

        # 获取详细统计 - 优先从checks中获取
        checks = audit_result.get('checks', {})
        if 'completeness' in checks:
            completeness = checks['completeness']
            freshness = checks.get('freshness', {})
        else:
            completeness = audit_result.get('completeness', {})
            freshness = audit_result.get('freshness', {})

        # 构建指标对象
        metrics = DataQualityMetrics(
            collection_rate=collection_rate,
            completeness_rate=completeness_rate,
            freshness_score=freshness_score,
            consistency_score=consistency_score,
            overall_score=overall_score,
            quality_level=quality_level,
            total_stocks=audit_result.get('total_stocks', 0),
            collected_stocks=audit_result.get('collected_stocks', 0),
            valid_stocks=completeness.get('valid', 0),
            invalid_stocks=completeness.get('invalid', 0),
            missing_fields_count=audit_result.get('missing_fields_count', 0),
            data_freshness_days=freshness.get('days_since_latest', 0),
            report_date=date,
            generated_at=datetime.now().isoformat()
        )

        return metrics

    def save_metrics(self, metrics: DataQualityMetrics):
        """保存指标到文件"""
        output_file = self.audit_dir / f"{metrics.report_date}_quality_metrics.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, ensure_ascii=False, indent=2)

        self._log(f"数据质量指标已保存: {output_file}")
        return output_file

    def generate_report(self, metrics: DataQualityMetrics) -> str:
        """生成数据质量报告文本"""
        level_map = {
            'excellent': '优秀',
            'good': '良好',
            'fair': '一般',
            'poor': '较差'
        }

        lines = []
        lines.append("=" * 60)
        lines.append("数据质量标准化指标报告")
        lines.append("=" * 60)
        lines.append(f"报告日期: {metrics.report_date}")
        lines.append(f"生成时间: {metrics.generated_at}")
        lines.append("")
        lines.append("【综合评分】")
        lines.append(f"  综合质量评分: {metrics.overall_score:.1f}/100")
        lines.append(f"  质量等级: {level_map.get(metrics.quality_level, metrics.quality_level)}")
        lines.append("")
        lines.append("【详细指标】")
        lines.append(f"  采集率: {metrics.collection_rate:.1f}% ({metrics.collected_stocks}/{metrics.total_stocks})")
        lines.append(f"  完整性: {metrics.completeness_rate:.1f}% ({metrics.valid_stocks}/{metrics.total_stocks})")
        lines.append(f"  新鲜度: {metrics.freshness_score:.1f}/100")
        lines.append(f"  一致性: {metrics.consistency_score:.1f}/100")
        lines.append("")
        lines.append("【数据统计】")
        lines.append(f"  应采集股票: {metrics.total_stocks}只")
        lines.append(f"  实际采集: {metrics.collected_stocks}只")
        lines.append(f"  有效数据: {metrics.valid_stocks}只")
        lines.append(f"  无效数据: {metrics.invalid_stocks}只")
        lines.append(f"  缺失字段: {metrics.missing_fields_count}个")
        lines.append(f"  数据新鲜度: {metrics.data_freshness_days}天")
        lines.append("=" * 60)

        return "\n".join(lines)


def main():
    """主函数 - 用于命令行调用"""
    import argparse

    parser = argparse.ArgumentParser(description='数据质量标准化指标计算')
    parser.add_argument('--date', type=str, help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出文件路径')
    parser.add_argument('--report', action='store_true', help='生成报告文本')

    args = parser.parse_args()

    calculator = DataQualityMetricsCalculator()
    metrics = calculator.calculate_metrics(args.date)

    # 保存指标
    calculator.save_metrics(metrics)

    # 输出
    if args.report:
        report = calculator.generate_report(metrics)
        print(report)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(asdict(metrics), f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(asdict(metrics), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
