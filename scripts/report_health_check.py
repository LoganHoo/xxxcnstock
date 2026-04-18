#!/usr/bin/env python3
"""
报告健康检查脚本

检查所有报告的数据就绪状态和发送历史
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.paths import ReportPaths
from core.data_availability import DataAvailabilityChecker
from services.report_tracking_service import get_tracking_service


class ReportHealthChecker:
    """报告健康检查器"""

    def __init__(self):
        self.checker = DataAvailabilityChecker()
        self.tracking = get_tracking_service()

    def check_all_reports(self) -> Dict:
        """检查所有报告"""
        results = {}

        report_configs = {
            'morning_report': {
                'required': ['foreign_index', 'market_analysis', 'daily_picks'],
                'optional': ['strategy_result', 'fund_behavior_result']
            },
            'morning_shao': {
                'required': ['foreign_index', 'market_analysis'],
                'optional': [
                    'macro_data', 'oil_dollar_data', 'commodities_data',
                    'sentiment_data', 'news_data', 'fund_behavior_result', 'daily_picks'
                ]
            },
            'review_report': {
                'required': [],
                'optional': ['dq_close', 'market_review', 'picks_review']
            }
        }

        for report_type, config in report_configs.items():
            results[report_type] = self._check_report(report_type, config)

        return results

    def _check_report(self, report_type: str, config: Dict) -> Dict:
        """检查单个报告"""
        result = {
            'report_type': report_type,
            'timestamp': datetime.now().isoformat(),
            'data_status': {},
            'send_status': {},
            'overall_healthy': True,
            'issues': []
        }

        # 检查数据就绪状态
        for data_source in config['required']:
            available, info = self._check_data_source(data_source)
            result['data_status'][data_source] = {
                'required': True,
                'available': available,
                'info': info
            }
            if not available:
                result['overall_healthy'] = False
                result['issues'].append(f"缺少必需数据: {data_source}")

        for data_source in config['optional']:
            available, info = self._check_data_source(data_source)
            result['data_status'][data_source] = {
                'required': False,
                'available': available,
                'info': info
            }

        # 检查发送状态
        today = datetime.now().strftime('%Y-%m-%d')
        send_status = self.tracking.get_send_status(report_type, today)

        if send_status:
            result['send_status'] = {
                'sent': True,
                'status': send_status['status'],
                'timestamp': send_status['timestamp'],
                'issues': send_status.get('validation_issues', [])
            }
            if send_status['status'] != 'success':
                result['overall_healthy'] = False
                result['issues'].append(f"发送状态异常: {send_status['status']}")
        else:
            result['send_status'] = {'sent': False}
            # 检查是否应该已发送（根据时间）
            should_be_sent = self._should_be_sent(report_type)
            if should_be_sent:
                result['issues'].append("报告尚未发送")

        # 获取成功率统计
        result['success_rate'] = self.tracking.get_success_rate(report_type, days=7)

        return result

    def _check_data_source(self, data_source: str) -> Tuple[bool, str]:
        """检查数据源"""
        path_map = {
            'foreign_index': ReportPaths.foreign_index(),
            'market_analysis': ReportPaths.market_analysis(),
            'daily_picks': ReportPaths.daily_picks(),
            'strategy_result': ReportPaths.strategy_result(),
            'fund_behavior_result': ReportPaths.fund_behavior_result(),
            'macro_data': ReportPaths.macro_data(),
            'oil_dollar_data': ReportPaths.oil_dollar_data(),
            'commodities_data': ReportPaths.commodities_data(),
            'sentiment_data': ReportPaths.sentiment_data(),
            'news_data': ReportPaths.news_data(),
            'dq_close': ReportPaths.dq_close(),
            'market_review': ReportPaths.market_review(),
            'picks_review': ReportPaths.picks_review()
        }

        path = path_map.get(data_source)
        if not path:
            return False, "未知数据源"

        if not path.exists():
            return False, "文件不存在"

        # 检查文件时效性
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age_hours = (datetime.now() - mtime).total_seconds() / 3600

        if age_hours > 24:
            return False, f"数据过旧 ({age_hours:.1f}小时)"

        return True, f"正常 ({age_hours:.1f}小时前)"

    def _should_be_sent(self, report_type: str) -> bool:
        """根据时间判断报告是否应该已发送"""
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        current_time = hour * 60 + minute

        # 定义预期发送时间（分钟从0点开始）
        expected_times = {
            'morning_report': 8 * 60 + 45,  # 08:45
            'morning_shao': 8 * 60 + 45,    # 08:45
            'review_report': 17 * 60 + 30   # 17:30
        }

        expected = expected_times.get(report_type)
        if expected:
            return current_time > expected + 30  # 延迟30分钟宽容度

        return False

    def generate_report(self, results: Dict) -> str:
        """生成健康检查报告"""
        lines = [
            "=" * 80,
            "【报告健康检查】",
            f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            ""
        ]

        for report_type, result in results.items():
            status_icon = "✅" if result['overall_healthy'] else "❌"
            lines.append(f"{status_icon} {report_type}")
            lines.append("-" * 80)

            # 数据状态
            lines.append("  数据状态:")
            for source, status in result['data_status'].items():
                req_icon = "●" if status['required'] else "○"
                avail_icon = "✓" if status['available'] else "✗"
                lines.append(f"    {req_icon} {source}: {avail_icon} {status['info']}")

            # 发送状态
            send_status = result['send_status']
            if send_status.get('sent'):
                status = send_status['status']
                icon = "✅" if status == 'success' else "❌"
                lines.append(f"  发送状态: {icon} {status}")
                lines.append(f"    时间: {send_status['timestamp']}")
                if send_status.get('issues'):
                    lines.append(f"    问题: {len(send_status['issues'])} 个")
            else:
                lines.append("  发送状态: ⏳ 未发送")

            # 成功率
            rate = result['success_rate']
            lines.append(f"  近7天成功率: {rate['success']}/{rate['total']} ({rate['rate']}%)")

            # 问题列表
            if result['issues']:
                lines.append("  问题:")
                for issue in result['issues']:
                    lines.append(f"    ⚠️  {issue}")

            lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)


def main():
    """主函数"""
    print("开始报告健康检查...")

    checker = ReportHealthChecker()
    results = checker.check_all_reports()

    report = checker.generate_report(results)
    print(report)

    # 保存到文件
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"health_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\n报告已保存: {log_file}")

    # 返回状态码
    all_healthy = all(r['overall_healthy'] for r in results.values())
    sys.exit(0 if all_healthy else 1)


if __name__ == "__main__":
    main()
