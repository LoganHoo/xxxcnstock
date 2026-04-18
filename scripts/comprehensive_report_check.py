#!/usr/bin/env python3
"""
报告全面检查脚本

检查所有报告的：
1. 是否正常发出
2. 内容是否完整
3. 是否有错误
4. 数据是否为空、为0、为NA等异常
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.paths import ReportPaths
from services.report_tracking_service import get_tracking_service


class ComprehensiveReportChecker:
    """全面报告检查器"""

    def __init__(self):
        self.tracking = get_tracking_service()
        self.issues = []
        self.warnings = []

    def check_all(self) -> Dict:
        """执行所有检查"""
        print("=" * 80)
        print("【报告全面检查】")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        results = {
            'timestamp': datetime.now().isoformat(),
            'reports': {},
            'summary': {
                'total': 0,
                'healthy': 0,
                'issues': 0,
                'errors': 0
            }
        }

        # 检查所有报告
        reports_to_check = [
            ('晨间报告', 'morning', self._check_morning_report),
            ('战略内参', 'morning_shao', self._check_morning_shao_report),
            ('复盘报告', 'review', self._check_review_report),
            ('夜间报告', 'night', self._check_night_report),
        ]

        for report_name, report_type, check_func in reports_to_check:
            print(f"\n{'='*80}")
            print(f"【{report_name}】")
            print("=" * 80)
            result = check_func()
            results['reports'][report_type] = result
            results['summary']['total'] += 1

            if result['status'] == 'healthy':
                results['summary']['healthy'] += 1
            elif result['status'] == 'issues':
                results['summary']['issues'] += 1
            else:
                results['summary']['errors'] += 1

        # 打印汇总
        self._print_summary(results['summary'])

        # 保存结果
        self._save_results(results)

        return results

    def _check_morning_report(self) -> Dict:
        """检查晨间报告"""
        result = {'status': 'healthy', 'checks': [], 'errors': [], 'warnings': []}

        # 1. 检查数据文件
        data_files = {
            '外盘数据': ReportPaths.foreign_index(),
            '大盘分析': ReportPaths.market_analysis(),
            '每日选股': ReportPaths.daily_picks(),
            '策略结果': ReportPaths.strategy_result(),
            '资金行为': ReportPaths.fund_behavior_result(),
        }

        for name, path in data_files.items():
            check = self._check_data_file(name, path)
            result['checks'].append(check)
            if check['status'] == 'error':
                result['errors'].append(f"{name}: {check['message']}")
            elif check['status'] == 'warning':
                result['warnings'].append(f"{name}: {check['message']}")

        # 2. 检查发送状态
        send_check = self._check_send_status('morning_report')
        result['checks'].append(send_check)

        # 3. 检查内容质量
        content_check = self._check_report_content('morning_report')
        result['checks'].append(content_check)

        # 确定整体状态
        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'issues'

        self._print_check_result('晨间报告', result)
        return result

    def _check_morning_shao_report(self) -> Dict:
        """检查战略内参报告"""
        result = {'status': 'healthy', 'checks': [], 'errors': [], 'warnings': []}

        data_files = {
            '外盘数据': ReportPaths.foreign_index(),
            '大盘分析': ReportPaths.market_analysis(),
            '宏观数据': ReportPaths.macro_data(),
            '原油美元': ReportPaths.oil_dollar_data(),
            '大宗商品': ReportPaths.commodities_data(),
            '情绪数据': ReportPaths.sentiment_data(),
            '新闻数据': ReportPaths.news_data(),
            '资金行为': ReportPaths.fund_behavior_result(),
            '每日选股': ReportPaths.daily_picks(),
        }

        for name, path in data_files.items():
            check = self._check_data_file(name, path)
            result['checks'].append(check)
            if check['status'] == 'error':
                result['errors'].append(f"{name}: {check['message']}")
            elif check['status'] == 'warning':
                result['warnings'].append(f"{name}: {check['message']}")

        send_check = self._check_send_status('morning_shao')
        result['checks'].append(send_check)

        content_check = self._check_report_content('morning_shao')
        result['checks'].append(content_check)

        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'issues'

        self._print_check_result('战略内参', result)
        return result

    def _check_review_report(self) -> Dict:
        """检查复盘报告"""
        result = {'status': 'healthy', 'checks': [], 'errors': [], 'warnings': []}

        data_files = {
            '数据质量': ReportPaths.dq_close(),
            '市场复盘': ReportPaths.market_review(),
            '选股回顾': ReportPaths.daily_picks(),
        }

        for name, path in data_files.items():
            check = self._check_data_file(name, path)
            result['checks'].append(check)
            if check['status'] == 'error':
                result['errors'].append(f"{name}: {check['message']}")
            elif check['status'] == 'warning':
                result['warnings'].append(f"{name}: {check['message']}")

        send_check = self._check_send_status('review_report')
        result['checks'].append(send_check)

        content_check = self._check_report_content('review_report')
        result['checks'].append(content_check)

        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'issues'

        self._print_check_result('复盘报告', result)
        return result

    def _check_night_report(self) -> Dict:
        """检查夜间报告"""
        result = {'status': 'healthy', 'checks': [], 'errors': [], 'warnings': []}

        # 夜间报告通常依赖较少的数据源
        data_files = {
            '外盘数据': ReportPaths.foreign_index(),
            '市场分析': ReportPaths.market_analysis(),
        }

        for name, path in data_files.items():
            check = self._check_data_file(name, path, required=False)
            result['checks'].append(check)
            if check['status'] == 'warning':
                result['warnings'].append(f"{name}: {check['message']}")

        send_check = self._check_send_status('night_report')
        result['checks'].append(send_check)

        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'issues'

        self._print_check_result('夜间报告', result)
        return result

    def _check_data_file(self, name: str, path: Path, required: bool = True) -> Dict:
        """检查数据文件"""
        check = {'name': name, 'path': str(path), 'status': 'ok', 'message': ''}

        if not path.exists():
            check['status'] = 'error' if required else 'warning'
            check['message'] = '文件不存在'
            return check

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 检查数据内容
            content_check = self._validate_data_content(name, data)
            if content_check['has_error']:
                check['status'] = 'error'
                check['message'] = content_check['message']
            elif content_check['has_warning']:
                check['status'] = 'warning'
                check['message'] = content_check['message']
            else:
                check['message'] = '数据正常'

            # 检查文件时间
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            age_hours = (datetime.now() - mtime).total_seconds() / 3600

            if age_hours > 24:
                check['status'] = 'warning'
                check['message'] += f' (数据过旧: {age_hours:.1f}小时)'

        except json.JSONDecodeError as e:
            check['status'] = 'error'
            check['message'] = f'JSON解析错误: {e}'
        except Exception as e:
            check['status'] = 'error'
            check['message'] = f'读取错误: {e}'

        return check

    def _validate_data_content(self, name: str, data: Any) -> Dict:
        """验证数据内容"""
        result = {'has_error': False, 'has_warning': False, 'message': ''}

        if data is None:
            result['has_error'] = True
            result['message'] = '数据为None'
            return result

        if isinstance(data, dict):
            # 检查空字典
            if not data:
                result['has_warning'] = True
                result['message'] = '数据为空字典'
                return result

            # 检查关键字段
            errors = []
            warnings = []

            for key, value in data.items():
                if value is None:
                    errors.append(f'{key}=None')
                elif value == 0 and key not in ['count', 'index', 'code']:
                    warnings.append(f'{key}=0')
                elif value == '':
                    warnings.append(f'{key}=空字符串')
                elif value == 'N/A' or value == 'NA':
                    warnings.append(f'{key}=N/A')
                elif isinstance(value, (list, dict)) and not value:
                    warnings.append(f'{key}=空容器')

            if errors:
                result['has_error'] = True
                result['message'] = f"异常数据: {', '.join(errors[:3])}"
            elif warnings:
                result['has_warning'] = True
                result['message'] = f"警告: {', '.join(warnings[:3])}"

        elif isinstance(data, list):
            if not data:
                result['has_warning'] = True
                result['message'] = '数据为空列表'

        return result

    def _check_send_status(self, report_type: str) -> Dict:
        """检查发送状态"""
        check = {'name': '发送状态', 'status': 'ok', 'message': ''}

        today = datetime.now().strftime('%Y-%m-%d')
        status = self.tracking.get_send_status(report_type, today)

        if status:
            if status['status'] == 'success':
                check['message'] = f"今日已发送 ({status['timestamp']})"
            elif status['status'] == 'failed':
                check['status'] = 'error'
                check['message'] = f"发送失败: {status.get('error', '未知错误')}"
            else:
                check['status'] = 'warning'
                check['message'] = f"状态: {status['status']}"
        else:
            check['status'] = 'warning'
            check['message'] = '今日尚未发送'

        return check

    def _check_report_content(self, report_type: str) -> Dict:
        """检查报告内容质量"""
        check = {'name': '内容质量', 'status': 'ok', 'message': ''}

        # 获取最近24小时的报告记录
        records = self.tracking.get_recent_records(report_type, hours=24)

        if records:
            latest = records[-1]  # 最新的记录
            content_hash = latest.get('content_hash', '')
            error_msg = latest.get('error_message', '')

            if error_msg:
                check['status'] = 'error'
                check['message'] = f"错误: {error_msg[:50]}"
            elif content_hash:
                check['message'] = f"内容完整 (hash: {content_hash[:8]})"
            else:
                check['status'] = 'warning'
                check['message'] = '无内容哈希'
        else:
            check['status'] = 'warning'
            check['message'] = '今日无发送记录'

        return check

    def _print_check_result(self, report_name: str, result: Dict):
        """打印检查结果"""
        status_icon = {
            'healthy': '✅',
            'issues': '⚠️',
            'error': '❌'
        }.get(result['status'], '❓')

        print(f"\n状态: {status_icon} {result['status'].upper()}")

        if result['errors']:
            print("\n  错误:")
            for error in result['errors']:
                print(f"    ❌ {error}")

        if result['warnings']:
            print("\n  警告:")
            for warning in result['warnings']:
                print(f"    ⚠️  {warning}")

        print("\n  详细检查:")
        for check in result['checks']:
            icon = {'ok': '✓', 'warning': '⚠', 'error': '✗'}.get(check['status'], '?')
            print(f"    {icon} {check['name']}: {check['message']}")

    def _print_summary(self, summary: Dict):
        """打印汇总"""
        print("\n" + "=" * 80)
        print("【检查汇总】")
        print("=" * 80)
        print(f"  总报告数: {summary['total']}")
        print(f"  ✅ 健康: {summary['healthy']}")
        print(f"  ⚠️  有问题: {summary['issues']}")
        print(f"  ❌ 错误: {summary['errors']}")

        if summary['errors'] == 0 and summary['issues'] == 0:
            print("\n  🎉 所有报告检查通过！")
        elif summary['errors'] == 0:
            print("\n  ⚠️  存在一些问题，需要关注")
        else:
            print("\n  ❌ 存在严重错误，需要立即处理！")

    def _save_results(self, results: Dict):
        """保存检查结果"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f'comprehensive_check_{timestamp}.json'

        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print(f"\n  详细结果已保存: {log_file}")


def main():
    """主函数"""
    checker = ComprehensiveReportChecker()
    results = checker.check_all()

    # 如果有错误，返回非零退出码
    if results['summary']['errors'] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
