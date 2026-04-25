#!/usr/bin/env python3
"""
运行 Great Expectations 数据质量验证
"""
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.data_service.quality.gx_validator import (
    KlineDataQualitySuite,
    StockListQualitySuite,
    validate_kline_data,
    validate_all_kline_data,
    generate_quality_report
)
import polars as pl


def print_header(title: str):
    """打印标题"""
    print('\n' + '=' * 70)
    print(f'🚀 {title}')
    print('=' * 70)


def print_section(title: str):
    """打印章节"""
    print(f'\n📋 {title}')
    print('-' * 70)


def print_metric(label: str, value: str, status: str = None):
    """打印指标"""
    status_icon = {'pass': '✅', 'fail': '❌', 'warn': '⚠️'}.get(status, '  ')
    print(f'   {status_icon} {label}: {value}')


def format_percent(value: float) -> str:
    """格式化百分比"""
    if value >= 0.99:
        return f'{value:.1%} 🟢'
    elif value >= 0.95:
        return f'{value:.1%} 🟡'
    else:
        return f'{value:.1%} 🔴'


def validate_stock_list():
    """验证股票列表"""
    print_section('1. 验证股票列表')

    stock_list_file = Path('data/stock_list.parquet')
    if not stock_list_file.exists():
        print_metric('状态', '文件不存在', 'fail')
        return None

    df = pl.read_parquet(stock_list_file).to_pandas()
    validator = StockListQualitySuite.create_validator()
    result = validator.validate(df, suite_name='stock_list')

    print_metric('股票数量', f'{len(df):,}')
    print_metric('验证结果', '通过' if result.success else '失败',
                'pass' if result.success else 'fail')
    print_metric('成功率', format_percent(result.success_rate))

    if result.failed_expectations:
        print(f'\n   ⚠️ 失败项 ({len(result.failed_expectations)}个):')
        for exp in result.failed_expectations[:3]:  # 只显示前3个
            print(f'      • {exp.expectation_type}: {exp.unexpected_percent:.1f}%')

    return result


def validate_single_kline():
    """验证单个K线文件"""
    print_section('2. 验证单个K线文件 (000001)')

    kline_file = Path('data/kline/000001.parquet')
    if not kline_file.exists():
        print_metric('状态', '文件不存在', 'fail')
        return None

    result = validate_kline_data(kline_file)

    print_metric('文件', kline_file.name)
    print_metric('数据条数', f'{result.statistics.get("row_count", 0):,}')
    print_metric('验证结果', '通过' if result.success else '失败',
                'pass' if result.success else 'fail')
    print_metric('成功率', format_percent(result.success_rate))
    print_metric('期望总数', str(result.statistics.get('total_expectations', 0)))
    print_metric('通过', str(result.statistics.get('successful_expectations', 0)))
    print_metric('失败', str(result.statistics.get('failed_expectations', 0)))

    if result.failed_expectations:
        print(f'\n   ❌ 失败的期望:')
        for exp in result.failed_expectations:
            print(f'      • {exp.expectation_type}')
            print(f'        列: {exp.column}')
            print(f'        未通过: {exp.unexpected_percent:.1f}%')

    return result


def validate_batch_kline():
    """批量验证K线数据"""
    print_section('3. 批量验证K线数据 (抽样50个)')

    results = validate_all_kline_data(Path('data/kline'), sample_size=50)

    passed = sum(1 for r in results.values() if r.success)
    total = len(results)
    avg_rate = sum(r.success_rate for r in results.values()) / total if total else 0

    print_metric('抽样数量', f'{total}')
    print_metric('通过', f'{passed}', 'pass')
    print_metric('失败', f'{total - passed}', 'fail' if total - passed > 0 else 'pass')
    print_metric('平均成功率', format_percent(avg_rate))

    # 统计失败类型
    failure_types = {}
    for code, result in results.items():
        for exp in result.failed_expectations:
            key = exp.expectation_type
            failure_types[key] = failure_types.get(key, 0) + 1

    if failure_types:
        print(f'\n   📊 失败类型统计:')
        for exp_type, count in sorted(failure_types.items(), key=lambda x: -x[1]):
            print(f'      • {exp_type}: {count}次')

    # 显示失败文件列表（限制数量）
    failed_files = [(code, r) for code, r in results.items() if not r.success]
    if failed_files:
        print(f'\n   ❌ 失败文件 ({len(failed_files)}个):')
        for code, result in failed_files[:5]:  # 只显示前5个
            print(f'      • {code}: {result.success_rate:.1%}')
        if len(failed_files) > 5:
            print(f'      ... 还有 {len(failed_files) - 5} 个')

    return results


def generate_report(results):
    """生成质量报告"""
    print_section('4. 生成质量报告')

    report_path = Path('data/gx_quality_report.json')
    report = generate_quality_report(results, report_path)

    print_metric('报告路径', str(report_path))
    print_metric('生成时间', report['generated_at'][:19])
    print_metric('总体成功率', format_percent(report['summary']['overall_success_rate']))

    return report


def print_summary(stock_result, single_result, batch_results, report):
    """打印总结"""
    print('\n' + '=' * 70)
    print('📊 验证总结')
    print('=' * 70)

    all_pass = (
        (stock_result is None or stock_result.success) and
        (single_result is None or single_result.success) and
        report['summary']['overall_success_rate'] >= 0.95
    )

    if all_pass:
        print('   ✅ 所有检查通过！数据质量良好')
    else:
        print('   ⚠️ 部分检查未通过，请查看详细报告')

    print(f'\n   📈 质量指标:')
    if stock_result:
        print(f'      股票列表: {format_percent(stock_result.success_rate)}')
    if single_result:
        print(f'      样本K线: {format_percent(single_result.success_rate)}')
    print(f'      批量K线: {format_percent(report["summary"]["overall_success_rate"])}')

    print(f'\n   📄 报告文件: data/gx_quality_report.json')
    print('=' * 70)


def main():
    """主函数"""
    print_header('Great Expectations 数据质量验证')
    print(f'   运行时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # 执行验证
    stock_result = validate_stock_list()
    single_result = validate_single_kline()
    batch_results = validate_batch_kline()
    report = generate_report(batch_results)

    # 打印总结
    print_summary(stock_result, single_result, batch_results, report)


if __name__ == "__main__":
    main()
