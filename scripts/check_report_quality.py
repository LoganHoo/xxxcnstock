"""
报告质量检查工具
用于分析资金行为学策略生成的报告，检查数据完整性
"""
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.pipeline_state import PipelineStateManager, get_pipeline_manager


def check_report_text_quality(report_text: str) -> Dict:
    """检查报告文本质量"""
    issues = []
    warnings = []
    stats = {}

    # 1. 检查报告结构完整性
    required_sections = [
        ("市场环境定性", "1️⃣"),
        ("宏观与外盘环境", "2️⃣"),
        ("防守信号", "4️⃣"),
        ("核心观察点", "5️⃣"),
        ("选股结果", "6️⃣"),
        ("仓位分配建议", "7️⃣"),
        ("今日策略建议", "8️⃣"),
    ]

    for section_name, emoji in required_sections:
        if emoji not in report_text and section_name not in report_text:
            issues.append(f"缺少必要章节: {section_name}")

    # 2. 检查数据空值/零值
    zero_value_patterns = [
        r'收盘价:\s*0\.00',
        r'评分:\s*0\.00',
        r'量比:\s*0\.00',
        r'量能判定：0\.00',
        r'情绪温度：0\.0°',
    ]

    for pattern in zero_value_patterns:
        matches = re.findall(pattern, report_text)
        if matches:
            warnings.append(f"发现零值数据: {pattern}")

    # 3. 检查选股结果
    trend_stock_match = re.search(r'波段趋势：(\d+)只', report_text)
    short_term_match = re.search(r'短线打板：(\d+)只', report_text)

    if trend_stock_match:
        trend_count = int(trend_stock_match.group(1))
        stats['trend_stock_count'] = trend_count
        if trend_count == 0:
            warnings.append("波段趋势选股数量为0")

    if short_term_match:
        short_count = int(short_term_match.group(1))
        stats['short_term_stock_count'] = short_count
        if short_count == 0:
            warnings.append("短线打板选股数量为0")

    # 4. 检查是否有"数据加载中"提示
    if "数据加载中" in report_text:
        issues.append("部分股票数据显示'数据加载中'，说明股票详情缺失")

    # 5. 检查仓位分配
    position_match = re.search(r'波段仓位：(\d+)万', report_text)
    if position_match:
        position_value = int(position_match.group(1))
        stats['trend_position'] = position_value
        if position_value == 0:
            warnings.append("波段仓位为0")

    # 6. 统计报告长度
    stats['report_length'] = len(report_text)
    stats['line_count'] = len(report_text.split('\n'))

    # 7. 检查是否有具体股票代码显示
    stock_codes = re.findall(r'\d{6}\.', report_text)
    stats['stock_code_mentions'] = len(stock_codes)

    return {
        'passed': len(issues) == 0,
        'issues': issues,
        'warnings': warnings,
        'stats': stats
    }


def check_json_result_quality(result: Dict) -> Dict:
    """检查JSON结果数据质量"""
    issues = []
    warnings = []
    stats = {}

    # 1. 检查必要字段
    required_fields = [
        'market_state', 'upward_pivot', 'hedge_effect',
        'trend_stocks', 'short_term_stocks', 'position_size',
        'defense_signals', 'v_total', 'sentiment_temperature'
    ]

    for field in required_fields:
        if field not in result:
            issues.append(f"缺少必要字段: {field}")

    # 2. 检查选股结果
    trend_stocks = result.get('trend_stocks', [])
    short_term_stocks = result.get('short_term_stocks', [])

    stats['trend_stock_count'] = len(trend_stocks)
    stats['short_term_stock_count'] = len(short_term_stocks)

    if len(trend_stocks) == 0:
        warnings.append("波段趋势选股列表为空")
    if len(short_term_stocks) == 0:
        warnings.append("短线打板选股列表为空")

    # 3. 检查股票详情
    trend_detail = result.get('trend_stocks_detail', {})
    short_detail = result.get('short_term_stocks_detail', {})

    stats['trend_detail_count'] = len(trend_detail)
    stats['short_detail_count'] = len(short_detail)

    # 检查详情完整性
    for code in trend_stocks[:5]:
        if code not in trend_detail:
            issues.append(f"波段股 {code} 缺少详细信息")
        else:
            detail = trend_detail[code]
            if detail.get('close', 0) == 0:
                warnings.append(f"波段股 {code} 收盘价为0")
            if not detail.get('name'):
                warnings.append(f"波段股 {code} 名称为空")

    for code in short_term_stocks[:5]:
        if code not in short_detail:
            issues.append(f"短线股 {code} 缺少详细信息")
        else:
            detail = short_detail[code]
            if detail.get('close', 0) == 0:
                warnings.append(f"短线股 {code} 收盘价为0")
            if not detail.get('name'):
                warnings.append(f"短线股 {code} 名称为空")

    # 4. 检查关键指标
    v_total = result.get('v_total', 0)
    sentiment = result.get('sentiment_temperature', 0)

    stats['v_total'] = v_total
    stats['sentiment_temperature'] = sentiment

    if v_total == 0:
        issues.append("总成交额(v_total)为0")
    if sentiment == 0:
        warnings.append("情绪温度为0")

    # 5. 检查仓位分配
    position = result.get('position_size', {})
    stats['position'] = position

    if not position:
        issues.append("仓位分配数据为空")
    else:
        if position.get('trend', 0) == 0:
            warnings.append("波段仓位为0")
        if position.get('short_term', 0) == 0:
            warnings.append("短线仓位为0")

    return {
        'passed': len(issues) == 0,
        'issues': issues,
        'warnings': warnings,
        'stats': stats
    }


def check_latest_report(report_date: str = None) -> Dict:
    """检查最新的报告"""
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')

    print(f"\n{'='*70}")
    print(f"【报告质量检查】日期: {report_date}")
    print(f"{'='*70}\n")

    # 1. 检查JSON结果文件
    pipeline = get_pipeline_manager(report_date)
    execute_result_path = pipeline.get_checkpoint_path("execute_result", ".json")

    json_check = {'passed': False, 'issues': ['未找到JSON结果文件'], 'warnings': [], 'stats': {}}

    if execute_result_path.exists():
        try:
            with open(execute_result_path, 'r', encoding='utf-8') as f:
                result = json.load(f)
            json_check = check_json_result_quality(result)
            print("✅ JSON结果文件检查完成")
        except Exception as e:
            json_check['issues'].append(f"JSON解析失败: {e}")
            print(f"❌ JSON解析失败: {e}")
    else:
        print(f"❌ 未找到JSON结果文件: {execute_result_path}")

    # 2. 检查TXT报告文件
    txt_path = Path(f"data/reports/fund_behavior_{report_date}.txt")
    text_check = {'passed': False, 'issues': ['未找到TXT报告文件'], 'warnings': [], 'stats': {}}

    if txt_path.exists():
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                report_text = f.read()
            text_check = check_report_text_quality(report_text)
            print("✅ TXT报告文件检查完成")
        except Exception as e:
            text_check['issues'].append(f"TXT读取失败: {e}")
            print(f"❌ TXT读取失败: {e}")
    else:
        print(f"❌ 未找到TXT报告文件: {txt_path}")

    # 3. 汇总结果
    print(f"\n{'='*70}")
    print("【检查结果汇总】")
    print(f"{'='*70}\n")

    all_issues = json_check['issues'] + text_check['issues']
    all_warnings = json_check['warnings'] + text_check['warnings']

    if all_issues:
        print("❌ 发现错误:")
        for issue in all_issues:
            print(f"   - {issue}")
        print()

    if all_warnings:
        print("⚠️ 发现警告:")
        for warning in all_warnings:
            print(f"   - {warning}")
        print()

    if not all_issues and not all_warnings:
        print("✅ 报告质量检查全部通过！\n")

    # 统计数据
    print("📊 统计数据:")
    stats = {**json_check.get('stats', {}), **text_check.get('stats', {})}
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print(f"\n{'='*70}\n")

    return {
        'passed': len(all_issues) == 0,
        'issues': all_issues,
        'warnings': all_warnings,
        'stats': stats
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='报告质量检查工具')
    parser.add_argument('--date', type=str, help='指定报告日期 (YYYY-MM-DD格式)')
    args = parser.parse_args()

    result = check_latest_report(args.date)

    if not result['passed']:
        print("\n💡 建议操作:")
        print("   1. 检查数据文件是否完整")
        print("   2. 重新运行策略脚本生成报告")
        print("   3. 检查 core/fund_behavior_strategy.py 中的数据返回逻辑")
        sys.exit(1)
    else:
        print("\n✅ 报告质量检查通过！")
        sys.exit(0)
