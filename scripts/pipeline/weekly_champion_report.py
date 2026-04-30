#!/usr/bin/env python3
"""
涨停板策略周冠军报告
每周生成策略对比报告，评选冠军策略，输出优化建议

使用方法:
    python scripts/pipeline/weekly_champion_report.py
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger

logger = setup_logger(
    name="weekly_champion_report",
    level="INFO",
    log_file="pipeline/weekly_champion_report.log"
)


def get_last_week_dates():
    """获取上周日期范围"""
    today = datetime.now()
    # 找到上周一和上周日
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def analyze_strategy_performance():
    """分析各策略上周表现"""
    logger.info("分析策略表现...")
    
    # 模拟策略数据（实际应从数据库读取）
    strategies = [
        {"name": "涨停回调", "win_rate": 0.65, "avg_return": 3.2, "trade_count": 15},
        {"name": "龙回头", "win_rate": 0.58, "avg_return": 2.8, "trade_count": 12},
        {"name": "尾盘突袭", "win_rate": 0.52, "avg_return": 2.1, "trade_count": 10},
        {"name": "资金共振", "win_rate": 0.70, "avg_return": 4.1, "trade_count": 8},
    ]
    
    # 评选冠军（按综合得分）
    for s in strategies:
        s['score'] = s['win_rate'] * 0.4 + (s['avg_return'] / 5) * 0.4 + (min(s['trade_count'], 20) / 20) * 0.2
    
    strategies.sort(key=lambda x: x['score'], reverse=True)
    champion = strategies[0]
    
    return strategies, champion


def generate_optimization_suggestions(strategies, champion):
    """生成优化建议"""
    suggestions = []
    
    # 基于冠军策略的建议
    suggestions.append(f"🏆 本周冠军策略: {champion['name']}")
    suggestions.append(f"   胜率: {champion['win_rate']*100:.1f}%, 平均收益: {champion['avg_return']:.2f}%")
    
    # 通用建议
    avg_win_rate = sum(s['win_rate'] for s in strategies) / len(strategies)
    if avg_win_rate < 0.55:
        suggestions.append("⚠️ 整体胜率偏低，建议降低仓位或增加过滤条件")
    
    # 找出表现最差的策略
    worst = strategies[-1]
    if worst['win_rate'] < 0.5:
        suggestions.append(f"📉 {worst['name']}策略表现不佳，建议暂停或优化参数")
    
    return suggestions


def generate_weekly_report():
    """生成周冠军报告"""
    logger.info("=" * 60)
    logger.info("生成涨停板策略周冠军报告")
    logger.info("=" * 60)
    
    last_monday, last_sunday = get_last_week_dates()
    week_range = f"{last_monday.strftime('%m%d')}-{last_sunday.strftime('%m%d')}"
    
    logger.info(f"统计周期: {last_monday.strftime('%Y-%m-%d')} 至 {last_sunday.strftime('%Y-%m-%d')}")
    
    # 分析策略表现
    strategies, champion = analyze_strategy_performance()
    
    # 生成优化建议
    suggestions = generate_optimization_suggestions(strategies, champion)
    
    # 构建报告
    report = {
        "week_range": week_range,
        "start_date": last_monday.strftime('%Y-%m-%d'),
        "end_date": last_sunday.strftime('%Y-%m-%d'),
        "generated_at": datetime.now().isoformat(),
        "champion_strategy": {
            "name": champion['name'],
            "win_rate": champion['win_rate'],
            "avg_return": champion['avg_return'],
            "trade_count": champion['trade_count'],
            "score": champion['score']
        },
        "all_strategies": strategies,
        "optimization_suggestions": suggestions,
        "summary": {
            "total_strategies": len(strategies),
            "avg_win_rate": sum(s['win_rate'] for s in strategies) / len(strategies),
            "avg_return": sum(s['avg_return'] for s in strategies) / len(strategies),
            "total_trades": sum(s['trade_count'] for s in strategies)
        }
    }
    
    # 保存报告
    report_dir = project_root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = report_dir / f"weekly_champion_report_{week_range}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"报告已保存: {report_file}")
    
    # 生成文本版本
    text_report = f"""
{'='*60}
🏆 涨停板策略周冠军报告 ({week_range})
{'='*60}

统计周期: {last_monday.strftime('%Y-%m-%d')} 至 {last_sunday.strftime('%Y-%m-%d')}

📊 策略排名:
"""
    for i, s in enumerate(strategies, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
        text_report += f"{medal} {i}. {s['name']}: 胜率{s['win_rate']*100:.1f}%, 平均收益{s['avg_return']:.2f}%, 交易{s['trade_count']}次\n"
    
    text_report += f"\n💡 优化建议:\n"
    for suggestion in suggestions:
        text_report += f"  {suggestion}\n"
    
    text_report += f"\n{'='*60}\n"
    
    text_file = report_dir / f"weekly_champion_report_{week_range}.txt"
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write(text_report)
    
    logger.info(f"文本报告已保存: {text_file}")
    logger.info("=" * 60)
    
    return report


def main():
    """主函数"""
    try:
        report = generate_weekly_report()
        logger.info("周冠军报告生成完成")
        return 0
    except Exception as e:
        logger.error(f"生成周冠军报告失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
