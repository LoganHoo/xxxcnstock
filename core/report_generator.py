"""
报告生成器
负责生成决策报告
"""
import polars as pl
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def generate_decision_report(
    result: Dict[str, Any], 
    load_meta: Optional[Dict[str, Any]] = None, 
    config: Optional[Dict[str, Any]] = None
) -> str:
    """
    生成决策报告
    
    Args:
        result: 执行结果
        load_meta: 数据加载元数据
        config: 配置
    
    Returns:
        报告文本
    """
    logger.info("[REPORT] 开始生成决策报告")
    
    report_lines = []
    
    report_lines.append("=" * 60)
    report_lines.append("资 金 行 为 学 策 略 决 策 报 告")
    report_lines.append("=" * 60)
    report_lines.append("")
    
    # 数据概览
    if load_meta:
        report_lines.append("📊 数据概览")
        report_lines.append("-" * 40)
        report_lines.append(f"总记录数: {load_meta.get('total_records', 'N/A')}")
        report_lines.append(f"股票数量: {load_meta.get('unique_stocks', 'N/A')}")
        date_range = load_meta.get('date_range', {})
        report_lines.append(f"日期范围: {date_range.get('start', 'N/A')} ~ {date_range.get('end', 'N/A')}")
        report_lines.append("")
    
    # 策略执行摘要
    report_lines.append("🎯 策略执行摘要")
    report_lines.append("-" * 40)
    
    summary = result.get('summary', {})
    report_lines.append(f"候选股票数: {summary.get('total_candidates', 'N/A')}")
    report_lines.append(f"过滤后股票数: {summary.get('total_filtered', 'N/A')}")
    
    filters_applied = summary.get('filters_applied', [])
    if filters_applied:
        report_lines.append(f"应用过滤器: {', '.join(filters_applied)}")
    
    report_lines.append("")
    
    # 股票推荐
    report_lines.append("📈 股票推荐（Top 20）")
    report_lines.append("-" * 40)
    
    stocks = result.get('stocks', [])
    
    if stocks:
        for i, stock in enumerate(stocks[:20], 1):
            report_lines.append(
                f"{i:2d}. {stock.get('code', 'N/A')} {stock.get('name', ''):8s} "
                f"价格: {stock.get('close', 'N/A'):8.2f} "
                f"成交量: {stock.get('volume', 0):>12,.0f}"
            )
    else:
        report_lines.append("无推荐股票")
    
    report_lines.append("")
    
    # 过滤器统计
    filter_stats = result.get('filter_stats', {})
    if filter_stats:
        report_lines.append("🔍 过滤器统计")
        report_lines.append("-" * 40)
        
        filters = filter_stats.get('filters', [])
        for fs in filters:
            if 'removed' in fs:
                report_lines.append(
                    f"{fs.get('name', 'N/A'):20s}: 移除 {fs.get('removed', 0):>5d} 只"
                )
        
        report_lines.append(f"共移除 {filter_stats.get('total_removed', 0)} 只股票")
        report_lines.append("")
    
    # 错误信息
    if 'error' in result:
        report_lines.append("❌ 错误信息")
        report_lines.append("-" * 40)
        report_lines.append(result['error'])
        report_lines.append("")
    
    report_lines.append("=" * 60)
    report_lines.append("报 告 生成 时 间 : " + str(__import__('datetime').datetime.now()))
    report_lines.append("=" * 60)
    
    report_text = "\n".join(report_lines)
    
    logger.info("[REPORT] ✅ 决策报告生成完成")
    
    return report_text
