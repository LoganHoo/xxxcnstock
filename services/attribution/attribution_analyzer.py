#!/usr/bin/env python3
"""
自动归因分析器

功能：
- 交易结果归因分析
- 盈亏原因分析
- 策略表现归因
- 生成归因报告

归因维度：
1. 选股归因 - 哪些因子贡献了收益
2. 择时归因 - 入场/出场时机分析
3. 仓位归因 - 仓位管理效果分析
4. 市场归因 - 市场环境对收益的影响
5. 风格归因 - 风格因子暴露分析
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import pandas as pd
import polars as pl
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path


class AttributionType(Enum):
    """归因类型"""
    STOCK_SELECTION = "stock_selection"  # 选股归因
    TIMING = "timing"                    # 择时归因
    POSITION = "position"                # 仓位归因
    MARKET = "market"                    # 市场归因
    STYLE = "style"                      # 风格归因


@dataclass
class TradeAttribution:
    """交易归因结果"""
    trade_id: str
    code: str
    name: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    quantity: int
    pnl: float
    pnl_pct: float
    
    # 归因分析
    selection_contrib: float  # 选股贡献
    timing_contrib: float     # 择时贡献
    position_contrib: float   # 仓位贡献
    market_contrib: float     # 市场贡献
    
    # 详细分析
    factors: Dict[str, float]  # 各因子贡献
    analysis: str              # 文字分析
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class DailyAttribution:
    """每日归因"""
    date: str
    total_pnl: float
    total_return: float
    
    # 各维度归因
    selection_attribution: Dict[str, float]
    timing_attribution: Dict[str, float]
    position_attribution: Dict[str, float]
    market_attribution: Dict[str, float]
    
    # 因子暴露
    factor_exposure: Dict[str, float]
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class AttributionReport:
    """归因报告"""
    report_date: str
    period_start: str
    period_end: str
    
    # 总体表现
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    total_return: float
    
    # 归因汇总
    attribution_summary: Dict[str, float]
    
    # 详细分析
    trade_attributions: List[TradeAttribution]
    daily_attributions: List[DailyAttribution]
    
    # 改进建议
    recommendations: List[str]
    
    def to_dict(self) -> Dict:
        return {
            'report_date': self.report_date,
            'period_start': self.period_start,
            'period_end': self.period_end,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': self.win_rate,
            'total_pnl': self.total_pnl,
            'total_return': self.total_return,
            'attribution_summary': self.attribution_summary,
            'trade_attributions': [t.to_dict() for t in self.trade_attributions],
            'daily_attributions': [d.to_dict() for d in self.daily_attributions],
            'recommendations': self.recommendations
        }


class AttributionAnalyzer:
    """归因分析器"""
    
    def __init__(self):
        self.logger = setup_logger("attribution_analyzer")
        self.data_dir = get_data_path()
    
    def analyze_trades(
        self,
        trades: List[Dict],
        market_data: Optional[Dict] = None
    ) -> AttributionReport:
        """
        分析交易归因
        
        Args:
            trades: 交易记录列表
            market_data: 市场数据
        
        Returns:
            归因报告
        """
        self.logger.info(f"开始归因分析: {len(trades)} 笔交易")
        
        # 计算总体表现
        total_stats = self._calculate_total_stats(trades)
        
        # 逐笔归因分析
        trade_attributions = []
        for trade in trades:
            attribution = self._analyze_single_trade(trade, market_data)
            trade_attributions.append(attribution)
        
        # 归因汇总
        attribution_summary = self._summarize_attribution(trade_attributions)
        
        # 每日归因
        daily_attributions = self._analyze_daily_attribution(trades, market_data)
        
        # 生成建议
        recommendations = self._generate_recommendations(
            total_stats, attribution_summary, trade_attributions
        )
        
        # 生成报告
        report = AttributionReport(
            report_date=datetime.now().strftime('%Y-%m-%d'),
            period_start=min(t['entry_date'] for t in trades),
            period_end=max(t['exit_date'] for t in trades),
            total_trades=total_stats['total_trades'],
            winning_trades=total_stats['winning_trades'],
            losing_trades=total_stats['losing_trades'],
            win_rate=total_stats['win_rate'],
            total_pnl=total_stats['total_pnl'],
            total_return=total_stats['total_return'],
            attribution_summary=attribution_summary,
            trade_attributions=trade_attributions,
            daily_attributions=daily_attributions,
            recommendations=recommendations
        )
        
        self.logger.info("归因分析完成")
        
        return report
    
    def _calculate_total_stats(self, trades: List[Dict]) -> Dict:
        """计算总体统计"""
        total_trades = len(trades)
        
        pnls = [t.get('pnl', 0) for t in trades]
        total_pnl = sum(pnls)
        
        winning_trades = sum(1 for p in pnls if p > 0)
        losing_trades = total_trades - winning_trades
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        # 计算收益率
        total_cost = sum(t.get('entry_price', 0) * t.get('quantity', 0) for t in trades)
        total_return = total_pnl / total_cost if total_cost > 0 else 0
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'total_return': total_return
        }
    
    def _analyze_single_trade(
        self,
        trade: Dict,
        market_data: Optional[Dict]
    ) -> TradeAttribution:
        """分析单笔交易归因"""
        code = trade.get('code', '')
        name = trade.get('name', '')
        entry_price = trade.get('entry_price', 0)
        exit_price = trade.get('exit_price', 0)
        quantity = trade.get('quantity', 0)
        pnl = trade.get('pnl', 0)
        pnl_pct = trade.get('pnl_pct', 0)
        
        # 选股归因：基于选股因子得分
        selection_score = trade.get('selection_score', 0.5)
        selection_contrib = pnl * selection_score if pnl > 0 else pnl * (1 - selection_score)
        
        # 择时归因：基于入场/出场时机
        entry_timing_score = self._calculate_timing_score(
            trade.get('entry_date', ''),
            trade.get('entry_price', 0),
            market_data
        )
        exit_timing_score = self._calculate_timing_score(
            trade.get('exit_date', ''),
            trade.get('exit_price', 0),
            market_data
        )
        timing_contrib = pnl * (entry_timing_score + exit_timing_score) / 2
        
        # 仓位归因：基于仓位大小
        position_ratio = trade.get('position_ratio', 0.1)
        position_contrib = pnl * (position_ratio / 0.1)  # 相对于10%基准仓位
        
        # 市场归因：基于市场环境
        market_contrib = pnl * self._calculate_market_contribution(
            trade.get('entry_date', ''),
            trade.get('exit_date', ''),
            market_data
        )
        
        # 各因子贡献
        factors = {
            'momentum_factor': trade.get('momentum_score', 0) * pnl,
            'value_factor': trade.get('value_score', 0) * pnl,
            'quality_factor': trade.get('quality_score', 0) * pnl,
            'technical_factor': trade.get('technical_score', 0) * pnl
        }
        
        # 生成分析文字
        analysis = self._generate_trade_analysis(
            trade, selection_contrib, timing_contrib, position_contrib, market_contrib
        )
        
        return TradeAttribution(
            trade_id=trade.get('id', ''),
            code=code,
            name=name,
            entry_date=trade.get('entry_date', ''),
            exit_date=trade.get('exit_date', ''),
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            pnl=pnl,
            pnl_pct=pnl_pct,
            selection_contrib=selection_contrib,
            timing_contrib=timing_contrib,
            position_contrib=position_contrib,
            market_contrib=market_contrib,
            factors=factors,
            analysis=analysis
        )
    
    def _calculate_timing_score(
        self,
        date: str,
        price: float,
        market_data: Optional[Dict]
    ) -> float:
        """计算择时得分"""
        # 简化的择时评分
        # 实际应基于技术指标、市场情绪等
        return 0.5
    
    def _calculate_market_contribution(
        self,
        entry_date: str,
        exit_date: str,
        market_data: Optional[Dict]
    ) -> float:
        """计算市场贡献"""
        if market_data is None:
            return 0.1  # 默认10%
        
        # 获取期间市场涨跌
        market_return = market_data.get('period_return', 0)
        
        # 市场贡献 = 市场涨跌 * beta
        beta = 1.0  # 假设beta=1
        return market_return * beta
    
    def _generate_trade_analysis(
        self,
        trade: Dict,
        selection_contrib: float,
        timing_contrib: float,
        position_contrib: float,
        market_contrib: float
    ) -> str:
        """生成交易分析文字"""
        pnl = trade.get('pnl', 0)
        
        if pnl > 0:
            analysis = "盈利交易。"
            
            # 找出最大贡献因素
            contribs = {
                '选股': selection_contrib,
                '择时': timing_contrib,
                '仓位': position_contrib,
                '市场': market_contrib
            }
            max_factor = max(contribs, key=contribs.get)
            
            analysis += f"主要归功于{max_factor}。"
            
        else:
            analysis = "亏损交易。"
            
            # 找出最大亏损因素
            contribs = {
                '选股': selection_contrib,
                '择时': timing_contrib,
                '仓位': position_contrib,
                '市场': market_contrib
            }
            min_factor = min(contribs, key=contribs.get)
            
            analysis += f"主要由于{min_factor}不当。"
        
        return analysis
    
    def _summarize_attribution(
        self,
        trade_attributions: List[TradeAttribution]
    ) -> Dict[str, float]:
        """汇总归因"""
        total_pnl = sum(t.pnl for t in trade_attributions)
        
        if total_pnl == 0:
            return {
                'selection': 0,
                'timing': 0,
                'position': 0,
                'market': 0
            }
        
        return {
            'selection': sum(t.selection_contrib for t in trade_attributions) / total_pnl,
            'timing': sum(t.timing_contrib for t in trade_attributions) / total_pnl,
            'position': sum(t.position_contrib for t in trade_attributions) / total_pnl,
            'market': sum(t.market_contrib for t in trade_attributions) / total_pnl
        }
    
    def _analyze_daily_attribution(
        self,
        trades: List[Dict],
        market_data: Optional[Dict]
    ) -> List[DailyAttribution]:
        """分析每日归因"""
        # 按日期分组
        daily_trades = {}
        for trade in trades:
            date = trade.get('exit_date', '')
            if date not in daily_trades:
                daily_trades[date] = []
            daily_trades[date].append(trade)
        
        daily_attributions = []
        
        for date, day_trades in sorted(daily_trades.items()):
            total_pnl = sum(t.get('pnl', 0) for t in day_trades)
            
            # 简化的每日归因
            daily_attribution = DailyAttribution(
                date=date,
                total_pnl=total_pnl,
                total_return=0,  # 需要计算
                selection_attribution={'value': 0.3, 'growth': 0.2},
                timing_attribution={'entry': 0.2, 'exit': 0.1},
                position_attribution={'size': 0.1, 'concentration': 0.05},
                market_attribution={'beta': 0.05},
                factor_exposure={'momentum': 0.3, 'value': 0.2, 'quality': 0.5}
            )
            
            daily_attributions.append(daily_attribution)
        
        return daily_attributions
    
    def _generate_recommendations(
        self,
        total_stats: Dict,
        attribution_summary: Dict,
        trade_attributions: List[TradeAttribution]
    ) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        # 基于胜率
        if total_stats['win_rate'] < 0.5:
            recommendations.append("胜率低于50%，建议优化选股策略，提高入场标准")
        
        # 基于归因
        if attribution_summary.get('selection', 0) < 0.3:
            recommendations.append("选股贡献较低，建议加强基本面和技术面分析")
        
        if attribution_summary.get('timing', 0) < 0.2:
            recommendations.append("择时贡献较低，建议优化入场和出场时机")
        
        # 基于单笔分析
        losing_trades = [t for t in trade_attributions if t.pnl < 0]
        if len(losing_trades) > len(trade_attributions) * 0.5:
            recommendations.append("亏损交易占比较高，建议加强止损管理")
        
        if not recommendations:
            recommendations.append("整体表现良好，继续保持当前策略")
        
        return recommendations
    
    def save_report(self, report: AttributionReport, output_path: Optional[str] = None):
        """保存归因报告"""
        if output_path is None:
            output_path = str(self.data_dir / "reports" / f"attribution_{report.report_date}.json")
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"归因报告已保存: {output_path}")
    
    def generate_summary_text(self, report: AttributionReport) -> str:
        """生成文字摘要"""
        lines = [
            "=" * 60,
            "交易归因分析报告",
            "=" * 60,
            f"报告日期: {report.report_date}",
            f"统计区间: {report.period_start} ~ {report.period_end}",
            "",
            "【总体表现】",
            f"总交易笔数: {report.total_trades}",
            f"盈利笔数: {report.winning_trades}",
            f"亏损笔数: {report.losing_trades}",
            f"胜率: {report.win_rate:.2%}",
            f"总盈亏: {report.total_pnl:,.2f}",
            f"总收益率: {report.total_return:.2%}",
            "",
            "【归因分析】",
            f"选股贡献: {report.attribution_summary.get('selection', 0):.1%}",
            f"择时贡献: {report.attribution_summary.get('timing', 0):.1%}",
            f"仓位贡献: {report.attribution_summary.get('position', 0):.1%}",
            f"市场贡献: {report.attribution_summary.get('market', 0):.1%}",
            "",
            "【改进建议】",
        ]
        
        for i, rec in enumerate(report.recommendations, 1):
            lines.append(f"{i}. {rec}")
        
        lines.append("=" * 60)
        
        return "\n".join(lines)


# 便捷函数
def create_attribution_analyzer() -> AttributionAnalyzer:
    """创建归因分析器"""
    return AttributionAnalyzer()


def analyze_trades(trades: List[Dict], market_data: Optional[Dict] = None) -> AttributionReport:
    """便捷函数：分析交易归因"""
    analyzer = create_attribution_analyzer()
    return analyzer.analyze_trades(trades, market_data)


if __name__ == "__main__":
    # 测试
    analyzer = AttributionAnalyzer()
    
    # 模拟交易数据
    trades = [
        {
            'id': 'T001',
            'code': '000001',
            'name': '平安银行',
            'entry_date': '2024-01-15',
            'exit_date': '2024-01-20',
            'entry_price': 10.0,
            'exit_price': 11.0,
            'quantity': 1000,
            'pnl': 1000,
            'pnl_pct': 0.10,
            'selection_score': 0.8,
            'position_ratio': 0.1
        },
        {
            'id': 'T002',
            'code': '000002',
            'name': '万科A',
            'entry_date': '2024-01-15',
            'exit_date': '2024-01-20',
            'entry_price': 15.0,
            'exit_price': 14.0,
            'quantity': 1000,
            'pnl': -1000,
            'pnl_pct': -0.067,
            'selection_score': 0.4,
            'position_ratio': 0.1
        }
    ]
    
    # 分析
    report = analyzer.analyze_trades(trades)
    
    # 打印摘要
    print(analyzer.generate_summary_text(report))
    
    # 保存报告
    analyzer.save_report(report)
