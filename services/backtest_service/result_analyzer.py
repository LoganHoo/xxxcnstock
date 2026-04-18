#!/usr/bin/env python3
"""
回测结果分析器
分析回测结果并生成报告
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class ResultAnalyzer:
    """
    回测结果分析器
    
    分析指标:
    - 总收益率
    - 年化收益率
    - Sharpe比率
    - 最大回撤
    - 胜率
    - 盈亏比
    """
    
    def analyze(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析回测结果
        
        Args:
            results: 回测结果字典
        
        Returns:
            分析报告
        """
        returns = results.get('returns', [])
        trades = results.get('trades', [])
        
        analysis = {
            'total_return': self._calc_total_return(returns),
            'annual_return': self._calc_annual_return(returns),
            'sharpe_ratio': self._calc_sharpe_ratio(returns),
            'max_drawdown': self._calc_max_drawdown(returns),
            'win_rate': self._calc_win_rate(trades),
            'profit_loss_ratio': self._calc_profit_loss_ratio(trades),
            'avg_trade_return': self._calc_avg_trade_return(trades),
            'max_consecutive_wins': self._calc_max_consecutive(trades, 'win'),
            'max_consecutive_losses': self._calc_max_consecutive(trades, 'loss')
        }
        
        return analysis
    
    def _calc_total_return(self, returns: List[float]) -> float:
        """计算总收益率"""
        if not returns:
            return 0.0
        return np.prod([1 + r for r in returns]) - 1
    
    def _calc_annual_return(self, returns: List[float], periods_per_year: int = 252) -> float:
        """计算年化收益率"""
        if not returns:
            return 0.0
        total_return = self._calc_total_return(returns)
        n_periods = len(returns)
        if n_periods == 0:
            return 0.0
        return (1 + total_return) ** (periods_per_year / n_periods) - 1
    
    def _calc_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.03) -> float:
        """计算Sharpe比率"""
        if not returns or len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        excess_returns = returns_array - risk_free_rate / 252
        
        std = excess_returns.std()
        if std == 0:
            return 0.0
        
        return excess_returns.mean() / std * np.sqrt(252)
    
    def _calc_max_drawdown(self, returns: List[float]) -> float:
        """计算最大回撤"""
        if not returns:
            return 0.0
        
        cumulative = np.cumprod([1 + r for r in returns])
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        
        return abs(drawdown.min())
    
    def _calc_win_rate(self, trades: List[Dict]) -> float:
        """计算胜率"""
        if not trades:
            return 0.0
        
        wins = sum(1 for t in trades if t.get('pnl', 0) > 0)
        return wins / len(trades)
    
    def _calc_profit_loss_ratio(self, trades: List[Dict]) -> float:
        """计算盈亏比"""
        if not trades:
            return 0.0
        
        profits = [t['pnl'] for t in trades if t.get('pnl', 0) > 0]
        losses = [abs(t['pnl']) for t in trades if t.get('pnl', 0) < 0]
        
        if not losses:
            return float('inf') if profits else 0.0
        
        avg_profit = np.mean(profits) if profits else 0
        avg_loss = np.mean(losses) if losses else 0
        
        return avg_profit / avg_loss if avg_loss > 0 else 0.0
    
    def _calc_avg_trade_return(self, trades: List[Dict]) -> float:
        """计算平均交易收益"""
        if not trades:
            return 0.0
        
        returns = [t.get('pnl_pct', 0) for t in trades]
        return np.mean(returns)
    
    def _calc_max_consecutive(self, trades: List[Dict], trade_type: str) -> int:
        """计算最大连续次数"""
        if not trades:
            return 0
        
        max_count = 0
        current_count = 0
        
        for trade in trades:
            is_type = (trade_type == 'win' and trade.get('pnl', 0) > 0) or \
                     (trade_type == 'loss' and trade.get('pnl', 0) < 0)
            
            if is_type:
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        
        return max_count
    
    def generate_report(self, backtest_results: Dict[str, Any]) -> str:
        """
        生成回测报告
        
        Args:
            backtest_results: 回测结果
        
        Returns:
            报告文本
        """
        analysis = self.analyze(backtest_results)
        
        report = f"""
=====================================
         回测报告
=====================================
收益指标:
  总收益率: {analysis['total_return']:.2%}
  年化收益率: {analysis['annual_return']:.2%}
  Sharpe比率: {analysis['sharpe_ratio']:.2f}

风险指标:
  最大回撤: {analysis['max_drawdown']:.2%}

交易统计:
  胜率: {analysis['win_rate']:.2%}
  盈亏比: {analysis['profit_loss_ratio']:.2f}
  平均交易收益: {analysis['avg_trade_return']:.2%}
  最大连续盈利: {analysis['max_consecutive_wins']}
  最大连续亏损: {analysis['max_consecutive_losses']}
=====================================
"""
        return report
