#!/usr/bin/env python3
"""
增强版风险管理系统

功能:
- 多维度风险评估
- 动态仓位管理
- 组合风险分析
- 压力测试
- 风险预算分配
"""
import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """风险等级"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskMetrics:
    """风险指标"""
    var_95: float = 0.0  # 95% VaR
    var_99: float = 0.0  # 99% VaR
    cvar_95: float = 0.0  # 条件VaR
    volatility: float = 0.0  # 波动率
    beta: float = 0.0  # Beta系数
    max_drawdown: float = 0.0  # 最大回撤
    sharpe_ratio: float = 0.0  # 夏普比率
    sortino_ratio: float = 0.0  # 索提诺比率
    calmar_ratio: float = 0.0  # 卡尔玛比率


@dataclass
class PositionRisk:
    """持仓风险"""
    code: str
    weight: float  # 权重
    contribution: float  # 风险贡献
    var_contribution: float  # VaR贡献
    beta: float  # 个股Beta
    correlation: float  # 与组合相关性


class EnhancedRiskManager:
    """增强版风险管理器"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
        # 风险限额
        self.max_position_pct = config.get('max_position_pct', 0.20)
        self.max_sector_pct = config.get('max_sector_pct', 0.30)
        self.max_drawdown_limit = config.get('max_drawdown_limit', 0.15)
        self.var_limit = config.get('var_limit', 0.03)
        
        # 风险预算
        self.risk_budget = config.get('risk_budget', 0.10)
        
        # 历史数据
        self.returns_history: List[float] = []
        self.positions_history: List[Dict] = []
        
    def calculate_var(self, returns: np.ndarray, confidence: float = 0.95) -> float:
        """计算VaR (Value at Risk)"""
        if len(returns) == 0:
            return 0.0
        return np.percentile(returns, (1 - confidence) * 100)
    
    def calculate_cvar(self, returns: np.ndarray, confidence: float = 0.95) -> float:
        """计算CVaR (Conditional VaR)"""
        var = self.calculate_var(returns, confidence)
        return np.mean(returns[returns <= var]) if len(returns) > 0 else 0.0
    
    def calculate_drawdown(self, equity_curve: np.ndarray) -> Tuple[float, int]:
        """计算最大回撤"""
        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        max_dd = np.min(drawdown)
        max_dd_idx = np.argmin(drawdown)
        return max_dd, max_dd_idx
    
    def calculate_beta(self, stock_returns: np.ndarray, market_returns: np.ndarray) -> float:
        """计算Beta系数"""
        if len(stock_returns) == 0 or len(market_returns) == 0:
            return 0.0
        
        covariance = np.cov(stock_returns, market_returns)[0, 1]
        market_variance = np.var(market_returns)
        
        return covariance / market_variance if market_variance > 0 else 0.0
    
    def calculate_sharpe_ratio(self, returns: np.ndarray, risk_free_rate: float = 0.03) -> float:
        """计算夏普比率"""
        if len(returns) == 0 or np.std(returns) == 0:
            return 0.0
        excess_returns = np.mean(returns) - risk_free_rate / 252
        return excess_returns / np.std(returns) * np.sqrt(252)
    
    def calculate_sortino_ratio(self, returns: np.ndarray, risk_free_rate: float = 0.03) -> float:
        """计算索提诺比率"""
        if len(returns) == 0:
            return 0.0
        
        downside_returns = returns[returns < 0]
        downside_std = np.std(downside_returns) if len(downside_returns) > 0 else 0.0001
        
        excess_returns = np.mean(returns) - risk_free_rate / 252
        return excess_returns / downside_std * np.sqrt(252)
    
    def calculate_correlation_matrix(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算相关性矩阵"""
        return returns_df.corr()
    
    def calculate_portfolio_risk(self, weights: np.ndarray, cov_matrix: np.ndarray) -> float:
        """计算组合风险"""
        portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        return np.sqrt(portfolio_variance)
    
    def assess_risk_level(self, metrics: RiskMetrics) -> RiskLevel:
        """评估风险等级"""
        score = 0
        
        # 根据各项指标评分
        if metrics.var_95 < -0.02:
            score += 2
        if metrics.max_drawdown < -0.10:
            score += 2
        if metrics.volatility > 0.30:
            score += 1
        if metrics.sharpe_ratio < 0.5:
            score += 1
        
        if score >= 5:
            return RiskLevel.CRITICAL
        elif score >= 3:
            return RiskLevel.HIGH
        elif score >= 1:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    def check_position_limits(self, positions: Dict[str, Dict]) -> List[str]:
        """检查持仓限制"""
        violations = []
        
        total_value = sum(p.get('value', 0) for p in positions.values())
        if total_value == 0:
            return violations
        
        for code, pos in positions.items():
            weight = pos.get('value', 0) / total_value
            
            # 检查单票限制
            if weight > self.max_position_pct:
                violations.append(f"{code} 权重 {weight:.2%} 超过限制 {self.max_position_pct:.2%}")
        
        return violations
    
    def calculate_risk_contribution(self, weights: np.ndarray, cov_matrix: np.ndarray) -> np.ndarray:
        """计算风险贡献"""
        portfolio_vol = self.calculate_portfolio_risk(weights, cov_matrix)
        
        if portfolio_vol == 0:
            return np.zeros_like(weights)
        
        marginal_risk = np.dot(cov_matrix, weights) / portfolio_vol
        risk_contrib = weights * marginal_risk
        
        return risk_contrib
    
    def optimize_risk_budget(self, cov_matrix: np.ndarray, target_budget: np.ndarray) -> np.ndarray:
        """风险预算优化"""
        n = len(target_budget)
        
        # 初始等权重
        weights = np.ones(n) / n
        
        # 迭代优化
        for _ in range(100):
            risk_contrib = self.calculate_risk_contribution(weights, cov_matrix)
            total_risk = np.sum(risk_contrib)
            
            if total_risk == 0:
                break
            
            # 调整权重
            risk_budget_ratio = risk_contrib / total_risk
            adjustment = target_budget / (risk_budget_ratio + 1e-8)
            weights = weights * np.sqrt(adjustment)
            weights = weights / np.sum(weights)
        
        return weights
    
    def stress_test(self, positions: Dict, scenarios: Dict[str, Dict]) -> Dict[str, Any]:
        """压力测试"""
        results = {}
        
        total_value = sum(p.get('value', 0) for p in positions.values())
        
        for scenario_name, scenario in scenarios.items():
            # 应用情景冲击
            shocked_value = 0
            for code, pos in positions.items():
                shock = scenario.get('market_shock', 0)
                # 根据Beta调整冲击
                beta = pos.get('beta', 1.0)
                adjusted_shock = shock * beta
                shocked_value += pos.get('value', 0) * (1 + adjusted_shock)
            
            loss = total_value - shocked_value
            loss_pct = loss / total_value if total_value > 0 else 0
            
            results[scenario_name] = {
                'original_value': total_value,
                'shocked_value': shocked_value,
                'loss': loss,
                'loss_pct': loss_pct,
                'pass': loss_pct < self.max_drawdown_limit
            }
        
        return results
    
    def generate_risk_report(self, portfolio: Dict, market_data: Dict = None) -> Dict[str, Any]:
        """生成风险报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'limits': {},
            'recommendations': []
        }
        
        positions = portfolio.get('positions', {})
        
        # 检查限额
        violations = self.check_position_limits(positions)
        report['limits']['violations'] = violations
        report['limits']['violation_count'] = len(violations)
        
        # 计算风险指标
        if self.returns_history:
            returns = np.array(self.returns_history)
            metrics = RiskMetrics(
                var_95=self.calculate_var(returns, 0.95),
                var_99=self.calculate_var(returns, 0.99),
                cvar_95=self.calculate_cvar(returns, 0.95),
                volatility=np.std(returns) * np.sqrt(252),
                sharpe_ratio=self.calculate_sharpe_ratio(returns),
                sortino_ratio=self.calculate_sortino_ratio(returns)
            )
            
            report['summary'] = {
                'var_95': f"{metrics.var_95:.2%}",
                'var_99': f"{metrics.var_99:.2%}",
                'volatility': f"{metrics.volatility:.2%}",
                'sharpe_ratio': f"{metrics.sharpe_ratio:.2f}",
                'sortino_ratio': f"{metrics.sortino_ratio:.2f}"
            }
            
            # 风险等级
            risk_level = self.assess_risk_level(metrics)
            report['summary']['risk_level'] = risk_level.value
            
            # 建议
            if risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                report['recommendations'].append("建议降低仓位")
            if metrics.sharpe_ratio < 0.5:
                report['recommendations'].append("夏普比率偏低，建议优化策略")
            if len(violations) > 0:
                report['recommendations'].append("存在持仓超限，建议调整")
        
        return report
    
    def update_history(self, daily_return: float, positions: Dict):
        """更新历史数据"""
        self.returns_history.append(daily_return)
        self.positions_history.append(positions)
        
        # 保持最近252个交易日（一年）
        if len(self.returns_history) > 252:
            self.returns_history = self.returns_history[-252:]
            self.positions_history = self.positions_history[-252:]


class DynamicPositionSizer:
    """动态仓位管理器"""
    
    def __init__(self, risk_manager: EnhancedRiskManager):
        self.risk_manager = risk_manager
        self.current_drawdown = 0.0
        self.equity_peak = 0.0
        
    def update_equity(self, current_equity: float):
        """更新权益"""
        if current_equity > self.equity_peak:
            self.equity_peak = current_equity
        
        self.current_drawdown = (self.equity_peak - current_equity) / self.equity_peak
    
    def calculate_position_size(self, signal_strength: float, 
                               volatility: float,
                               max_position: float = 0.20) -> float:
        """计算仓位大小"""
        # 基础仓位
        base_size = signal_strength * max_position
        
        # 根据回撤调整
        if self.current_drawdown > 0.05:
            drawdown_factor = 1 - (self.current_drawdown / 0.20)
            base_size *= max(0.2, drawdown_factor)
        
        # 根据波动率调整
        if volatility > 0.30:
            volatility_factor = 0.30 / volatility
            base_size *= volatility_factor
        
        return min(base_size, max_position)
    
    def calculate_kelly_position(self, win_rate: float, 
                                avg_win: float, 
                                avg_loss: float,
                                max_position: float = 0.20) -> float:
        """使用凯利公式计算仓位"""
        if avg_loss == 0:
            return 0
        
        win_loss_ratio = avg_win / avg_loss
        kelly_pct = win_rate - ((1 - win_rate) / win_loss_ratio)
        
        # 使用半凯利
        half_kelly = kelly_pct / 2
        
        return min(max(half_kelly, 0), max_position)


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    
    # 创建风险管理器
    risk_manager = EnhancedRiskManager({
        'max_position_pct': 0.20,
        'max_drawdown_limit': 0.15
    })
    
    # 模拟历史收益
    np.random.seed(42)
    for _ in range(100):
        daily_return = np.random.normal(0.001, 0.02)
        risk_manager.update_history(daily_return, {})
    
    # 生成风险报告
    portfolio = {
        'positions': {
            '000001': {'value': 200000},
            '000002': {'value': 150000},
            '000063': {'value': 100000}
        },
        'total_value': 450000
    }
    
    report = risk_manager.generate_risk_report(portfolio)
    print("\n风险报告:")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    
    # 压力测试
    scenarios = {
        'market_crash': {'market_shock': -0.20},
        'correction': {'market_shock': -0.10},
        'mild_decline': {'market_shock': -0.05}
    }
    
    stress_results = risk_manager.stress_test(portfolio['positions'], scenarios)
    print("\n压力测试结果:")
    for scenario, result in stress_results.items():
        print(f"  {scenario}: 损失 {result['loss_pct']:.2%} - {'通过' if result['pass'] else '不通过'}")
