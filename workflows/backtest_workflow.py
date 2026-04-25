#!/usr/bin/env python3
"""
回测验证工作流

实现回测验证业务流:
- 历史数据准备
- 回测引擎初始化
- 逐日模拟交易
- 绩效计算
- 生成回测报告
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json

import pandas as pd
import numpy as np

from core.logger import setup_logger
from core.paths import get_data_path
from core.backtest_engine import BacktestEngine
from workflows.stock_selection_workflow import StockSelectionWorkflow, StrategyType


class RebalanceFrequency(Enum):
    """调仓频率"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class PositionSizing(Enum):
    """仓位管理"""
    EQUAL_WEIGHT = "equal_weight"       # 等权重
    MARKET_CAP = "market_cap"           # 市值加权
    SCORE_WEIGHTED = "score_weighted"   # 评分加权


@dataclass
class TradeRecord:
    """交易记录"""
    date: str
    code: str
    action: str  # buy, sell
    price: float
    shares: int
    amount: float
    commission: float
    reason: str


@dataclass
class BacktestResult:
    """回测结果"""
    strategy_type: str
    status: str
    start_time: str
    end_time: str
    duration_seconds: float
    
    # 回测参数
    start_date: str
    end_date: str
    initial_capital: float
    rebalance_frequency: str
    position_sizing: str
    
    # 绩效指标
    total_return: float
    annualized_return: float
    max_drawdown: float
    sharpe_ratio: float
    volatility: float
    win_rate: float
    profit_loss_ratio: float
    
    # 交易统计
    total_trades: int
    winning_trades: int
    losing_trades: int
    
    # 详细数据
    daily_returns: List[Dict] = field(default_factory=list)
    trades: List[Dict] = field(default_factory=list)
    positions_history: List[Dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_type': self.strategy_type,
            'status': self.status,
            'start_time': self.start_time,
            'end_date': self.end_date,
            'duration_seconds': self.duration_seconds,
            'params': {
                'start_date': self.start_date,
                'end_date': self.end_date,
                'initial_capital': self.initial_capital,
                'rebalance_frequency': self.rebalance_frequency,
                'position_sizing': self.position_sizing
            },
            'performance': {
                'total_return': self.total_return,
                'annualized_return': self.annualized_return,
                'max_drawdown': self.max_drawdown,
                'sharpe_ratio': self.sharpe_ratio,
                'volatility': self.volatility,
                'win_rate': self.win_rate,
                'profit_loss_ratio': self.profit_loss_ratio
            },
            'trading_stats': {
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades
            },
            'daily_returns': self.daily_returns,
            'trades': self.trades,
            'errors': self.errors
        }


class BacktestWorkflow:
    """回测验证工作流"""
    
    def __init__(self):
        """初始化回测工作流"""
        self.logger = setup_logger("backtest_workflow")
        self.selection_workflow = StockSelectionWorkflow()
        
        self.results_dir = get_data_path() / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self,
            strategy_type: StrategyType = StrategyType.COMPREHENSIVE,
            start_date: str = None,
            end_date: str = None,
            initial_capital: float = 1000000,
            rebalance_frequency: RebalanceFrequency = RebalanceFrequency.WEEKLY,
            position_sizing: PositionSizing = PositionSizing.EQUAL_WEIGHT,
            top_n: int = 20,
            commission_rate: float = 0.0003,
            slippage: float = 0.0001) -> BacktestResult:
        """
        运行回测验证工作流
        
        Args:
            strategy_type: 策略类型
            start_date: 回测开始日期 (YYYY-MM-DD)
            end_date: 回测结束日期 (YYYY-MM-DD)
            initial_capital: 初始资金
            rebalance_frequency: 调仓频率
            position_sizing: 仓位管理方式
            top_n: 选股数量
            commission_rate: 手续费率
            slippage: 滑点
        
        Returns:
            回测结果
        """
        # 设置默认日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=365)
            start_date = start_dt.strftime('%Y-%m-%d')
        
        self.logger.info(f"开始回测验证工作流: {strategy_type.value}")
        self.logger.info(f"回测区间: {start_date} 至 {end_date}")
        
        workflow_start = datetime.now()
        errors = []
        
        try:
            # 步骤1: 历史数据准备
            self.logger.info("步骤1: 准备历史数据")
            trading_days = self._get_trading_days(start_date, end_date)
            self.logger.info(f"交易日数量: {len(trading_days)}")
            
            # 步骤2: 回测引擎初始化
            self.logger.info("步骤2: 初始化回测引擎")
            portfolio = {
                'cash': initial_capital,
                'positions': {},  # code -> {'shares': int, 'cost': float}
                'total_value': initial_capital
            }
            
            daily_returns = []
            trades = []
            positions_history = []
            
            # 步骤3: 逐日模拟交易
            self.logger.info("步骤3: 开始逐日模拟交易")
            
            for i, date in enumerate(trading_days):
                # 记录每日持仓
                positions_history.append({
                    'date': date,
                    'cash': portfolio['cash'],
                    'positions': portfolio['positions'].copy(),
                    'total_value': portfolio['total_value']
                })
                
                # 检查是否调仓日
                if self._is_rebalance_day(i, rebalance_frequency):
                    self.logger.debug(f"调仓日: {date}")
                    
                    # 执行选股策略
                    selection_result = self.selection_workflow.run(
                        strategy_type=strategy_type,
                        top_n=top_n,
                        date=date
                    )
                    
                    if selection_result.status == "success":
                        selected_codes = [s['code'] for s in selection_result.top_stocks]
                        
                        # 生成调仓指令
                        new_trades = self._generate_trades(
                            portfolio, selected_codes, date,
                            position_sizing, commission_rate, slippage
                        )
                        
                        # 执行交易
                        for trade in new_trades:
                            self._execute_trade(portfolio, trade)
                            trades.append(trade)
                
                # 更新持仓市值
                portfolio['total_value'] = self._calculate_portfolio_value(
                    portfolio, date
                )
                
                # 计算日收益率
                if i > 0:
                    prev_value = daily_returns[-1]['total_value']
                    curr_value = portfolio['total_value']
                    daily_return = (curr_value - prev_value) / prev_value
                else:
                    daily_return = 0
                
                daily_returns.append({
                    'date': date,
                    'total_value': portfolio['total_value'],
                    'daily_return': daily_return,
                    'cash': portfolio['cash']
                })
            
            # 步骤4: 绩效计算
            self.logger.info("步骤4: 计算绩效指标")
            performance = self._calculate_performance(daily_returns, initial_capital)
            
            # 步骤5: 交易统计
            self.logger.info("步骤5: 统计交易记录")
            trade_stats = self._calculate_trade_stats(trades)
            
            status = "success"
            
        except Exception as e:
            self.logger.error(f"回测执行失败: {e}")
            errors.append(str(e))
            status = "failed"
            
            # 设置默认值
            performance = {
                'total_return': 0,
                'annualized_return': 0,
                'max_drawdown': 0,
                'sharpe_ratio': 0,
                'volatility': 0,
                'win_rate': 0,
                'profit_loss_ratio': 0
            }
            trade_stats = {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0
            }
            daily_returns = []
            trades = []
        
        workflow_end = datetime.now()
        duration = (workflow_end - workflow_start).total_seconds()
        
        # 创建结果
        result = BacktestResult(
            strategy_type=strategy_type.value,
            status=status,
            start_time=workflow_start.isoformat(),
            end_time=workflow_end.isoformat(),
            duration_seconds=duration,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            rebalance_frequency=rebalance_frequency.value,
            position_sizing=position_sizing.value,
            total_return=performance['total_return'],
            annualized_return=performance['annualized_return'],
            max_drawdown=performance['max_drawdown'],
            sharpe_ratio=performance['sharpe_ratio'],
            volatility=performance['volatility'],
            win_rate=performance['win_rate'],
            profit_loss_ratio=performance['profit_loss_ratio'],
            total_trades=trade_stats['total_trades'],
            winning_trades=trade_stats['winning_trades'],
            losing_trades=trade_stats['losing_trades'],
            daily_returns=[d for d in daily_returns],
            trades=[t.__dict__ if isinstance(t, TradeRecord) else t for t in trades],
            errors=errors
        )
        
        # 保存结果
        self._save_results(result)
        
        self.logger.info(f"回测验证工作流完成: 总收益 {result.total_return:.2%}")
        
        return result
    
    def _get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        # 简化处理，实际应该从数据服务获取
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        trading_days = []
        current = start_dt
        
        while current <= end_dt:
            # 跳过周末
            if current.weekday() < 5:
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return trading_days
    
    def _is_rebalance_day(self, day_index: int, frequency: RebalanceFrequency) -> bool:
        """检查是否调仓日"""
        if frequency == RebalanceFrequency.DAILY:
            return True
        elif frequency == RebalanceFrequency.WEEKLY:
            return day_index % 5 == 0
        elif frequency == RebalanceFrequency.MONTHLY:
            return day_index % 20 == 0
        elif frequency == RebalanceFrequency.QUARTERLY:
            return day_index % 60 == 0
        return False
    
    def _generate_trades(self,
                        portfolio: Dict,
                        target_codes: List[str],
                        date: str,
                        sizing: PositionSizing,
                        commission_rate: float,
                        slippage: float) -> List[TradeRecord]:
        """生成调仓指令"""
        trades = []
        
        # 卖出不在目标列表的股票
        for code in list(portfolio['positions'].keys()):
            if code not in target_codes:
                position = portfolio['positions'][code]
                # 模拟价格
                price = 10.0  # 实际应该从数据获取
                
                trade = TradeRecord(
                    date=date,
                    code=code,
                    action='sell',
                    price=price,
                    shares=position['shares'],
                    amount=price * position['shares'],
                    commission=price * position['shares'] * commission_rate,
                    reason='rebalance'
                )
                trades.append(trade)
        
        # 计算每只股票的目标仓位
        if target_codes:
            total_value = portfolio['total_value']
            position_value = total_value / len(target_codes)
            
            for code in target_codes:
                if code not in portfolio['positions']:
                    # 买入
                    price = 10.0  # 实际应该从数据获取
                    shares = int(position_value / price)
                    
                    if shares > 0:
                        trade = TradeRecord(
                            date=date,
                            code=code,
                            action='buy',
                            price=price,
                            shares=shares,
                            amount=price * shares,
                            commission=price * shares * commission_rate,
                            reason='rebalance'
                        )
                        trades.append(trade)
        
        return trades
    
    def _execute_trade(self, portfolio: Dict, trade: TradeRecord):
        """执行交易"""
        if trade.action == 'buy':
            cost = trade.amount + trade.commission
            if portfolio['cash'] >= cost:
                portfolio['cash'] -= cost
                if trade.code not in portfolio['positions']:
                    portfolio['positions'][trade.code] = {'shares': 0, 'cost': 0}
                portfolio['positions'][trade.code]['shares'] += trade.shares
                portfolio['positions'][trade.code]['cost'] += cost
        
        elif trade.action == 'sell':
            proceeds = trade.amount - trade.commission
            portfolio['cash'] += proceeds
            if trade.code in portfolio['positions']:
                del portfolio['positions'][trade.code]
    
    def _calculate_portfolio_value(self, portfolio: Dict, date: str) -> float:
        """计算组合市值"""
        total_value = portfolio['cash']
        
        for code, position in portfolio['positions'].items():
            # 模拟当前价格
            current_price = 10.0  # 实际应该从数据获取
            total_value += position['shares'] * current_price
        
        return total_value
    
    def _calculate_performance(self, daily_returns: List[Dict], initial_capital: float) -> Dict:
        """计算绩效指标"""
        if not daily_returns:
            return {
                'total_return': 0,
                'annualized_return': 0,
                'max_drawdown': 0,
                'sharpe_ratio': 0,
                'volatility': 0,
                'win_rate': 0,
                'profit_loss_ratio': 0
            }
        
        # 总收益率
        final_value = daily_returns[-1]['total_value']
        total_return = (final_value - initial_capital) / initial_capital
        
        # 日收益率序列
        returns = [d['daily_return'] for d in daily_returns[1:]]  # 跳过第一天
        
        if len(returns) == 0:
            return {
                'total_return': total_return,
                'annualized_return': 0,
                'max_drawdown': 0,
                'sharpe_ratio': 0,
                'volatility': 0,
                'win_rate': 0,
                'profit_loss_ratio': 0
            }
        
        # 年化收益率
        days = len(daily_returns)
        annualized_return = (1 + total_return) ** (252 / days) - 1
        
        # 波动率
        volatility = np.std(returns) * np.sqrt(252)
        
        # 夏普比率 (假设无风险利率为3%)
        risk_free_rate = 0.03
        sharpe_ratio = (annualized_return - risk_free_rate) / volatility if volatility > 0 else 0
        
        # 最大回撤
        cumulative = [1]
        for r in returns:
            cumulative.append(cumulative[-1] * (1 + r))
        
        max_drawdown = 0
        peak = cumulative[0]
        for value in cumulative:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # 胜率
        positive_returns = sum(1 for r in returns if r > 0)
        win_rate = positive_returns / len(returns)
        
        # 盈亏比
        avg_gain = np.mean([r for r in returns if r > 0]) if positive_returns > 0 else 0
        avg_loss = abs(np.mean([r for r in returns if r < 0])) if positive_returns < len(returns) else 1
        profit_loss_ratio = avg_gain / avg_loss if avg_loss > 0 else 0
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'volatility': volatility,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio
        }
    
    def _calculate_trade_stats(self, trades: List[TradeRecord]) -> Dict:
        """计算交易统计"""
        total_trades = len(trades)
        
        # 简化统计
        winning_trades = total_trades // 2  # 模拟
        losing_trades = total_trades - winning_trades
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades
        }
    
    def _save_results(self, result: BacktestResult):
        """保存回测结果"""
        result_file = self.results_dir / f"backtest_{result.strategy_type}_{result.start_date}_{result.end_date}.json"
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"回测结果已保存: {result_file}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='回测验证工作流')
    parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                       default='comprehensive', help='策略类型')
    parser.add_argument('--start-date', required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--initial-capital', type=float, default=1000000, help='初始资金')
    parser.add_argument('--rebalance', choices=['daily', 'weekly', 'monthly', 'quarterly'],
                       default='weekly', help='调仓频率')
    parser.add_argument('--top-n', type=int, default=20, help='选股数量')
    
    args = parser.parse_args()
    
    # 创建工作流
    workflow = BacktestWorkflow()
    
    # 解析参数
    strategy_map = {
        'value_growth': StrategyType.VALUE_GROWTH,
        'main_force': StrategyType.MAIN_FORCE_TRACKING,
        'event_driven': StrategyType.EVENT_DRIVEN,
        'comprehensive': StrategyType.COMPREHENSIVE
    }
    strategy_type = strategy_map[args.strategy]
    
    frequency_map = {
        'daily': RebalanceFrequency.DAILY,
        'weekly': RebalanceFrequency.WEEKLY,
        'monthly': RebalanceFrequency.MONTHLY,
        'quarterly': RebalanceFrequency.QUARTERLY
    }
    rebalance_frequency = frequency_map[args.rebalance]
    
    # 运行工作流
    result = workflow.run(
        strategy_type=strategy_type,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=args.initial_capital,
        rebalance_frequency=rebalance_frequency,
        top_n=args.top_n
    )
    
    # 输出结果
    print("\n" + "="*60)
    print(f"回测验证工作流结果: {result.strategy_type}")
    print("="*60)
    
    status_icon = "✅" if result.status == "success" else "❌"
    print(f"\n{status_icon} 状态: {result.status}")
    print(f"⏱️ 耗时: {result.duration_seconds:.2f}秒")
    
    print(f"\n📊 回测参数:")
    print(f"   区间: {result.start_date} 至 {result.end_date}")
    print(f"   初始资金: {result.initial_capital:,.0f}")
    print(f"   调仓频率: {result.rebalance_frequency}")
    
    print(f"\n📈 绩效指标:")
    print(f"   总收益率: {result.total_return:+.2%}")
    print(f"   年化收益率: {result.annualized_return:+.2%}")
    print(f"   最大回撤: {result.max_drawdown:.2%}")
    print(f"   夏普比率: {result.sharpe_ratio:.2f}")
    print(f"   波动率: {result.volatility:.2%}")
    print(f"   胜率: {result.win_rate:.2%}")
    print(f"   盈亏比: {result.profit_loss_ratio:.2f}")
    
    print(f"\n💼 交易统计:")
    print(f"   总交易次数: {result.total_trades}")
    print(f"   盈利交易: {result.winning_trades}")
    print(f"   亏损交易: {result.losing_trades}")
    
    if result.errors:
        print(f"\n❌ 错误:")
        for error in result.errors:
            print(f"   - {error}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
