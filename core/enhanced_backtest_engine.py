#!/usr/bin/env python3
"""
增强型回测引擎

支持基于新数据模块的回测:
- 财务数据回测: 基于财务指标选股的历史回测
- 市场行为回测: 基于龙虎榜/资金流向的历史回测
- 公告事件回测: 基于公告事件的历史回测
- 综合策略回测: 多维度数据综合选股回测

使用示例:
    engine = EnhancedBacktestEngine(strategy_config="config/strategies/champion.yaml")
    results = engine.run_financial_backtest(
        start_date="2024-01-01",
        end_date="2024-12-31",
        rebalance_freq="quarterly"  # 季度调仓
    )
"""
import polars as pl
import pandas as pd
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path

from core.backtest_engine import BacktestEngine
from core.logger import get_logger
from core.paths import get_data_path
from filters.filter_engine import FilterEngine
from filters.financial_filter import FinancialFilterBase
from filters.market_behavior_filter import MarketBehaviorFilterBase
from filters.announcement_filter import AnnouncementFilterBase
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.storage.financial_storage import FinancialStorageManager

logger = get_logger(__name__)


class RebalanceFreq(Enum):
    """调仓频率"""
    DAILY = "daily"           # 每日
    WEEKLY = "weekly"         # 每周
    MONTHLY = "monthly"       # 每月
    QUARTERLY = "quarterly"   # 每季度


@dataclass
class BacktestResult:
    """回测结果"""
    strategy_name: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_return: float           # 总收益率
    annualized_return: float      # 年化收益率
    max_drawdown: float           # 最大回撤
    sharpe_ratio: float           # 夏普比率
    trade_count: int              # 交易次数
    win_rate: float               # 胜率
    avg_holding_days: float       # 平均持仓天数
    trades: List[Dict] = field(default_factory=list)  # 交易记录
    daily_values: List[Dict] = field(default_factory=list)  # 每日净值
    monthly_returns: List[Dict] = field(default_factory=list)  # 月度收益


class EnhancedBacktestEngine:
    """增强型回测引擎"""
    
    def __init__(
        self,
        strategy_config: str = None,
        filter_engine: FilterEngine = None
    ):
        self.logger = logger
        self.data_service = UnifiedDataService()
        self.financial_storage = FinancialStorageManager()
        self.filter_engine = filter_engine or FilterEngine()
        
        # 回测结果保存目录
        self.results_dir = get_data_path() / "backtest_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run_financial_backtest(
        self,
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000,
        position_size: int = 10,
        rebalance_freq: RebalanceFreq = RebalanceFreq.QUARTERLY,
        filter_names: List[str] = None
    ) -> BacktestResult:
        """
        财务数据选股回测
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            initial_capital: 初始资金
            position_size: 持仓数量
            rebalance_freq: 调仓频率
            filter_names: 过滤器名称列表
        
        Returns:
            回测结果
        """
        self.logger.info(f"开始财务数据回测: {start_date} ~ {end_date}")
        
        # 生成调仓日期
        rebalance_dates = self._generate_rebalance_dates(
            start_date, end_date, rebalance_freq
        )
        
        # 获取所有股票代码
        all_codes = self.financial_storage.get_available_codes('balance_sheet')
        
        # 回测状态
        cash = initial_capital
        positions = {}  # {code: {shares: int, cost: float, buy_date: str}}
        trades = []
        daily_values = []
        
        for i, date in enumerate(rebalance_dates):
            self.logger.info(f"调仓日期: {date}, 当前持仓: {len(positions)} 只")
            
            # 获取当日K线数据用于计算收益
            # 这里简化处理，实际应该加载历史K线
            
            # 卖出当前持仓
            for code in list(positions.keys()):
                # 模拟卖出
                pos = positions[code]
                # 获取卖出价格(简化处理)
                sell_price = self._get_price_on_date(code, date)
                
                if sell_price:
                    sell_value = pos['shares'] * sell_price
                    cash += sell_value
                    
                    # 计算收益
                    buy_value = pos['shares'] * pos['cost']
                    profit = sell_value - buy_value
                    
                    trades.append({
                        'date': date,
                        'code': code,
                        'action': 'sell',
                        'price': sell_price,
                        'shares': pos['shares'],
                        'value': sell_value,
                        'profit': profit,
                        'return': profit / buy_value if buy_value > 0 else 0
                    })
            
            positions.clear()
            
            # 选股
            # 创建股票列表DataFrame
            stock_list = pl.DataFrame({
                'code': all_codes,
                'name': [''] * len(all_codes)
            })
            
            # 应用过滤器
            if filter_names:
                filtered_stocks = self.filter_engine.apply_filters(stock_list, filter_names)
            else:
                # 使用默认财务过滤器
                from filters.financial_filter import ROEFilter, FinancialCompositeFilter
                roe_filter = ROEFilter(params={'min_roe': 15.0})
                filtered_stocks = roe_filter.filter(stock_list)
            
            # 限制持仓数量
            selected_codes = filtered_stocks.head(position_size)['code'].to_list()
            
            if selected_codes and cash > 0:
                capital_per_stock = cash / len(selected_codes)
                
                for code in selected_codes:
                    buy_price = self._get_price_on_date(code, date)
                    
                    if buy_price:
                        # 买入100股整数倍
                        shares = int(capital_per_stock / buy_price / 100) * 100
                        
                        if shares > 0:
                            buy_value = shares * buy_price
                            cash -= buy_value
                            
                            positions[code] = {
                                'shares': shares,
                                'cost': buy_price,
                                'buy_date': date
                            }
                            
                            trades.append({
                                'date': date,
                                'code': code,
                                'action': 'buy',
                                'price': buy_price,
                                'shares': shares,
                                'value': buy_value
                            })
            
            # 记录每日净值
            portfolio_value = cash + sum(
                pos['shares'] * self._get_price_on_date(code, date) or pos['cost']
                for code, pos in positions.items()
            )
            
            daily_values.append({
                'date': date,
                'cash': cash,
                'positions_value': portfolio_value - cash,
                'total_value': portfolio_value
            })
        
        # 计算回测指标
        result = self._calculate_metrics(
            strategy_name="financial_backtest",
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            trades=trades,
            daily_values=daily_values
        )
        
        self.logger.info(f"财务数据回测完成: 总收益 {result.total_return:.2%}")
        return result
    
    def run_composite_backtest(
        self,
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000,
        position_size: int = 10,
        rebalance_freq: RebalanceFreq = RebalanceFreq.MONTHLY,
        financial_filters: List[str] = None,
        market_behavior_filters: List[str] = None,
        announcement_filters: List[str] = None
    ) -> BacktestResult:
        """
        综合策略回测
        
        结合财务、市场行为、公告三个维度的选股回测
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            position_size: 持仓数量
            rebalance_freq: 调仓频率
            financial_filters: 财务过滤器列表
            market_behavior_filters: 市场行为过滤器列表
            announcement_filters: 公告过滤器列表
        
        Returns:
            回测结果
        """
        self.logger.info(f"开始综合策略回测: {start_date} ~ {end_date}")
        
        # 生成调仓日期
        rebalance_dates = self._generate_rebalance_dates(
            start_date, end_date, rebalance_freq
        )
        
        # 获取股票列表
        all_codes = self.financial_storage.get_available_codes('balance_sheet')
        
        # 回测状态
        cash = initial_capital
        positions = {}
        trades = []
        daily_values = []
        
        for date in rebalance_dates:
            self.logger.info(f"调仓日期: {date}")
            
            # 卖出当前持仓
            for code in list(positions.keys()):
                pos = positions[code]
                sell_price = self._get_price_on_date(code, date)
                
                if sell_price:
                    sell_value = pos['shares'] * sell_price
                    cash += sell_value
                    
                    profit = sell_value - pos['shares'] * pos['cost']
                    trades.append({
                        'date': date,
                        'code': code,
                        'action': 'sell',
                        'price': sell_price,
                        'shares': pos['shares'],
                        'value': sell_value,
                        'profit': profit
                    })
            
            positions.clear()
            
            # 创建股票列表
            stock_list = pl.DataFrame({
                'code': all_codes,
                'name': [''] * len(all_codes)
            })
            
            # 应用财务过滤器
            if financial_filters:
                stock_list = self.filter_engine.apply_filters(stock_list, financial_filters)
            
            # 应用市场行为过滤器
            if market_behavior_filters:
                stock_list = self.filter_engine.apply_filters(stock_list, market_behavior_filters)
            
            # 应用公告过滤器
            if announcement_filters:
                stock_list = self.filter_engine.apply_filters(stock_list, announcement_filters)
            
            # 限制持仓数量
            selected_codes = stock_list.head(position_size)['code'].to_list()
            
            if selected_codes and cash > 0:
                capital_per_stock = cash / len(selected_codes)
                
                for code in selected_codes:
                    buy_price = self._get_price_on_date(code, date)
                    
                    if buy_price:
                        shares = int(capital_per_stock / buy_price / 100) * 100
                        
                        if shares > 0:
                            buy_value = shares * buy_price
                            cash -= buy_value
                            
                            positions[code] = {
                                'shares': shares,
                                'cost': buy_price,
                                'buy_date': date
                            }
                            
                            trades.append({
                                'date': date,
                                'code': code,
                                'action': 'buy',
                                'price': buy_price,
                                'shares': shares,
                                'value': buy_value
                            })
            
            # 记录净值
            portfolio_value = cash + sum(
                pos['shares'] * (self._get_price_on_date(code, date) or pos['cost'])
                for code, pos in positions.items()
            )
            
            daily_values.append({
                'date': date,
                'total_value': portfolio_value
            })
        
        # 计算回测指标
        result = self._calculate_metrics(
            strategy_name="composite_backtest",
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            trades=trades,
            daily_values=daily_values
        )
        
        self.logger.info(f"综合策略回测完成: 总收益 {result.total_return:.2%}")
        return result
    
    def _generate_rebalance_dates(
        self,
        start_date: str,
        end_date: str,
        freq: RebalanceFreq
    ) -> List[str]:
        """生成调仓日期列表"""
        dates = []
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            
            if freq == RebalanceFreq.DAILY:
                current += timedelta(days=1)
            elif freq == RebalanceFreq.WEEKLY:
                current += timedelta(weeks=1)
            elif freq == RebalanceFreq.MONTHLY:
                # 简单处理：每月同一天
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
            elif freq == RebalanceFreq.QUARTERLY:
                # 财报季：4月、8月、10月
                month = current.month
                if month < 4:
                    current = current.replace(month=4)
                elif month < 8:
                    current = current.replace(month=8)
                elif month < 10:
                    current = current.replace(month=10)
                else:
                    current = current.replace(year=current.year + 1, month=4)
        
        return dates
    
    def _get_price_on_date(self, code: str, date: str) -> Optional[float]:
        """获取股票在某日期的价格"""
        # 简化实现，实际应该从K线数据加载
        # 这里返回一个模拟价格
        return 10.0
    
    def _calculate_metrics(
        self,
        strategy_name: str,
        start_date: str,
        end_date: str,
        initial_capital: float,
        trades: List[Dict],
        daily_values: List[Dict]
    ) -> BacktestResult:
        """计算回测指标"""
        if not daily_values:
            return BacktestResult(
                strategy_name=strategy_name,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                final_capital=initial_capital,
                total_return=0.0,
                annualized_return=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                trade_count=0,
                win_rate=0.0,
                avg_holding_days=0.0
            )
        
        final_value = daily_values[-1]['total_value']
        total_return = (final_value - initial_capital) / initial_capital
        
        # 计算年化收益
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        years = (end - start).days / 365.25
        annualized_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        # 计算最大回撤
        max_drawdown = 0.0
        peak = initial_capital
        for dv in daily_values:
            value = dv['total_value']
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # 计算胜率
        sell_trades = [t for t in trades if t['action'] == 'sell']
        if sell_trades:
            win_trades = [t for t in sell_trades if t.get('profit', 0) > 0]
            win_rate = len(win_trades) / len(sell_trades)
        else:
            win_rate = 0.0
        
        # 计算平均持仓天数
        avg_holding_days = 0.0
        if sell_trades:
            holding_days = []
            for trade in sell_trades:
                # 简化计算
                holding_days.append(30)  # 假设平均持仓30天
            avg_holding_days = sum(holding_days) / len(holding_days)
        
        return BacktestResult(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_capital=final_value,
            total_return=total_return,
            annualized_return=annualized_return,
            max_drawdown=max_drawdown,
            sharpe_ratio=0.0,  # 简化处理
            trade_count=len(sell_trades),
            win_rate=win_rate,
            avg_holding_days=avg_holding_days,
            trades=trades,
            daily_values=daily_values
        )
    
    def save_result(self, result: BacktestResult, filename: str = None):
        """保存回测结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"backtest_{result.strategy_name}_{timestamp}.json"
        
        filepath = self.results_dir / filename
        
        # 转换为字典
        result_dict = {
            'strategy_name': result.strategy_name,
            'start_date': result.start_date,
            'end_date': result.end_date,
            'initial_capital': result.initial_capital,
            'final_capital': result.final_capital,
            'total_return': result.total_return,
            'annualized_return': result.annualized_return,
            'max_drawdown': result.max_drawdown,
            'sharpe_ratio': result.sharpe_ratio,
            'trade_count': result.trade_count,
            'win_rate': result.win_rate,
            'avg_holding_days': result.avg_holding_days,
            'trades_count': len(result.trades),
            'daily_values_count': len(result.daily_values)
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"回测结果已保存: {filepath}")
        return str(filepath)


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 增强型回测引擎")
    print("=" * 60)
    
    engine = EnhancedBacktestEngine()
    
    # 测试财务数据回测
    print("\n1. 测试财务数据回测:")
    result = engine.run_financial_backtest(
        start_date="2024-01-01",
        end_date="2024-03-31",
        initial_capital=1000000,
        position_size=5,
        rebalance_freq=RebalanceFreq.QUARTERLY
    )
    print(f"总收益: {result.total_return:.2%}")
    print(f"年化收益: {result.annualized_return:.2%}")
    print(f"最大回撤: {result.max_drawdown:.2%}")
    print(f"交易次数: {result.trade_count}")
    print(f"胜率: {result.win_rate:.2%}")
