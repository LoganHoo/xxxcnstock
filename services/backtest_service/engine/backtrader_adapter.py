#!/usr/bin/env python3
"""
Backtrader适配器
将Backtrader集成到系统中
"""
import logging
from typing import Dict, Any, Optional, List
import backtrader as bt

logger = logging.getLogger(__name__)


class BacktraderAdapter:
    """
    Backtrader适配器
    
    封装Backtrader功能，提供简化的回测接口
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.cerebro = bt.Cerebro()
        self._setup_cerebro()
    
    def _setup_cerebro(self):
        """配置Cerebro"""
        # 设置初始资金
        initial_cash = self.config.get('initial_cash', 100000.0)
        self.cerebro.broker.setcash(initial_cash)
        
        # 设置手续费
        commission = self.config.get('commission', 0.00025)
        self.cerebro.broker.setcommission(commission=commission)
        
        # 设置滑点
        slip_perc = self.config.get('slippage', 0.001)
        self.cerebro.broker.set_slippage_perc(slip_perc)
        
        logger.info(f"Cerebro configured: initial_cash={initial_cash}, commission={commission}")
    
    def add_strategy(self, strategy_class, **kwargs):
        """添加策略"""
        self.cerebro.addstrategy(strategy_class, **kwargs)
        logger.info(f"Added strategy: {strategy_class.__name__}")
    
    def add_data(self, data, name: Optional[str] = None):
        """添加数据"""
        if name:
            data._name = name
        self.cerebro.adddata(data)
    
    def add_analyzers(self):
        """添加分析器"""
        # 添加夏普比率分析器
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        
        # 添加回撤分析器
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        
        # 添加交易分析器
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 添加收益分析器
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    
    def run(self) -> Dict[str, Any]:
        """
        运行回测
        
        Returns:
            回测结果
        """
        self.add_analyzers()
        
        logger.info("Starting backtest...")
        results = self.cerebro.run()
        
        if not results:
            return {'error': 'No results returned'}
        
        strat = results[0]
        
        # 获取回测结果
        final_value = self.cerebro.broker.getvalue()
        initial_value = self.config.get('initial_cash', 100000.0)
        
        # 获取分析器结果
        sharpe = strat.analyzers.sharpe.get_analysis()
        drawdown = strat.analyzers.drawdown.get_analysis()
        trades = strat.analyzers.trades.get_analysis()
        returns = strat.analyzers.returns.get_analysis()
        
        result = {
            'initial_value': initial_value,
            'final_value': final_value,
            'return_pct': (final_value - initial_value) / initial_value,
            'sharpe_ratio': sharpe.get('sharperatio', 0),
            'max_drawdown': drawdown.get('max', {}).get('drawdown', 0),
            'total_trades': trades.get('total', {}).get('total', 0),
            'won_trades': trades.get('won', {}).get('total', 0),
            'lost_trades': trades.get('lost', {}).get('total', 0),
            'annual_return': returns.get('rnorm100', 0)
        }
        
        logger.info(f"Backtest completed: return={result['return_pct']:.2%}, "
                   f"sharpe={result['sharpe_ratio']:.2f}")
        
        return result
    
    def plot(self, filename: Optional[str] = None):
        """绘制回测结果"""
        try:
            if filename:
                self.cerebro.plot(style='candlestick', savefig=True, 
                                 figfilename=filename)
            else:
                self.cerebro.plot(style='candlestick')
        except Exception as e:
            logger.warning(f"Failed to plot: {e}")
