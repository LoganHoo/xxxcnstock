#!/usr/bin/env python3
"""
策略包装器
将我们的策略适配到Backtrader框架
"""
import backtrader as bt
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class StrategyWrapper(bt.Strategy):
    """
    策略包装器
    
    将我们的策略适配到Backtrader框架
    """
    
    params = (
        ('strategy_config', None),
        ('position_size', 0.1),  # 默认仓位10%
        ('stop_loss', 0.05),     # 默认止损5%
        ('take_profit', 0.10),   # 默认止盈10%
    )
    
    def __init__(self):
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.datavolume = self.datas[0].volume
        
        # 订单状态
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        
        # 交易记录
        self.trades = []
    
    def log(self, txt, dt=None):
        """日志记录"""
        dt = dt or self.datas[0].datetime.date(0)
        logger.debug(f'{dt.isoformat()} {txt}')
    
    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, '
                        f'Cost: {order.executed.value:.2f}, '
                        f'Comm: {order.executed.comm:.2f}')
                self.buy_price = order.executed.price
                self.buy_comm = order.executed.comm
            else:
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, '
                        f'Cost: {order.executed.value:.2f}, '
                        f'Comm: {order.executed.comm:.2f}')
                
                # 记录交易
                if self.buy_price:
                    pnl = order.executed.price - self.buy_price
                    pnl_pct = pnl / self.buy_price
                    self.trades.append({
                        'buy_price': self.buy_price,
                        'sell_price': order.executed.price,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct
                    })
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
        
        self.order = None
    
    def notify_trade(self, trade):
        """交易通知"""
        if not trade.isclosed:
            return
        
        self.log(f'OPERATION PROFIT, GROSS: {trade.pnl:.2f}, NET: {trade.pnlcomm:.2f}')
    
    def next(self):
        """下一根K线"""
        # 如果有未完成的订单，不操作
        if self.order:
            return
        
        # 检查是否持仓
        if not self.position:
            # 买入逻辑 - 子类可以重写
            if self.should_buy():
                self.order = self.buy(size=self.calculate_position_size())
        else:
            # 卖出逻辑 - 子类可以重写
            if self.should_sell():
                self.order = self.sell(size=self.position.size)
            # 止损检查
            elif self.check_stop_loss():
                self.order = self.sell(size=self.position.size)
            # 止盈检查
            elif self.check_take_profit():
                self.order = self.sell(size=self.position.size)
    
    def should_buy(self) -> bool:
        """
        是否应该买入
        
        子类可以重写此方法实现自定义买入逻辑
        """
        return False
    
    def should_sell(self) -> bool:
        """
        是否应该卖出
        
        子类可以重写此方法实现自定义卖出逻辑
        """
        return False
    
    def check_stop_loss(self) -> bool:
        """检查是否触发止损"""
        if not self.buy_price:
            return False
        
        current_price = self.dataclose[0]
        loss_pct = (self.buy_price - current_price) / self.buy_price
        
        return loss_pct >= self.params.stop_loss
    
    def check_take_profit(self) -> bool:
        """检查是否触发止盈"""
        if not self.buy_price:
            return False
        
        current_price = self.dataclose[0]
        profit_pct = (current_price - self.buy_price) / self.buy_price
        
        return profit_pct >= self.params.take_profit
    
    def calculate_position_size(self) -> int:
        """计算仓位大小"""
        cash = self.broker.getcash()
        position_value = cash * self.params.position_size
        size = int(position_value / self.dataclose[0])
        return max(0, size)
    
    def stop(self):
        """回测结束"""
        final_value = self.broker.getvalue()
        initial_value = self.broker.startingcash
        return_pct = (final_value - initial_value) / initial_value
        
        self.log(f'FINAL PORTFOLIO VALUE: {final_value:.2f}, '
                f'RETURN: {return_pct:.2%}')
