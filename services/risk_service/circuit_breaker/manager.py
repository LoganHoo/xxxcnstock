#!/usr/bin/env python3
"""
熔断机制管理器

实现:
- 大盘跌超2%暂停买入
- 大盘跌超5%清仓
- MACD死叉减仓50%
- 跌停板止损
"""
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitAction(Enum):
    """熔断操作"""
    NONE = "none"
    PAUSE_BUY = "pause_buy"
    REDUCE_50PCT = "reduce_50pct"
    CLOSE_ALL = "close_all"
    STOP_LOSS = "stop_loss"


@dataclass
class CircuitResult:
    """熔断结果"""
    triggered: bool
    rule: str
    action: CircuitAction
    reason: str
    duration: str


class CircuitBreaker:
    """熔断机制"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.market_drop_2pct = self.config.get('market_drop_2pct', -0.02)
        self.market_drop_5pct = self.config.get('market_drop_5pct', -0.05)
    
    def check_market(self, market_data: Dict[str, Any]) -> CircuitResult:
        """
        检查大盘熔断条件
        
        Args:
            market_data: {
                'index_change_pct': -0.025,  # 大盘涨跌幅
                'index_code': '000001.SH'
            }
        
        Returns:
            CircuitResult 熔断结果
        """
        change_pct = market_data.get('index_change_pct', 0)
        
        # 大盘跌超5%清仓
        if change_pct <= self.market_drop_5pct:
            return CircuitResult(
                triggered=True,
                rule='market_drop_5pct',
                action=CircuitAction.CLOSE_ALL,
                reason=f'Market dropped {change_pct:.1%}, close all positions',
                duration='1d'
            )
        
        # 大盘跌超2%暂停买入
        if change_pct <= self.market_drop_2pct:
            return CircuitResult(
                triggered=True,
                rule='market_drop_2pct',
                action=CircuitAction.PAUSE_BUY,
                reason=f'Market dropped {change_pct:.1%}, pause buying',
                duration='1d'
            )
        
        return CircuitResult(
            triggered=False,
            rule='none',
            action=CircuitAction.NONE,
            reason='No circuit breaker triggered',
            duration='0d'
        )
    
    def check_macd(self, macd_signal: str) -> CircuitResult:
        """
        检查MACD信号熔断
        
        Args:
            macd_signal: 'golden_cross' 或 'death_cross'
        
        Returns:
            CircuitResult 熔断结果
        """
        if macd_signal == 'death_cross':
            return CircuitResult(
                triggered=True,
                rule='macd_death_cross',
                action=CircuitAction.REDUCE_50PCT,
                reason='MACD death cross detected, reduce 50% position',
                duration='until_golden_cross'
            )
        
        return CircuitResult(
            triggered=False,
            rule='none',
            action=CircuitAction.NONE,
            reason='No MACD circuit breaker triggered',
            duration='0d'
        )
    
    def check_limit_down(
        self,
        current_price: float,
        limit_down_price: float
    ) -> CircuitResult:
        """
        检查跌停板熔断
        
        Args:
            current_price: 当前价格
            limit_down_price: 跌停价格
        
        Returns:
            CircuitResult 熔断结果
        """
        if current_price <= limit_down_price * 1.001:  # 允许微小误差
            return CircuitResult(
                triggered=True,
                rule='limit_down',
                action=CircuitAction.STOP_LOSS,
                reason=f'Price hit limit down ({limit_down_price})',
                duration='immediate'
            )
        
        return CircuitResult(
            triggered=False,
            rule='none',
            action=CircuitAction.NONE,
            reason='No limit down',
            duration='0d'
        )
    
    def check_all(
        self,
        market_data: Dict[str, Any],
        macd_signal: str,
        stock_data: Dict[str, Any]
    ) -> CircuitResult:
        """
        检查所有熔断条件
        
        优先级: 跌停 > 大盘跌5% > 大盘跌2% > MACD死叉
        """
        # 检查跌停
        result = self.check_limit_down(
            stock_data.get('current_price', 0),
            stock_data.get('limit_down_price', 0)
        )
        if result.triggered:
            return result
        
        # 检查大盘
        result = self.check_market(market_data)
        if result.triggered and result.action == CircuitAction.CLOSE_ALL:
            return result
        if result.triggered and result.action == CircuitAction.PAUSE_BUY:
            return result
        
        # 检查MACD
        result = self.check_macd(macd_signal)
        if result.triggered:
            return result
        
        return CircuitResult(
            triggered=False,
            rule='none',
            action=CircuitAction.NONE,
            reason='All checks passed',
            duration='0d'
        )
