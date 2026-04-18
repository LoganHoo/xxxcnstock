#!/usr/bin/env python3
"""
利弗莫尔仓位管理器

实现利弗莫尔金字塔加仓法:
- 20%底仓试水
- 每上涨10%加仓
- 最高价回落10%清仓
"""
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """持仓记录"""
    code: str
    entry_price: float
    quantity: int
    entry_date: datetime = field(default_factory=datetime.now)
    max_price: float = 0.0


@dataclass
class LivermoreConfig:
    """利弗莫尔配置"""
    initial_position_pct: float = 0.20  # 初始仓位比例
    add_position_pct: float = 0.10  # 加仓比例
    add_trigger_pct: float = 0.10  # 加仓触发涨幅
    stoploss_trigger_pct: float = 0.10  # 止损触发跌幅


class LivermoreManager:
    """利弗莫尔仓位管理器"""
    
    def __init__(self, config: Optional[LivermoreConfig] = None):
        self.config = config or LivermoreConfig()
        self.positions: Dict[str, Position] = {}
    
    def should_buy_initial(self, code: str, capital: float) -> Dict[str, Any]:
        """
        判断是否买入初始仓位
        
        Args:
            code: 股票代码
            capital: 总资金
        
        Returns:
            {'should_buy': bool, 'amount': float, 'reason': str}
        """
        if code in self.positions:
            return {'should_buy': False, 'amount': 0, 'reason': 'Already has position'}
        
        amount = capital * self.config.initial_position_pct
        
        return {
            'should_buy': True,
            'amount': amount,
            'reason': 'Initial position (20%)'
        }
    
    def should_add_position(self, code: str, current_price: float, capital: float) -> Dict[str, Any]:
        """
        判断是否加仓
        
        Args:
            code: 股票代码
            current_price: 当前价格
            capital: 总资金
        
        Returns:
            {'should_add': bool, 'amount': float, 'reason': str}
        """
        if code not in self.positions:
            return {'should_add': False, 'amount': 0, 'reason': 'No position'}
        
        position = self.positions[code]
        gain_pct = (current_price - position.entry_price) / position.entry_price
        
        # 检查是否达到加仓条件 (上涨10%)
        if gain_pct >= self.config.add_trigger_pct:
            amount = capital * self.config.add_position_pct
            return {
                'should_add': True,
                'amount': amount,
                'reason': f'Price up {gain_pct:.1%}, add position (10%)'
            }
        
        return {'should_add': False, 'amount': 0, 'reason': 'No add signal'}
    
    def should_stoploss(self, code: str, current_price: float) -> Dict[str, Any]:
        """
        判断是否止损
        
        Args:
            code: 股票代码
            current_price: 当前价格
        
        Returns:
            {'should_stop': bool, 'price': float, 'reason': str}
        """
        if code not in self.positions:
            return {'should_stop': False, 'price': 0, 'reason': 'No position'}
        
        position = self.positions[code]
        
        # 更新最高价
        if current_price > position.max_price:
            position.max_price = current_price
        
        # 检查是否从最高价回落10%
        if position.max_price > 0:
            drop_pct = (position.max_price - current_price) / position.max_price
            if drop_pct >= self.config.stoploss_trigger_pct:
                return {
                    'should_stop': True,
                    'price': current_price,
                    'reason': f'Price dropped {drop_pct:.1%} from high {position.max_price}'
                }
        
        return {'should_stop': False, 'price': 0, 'reason': 'No stop signal'}
    
    def add_position(self, code: str, entry_price: float, quantity: int):
        """添加持仓"""
        self.positions[code] = Position(
            code=code,
            entry_price=entry_price,
            quantity=quantity,
            max_price=entry_price
        )
    
    def remove_position(self, code: str):
        """移除持仓"""
        if code in self.positions:
            del self.positions[code]
    
    def get_position(self, code: str) -> Optional[Position]:
        """获取持仓信息"""
        return self.positions.get(code)
