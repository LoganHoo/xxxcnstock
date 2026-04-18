#!/usr/bin/env python3
"""
止盈止损管理器

实现:
- 20日均线下3%止损
- 盈利10%减仓一半
- 盈利20%清仓
"""
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """操作类型"""
    HOLD = "hold"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT_1 = "take_profit_1"
    TAKE_PROFIT_2 = "take_profit_2"


@dataclass
class StopLossResult:
    """止盈止损结果"""
    action: ActionType
    price: float
    reason: str
    position_pct: float  # 建议持仓比例


class StopLossManager:
    """止盈止损管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.stoploss_ma = self.config.get('stoploss_ma', 20)
        self.stoploss_pct = self.config.get('stoploss_pct', 0.03)
        self.take_profit_1_pct = self.config.get('take_profit_1_pct', 0.10)
        self.take_profit_2_pct = self.config.get('take_profit_2_pct', 0.20)
    
    def check(
        self,
        code: str,
        current_price: float,
        entry_price: float,
        ema_20: float
    ) -> StopLossResult:
        """
        检查止盈止损条件
        
        Args:
            code: 股票代码
            current_price: 当前价格
            entry_price: 买入价格
            ema_20: 20日均线
        
        Returns:
            StopLossResult 操作建议
        """
        # 计算盈亏比例
        gain_pct = (current_price - entry_price) / entry_price
        
        # 检查止盈条件 - 盈利20%清仓
        if gain_pct >= self.take_profit_2_pct:
            return StopLossResult(
                action=ActionType.TAKE_PROFIT_2,
                price=current_price,
                reason=f'Profit {gain_pct:.1%} >= 20%, close all',
                position_pct=0.0
            )
        
        # 检查止盈条件 - 盈利10%减仓一半
        if gain_pct >= self.take_profit_1_pct:
            return StopLossResult(
                action=ActionType.TAKE_PROFIT_1,
                price=current_price,
                reason=f'Profit {gain_pct:.1%} >= 10%, reduce half',
                position_pct=0.5
            )
        
        # 检查止损条件 - 20日均线下3%
        if ema_20 > 0:
            stoploss_price = ema_20 * (1 - self.stoploss_pct)
            if current_price <= stoploss_price:
                return StopLossResult(
                    action=ActionType.STOP_LOSS,
                    price=current_price,
                    reason=f'Price {current_price} <= EMA20*{1-self.stoploss_pct:.0%} ({stoploss_price:.2f})',
                    position_pct=0.0
                )
        
        # 继续持有
        return StopLossResult(
            action=ActionType.HOLD,
            price=current_price,
            reason='No action triggered',
            position_pct=1.0
        )
    
    def check_batch(
        self,
        positions: List[Dict[str, Any]]
    ) -> Dict[str, StopLossResult]:
        """
        批量检查持仓
        
        Args:
            positions: [{'code': '000001', 'current_price': 10.5, 'entry_price': 10.0, 'ema_20': 10.2}, ...]
        
        Returns:
            {code: StopLossResult}
        """
        results = {}
        for pos in positions:
            results[pos['code']] = self.check(
                code=pos['code'],
                current_price=pos['current_price'],
                entry_price=pos['entry_price'],
                ema_20=pos.get('ema_20', 0)
            )
        return results
