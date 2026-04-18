#!/usr/bin/env python3
"""
策略信号定义
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class BuySignal:
    """买入信号"""
    code: str
    name: Optional[str] = None
    trigger_price: float = 0.0
    stoploss_price: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    confidence: float = 0.0
    reason: str = ''
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
