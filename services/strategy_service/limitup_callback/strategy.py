#!/usr/bin/env python3
"""
涨停回调战法策略

核心逻辑:
1. 筛除三连板以上、换手率>20%、业绩亏损股
2. 确认月线MACD金叉且股价>60月线
3. 回调至20日均线且放量阳线时买入
"""
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd

from .signals import BuySignal

logger = logging.getLogger(__name__)


@dataclass
class StrategyConfig:
    """策略配置"""
    max_limitup_days: int = 3
    max_turnover: float = 20.0
    min_roe: float = 0.0
    ema_tolerance: float = 0.02
    volume_surge_ratio: float = 1.5


class LimitupCallbackStrategy:
    """涨停回调战法策略"""
    
    def __init__(self, config: Optional[StrategyConfig] = None):
        self.config = config or StrategyConfig()
    
    def execute(self, stock_data: pd.DataFrame) -> List[BuySignal]:
        """
        执行策略
        
        Args:
            stock_data: 股票数据DataFrame
        
        Returns:
            买入信号列表
        """
        # Step 1: 初步筛选
        filtered = self.step1_filter(stock_data)
        logger.info(f"Step 1 filtered: {len(filtered)} stocks")
        
        if filtered.empty:
            return []
        
        # Step 2: 趋势确认
        confirmed = self.step2_confirm(filtered)
        logger.info(f"Step 2 confirmed: {len(confirmed)} stocks")
        
        if confirmed.empty:
            return []
        
        # Step 3: 买入时机
        signals = self.step3_timing(confirmed.to_dict('records'))
        logger.info(f"Step 3 signals: {len(signals)} stocks")
        
        return signals
    
    def step1_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        初步筛选
        
        筛除:
        - 三连板以上
        - 换手率>20%
        - 业绩亏损股 (ROE <= 0)
        """
        filtered = data.copy()
        
        # 筛除三连板以上
        if 'limitup_days' in filtered.columns:
            filtered = filtered[filtered['limitup_days'] <= self.config.max_limitup_days]
        
        # 筛除高换手率
        if 'turnover' in filtered.columns:
            filtered = filtered[filtered['turnover'] <= self.config.max_turnover]
        
        # 筛除亏损股
        if 'roe' in filtered.columns:
            filtered = filtered[filtered['roe'] > self.config.min_roe]
        
        return filtered
    
    def step2_confirm(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        趋势确认
        
        确认:
        - 月线MACD金叉
        - 股价>60月线
        """
        confirmed = data.copy()
        
        # 确认月线MACD金叉
        if 'macd_monthly' in confirmed.columns:
            confirmed = confirmed[confirmed['macd_monthly'] == 'golden_cross']
        
        # 确认股价>60月线
        if 'close' in confirmed.columns and 'ema_60_monthly' in confirmed.columns:
            confirmed = confirmed[confirmed['close'] > confirmed['ema_60_monthly']]
        
        return confirmed
    
    def step3_timing(self, stocks: List[Dict]) -> List[BuySignal]:
        """
        买入时机判断
        
        条件:
        - 回调至20日均线附近 (±2%)
        - 放量阳线 (成交量>20日均量*1.5)
        """
        signals = []
        
        for stock in stocks:
            close = stock.get('close', 0)
            open_price = stock.get('open', 0)
            ema_20 = stock.get('ema_20', 0)
            volume = stock.get('volume', 0)
            volume_20_avg = stock.get('volume_20_avg', 0)
            
            # 检查是否在20日均线附近
            if ema_20 <= 0:
                continue
            
            ema_upper = ema_20 * (1 + self.config.ema_tolerance)
            ema_lower = ema_20 * (1 - self.config.ema_tolerance)
            
            near_ema = ema_lower <= close <= ema_upper
            
            # 检查是否放量
            volume_surge = volume > volume_20_avg * self.config.volume_surge_ratio
            
            # 检查是否阳线
            is_red = close > open_price
            
            if near_ema and volume_surge and is_red:
                signal = BuySignal(
                    code=stock.get('code'),
                    name=stock.get('name'),
                    trigger_price=close,
                    stoploss_price=self.calculate_stoploss(stock),
                    take_profit_1=close * 1.10,
                    take_profit_2=close * 1.20,
                    confidence=0.8,
                    reason='涨停回调至20日均线，放量阳线'
                )
                signals.append(signal)
        
        return signals
    
    def calculate_stoploss(self, stock: Dict) -> float:
        """计算止损价 (20日均线下3%)"""
        ema_20 = stock.get('ema_20', 0)
        return ema_20 * 0.97 if ema_20 > 0 else 0
    
    def calculate_take_profit(self, entry_price: float) -> List[Dict]:
        """计算止盈价"""
        return [
            {'level': 1, 'price': entry_price * 1.10, 'action': 'reduce_half'},
            {'level': 2, 'price': entry_price * 1.20, 'action': 'close_all'}
        ]
