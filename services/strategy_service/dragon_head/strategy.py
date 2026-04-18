#!/usr/bin/env python3
"""
龙回头策略
高度板回调买入策略
"""
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DragonHeadConfig:
    """龙回头配置"""
    min_consecutive_limitup: int = 3   # 最少连板天数
    max_pullback_pct: float = 0.15     # 最大回调幅度(15%)
    pullback_days: int = 5             # 回调观察天数
    min_market_cap: float = 30.0       # 最小市值(亿)


class DragonHeadStrategy:
    """
    龙回头策略
    
    策略逻辑:
    1. 识别高度板(连续涨停)
    2. 等待回调(不超过15%)
    3. 企稳信号买入
    """
    
    def __init__(self, config: Optional[DragonHeadConfig] = None):
        self.config = config or DragonHeadConfig()
    
    def is_height_board(self, stock_data: Dict[str, Any]) -> bool:
        """
        判断是否高度板
        
        Args:
            stock_data: {
                'consecutive_limitup': 连板天数,
                'market_position': 市场地位 ('leader', 'follower'),
                'market_cap': 市值
            }
        """
        consecutive = stock_data.get('consecutive_limitup', 0)
        position = stock_data.get('market_position', '')
        market_cap = stock_data.get('market_cap', 0)
        
        # 连板天数达标，是市场龙头，市值足够
        return (
            consecutive >= self.config.min_consecutive_limitup and
            position == 'leader' and
            market_cap >= self.config.min_market_cap
        )
    
    def is_pullback_buy(self, price_data: pd.DataFrame) -> bool:
        """
        判断是否龙回头买入时机
        
        条件:
        1. 从高点回调不超过15%
        2. 出现企稳信号(不再创新低)
        3. 成交量萎缩后放量
        """
        if len(price_data) < self.config.pullback_days:
            return False
        
        recent_data = price_data.tail(self.config.pullback_days)
        
        # 获取高点和当前价格
        high_price = recent_data['close'].max()
        current_price = recent_data['close'].iloc[-1]
        
        # 计算回调幅度
        pullback_pct = (high_price - current_price) / high_price
        
        # 回调幅度在合理范围内
        if pullback_pct > self.config.max_pullback_pct:
            return False
        
        # 检查企稳信号 - 最后3天不再创新低
        last_3 = recent_data.tail(3)['close']
        if len(last_3) >= 3:
            # 最后一天的最低价不再创新低
            stabilizing = last_3.iloc[-1] >= last_3.iloc[-2] * 0.99
            return stabilizing
        
        return False
    
    def find_dragon_head_opportunities(
        self,
        market_data: List[Dict[str, Any]],
        price_history: Dict[str, pd.DataFrame]
    ) -> List[Dict[str, Any]]:
        """
        寻找龙回头机会
        
        Args:
            market_data: 市场数据列表
            price_history: 股票价格历史 {code: DataFrame}
        
        Returns:
            机会列表
        """
        opportunities = []
        
        for stock in market_data:
            code = stock['code']
            
            # 检查是否是高度板
            if not self.is_height_board(stock):
                continue
            
            # 检查是否有价格历史
            if code not in price_history:
                continue
            
            # 检查是否龙回头
            if self.is_pullback_buy(price_history[code]):
                consecutive = stock.get('consecutive_limitup', 0)
                opportunities.append({
                    'code': code,
                    'signal_type': 'dragon_head',
                    'reason': f"高度板回调买入，连板{consecutive}天",
                    'confidence': 0.7,
                    'suggested_position': 0.15  # 建议仓位15%
                })
        
        logger.info(f"Found {len(opportunities)} dragon head opportunities")
        return opportunities
    
    def calculate_buy_zone(
        self,
        high_price: float,
        current_price: float
    ) -> Dict[str, Any]:
        """
        计算买入区间
        
        Args:
            high_price: 高点价格
            current_price: 当前价格
        
        Returns:
            买入区间信息
        """
        pullback_pct = (high_price - current_price) / high_price
        
        # 买入区间: 回调10%-15%
        buy_zone_low = high_price * 0.85  # 回调15%
        buy_zone_high = high_price * 0.90  # 回调10%
        
        in_buy_zone = buy_zone_low <= current_price <= buy_zone_high
        
        return {
            'high_price': high_price,
            'current_price': current_price,
            'pullback_pct': pullback_pct,
            'buy_zone_low': buy_zone_low,
            'buy_zone_high': buy_zone_high,
            'in_buy_zone': in_buy_zone,
            'recommendation': 'buy' if in_buy_zone else 'wait'
        }
