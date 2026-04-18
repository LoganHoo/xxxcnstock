#!/usr/bin/env python3
"""
尾盘选股策略
14:30后基于技术指标选股
"""
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class EndstockConfig:
    """尾盘选股配置"""
    price_change_min: float = 3.0      # 最小涨幅(%)
    price_change_max: float = 5.0      # 最大涨幅(%)
    volume_ratio_min: float = 1.0      # 最小量比
    volume_ratio_max: float = 5.0      # 最大量比
    market_cap_min: float = 50.0       # 最小市值(亿)
    market_cap_max: float = 200.0      # 最大市值(亿)
    above_ma: bool = True              # 是否要求在均线上方


class EndstockPickStrategy:
    """
    尾盘选股策略
    
    选股条件:
    1. 涨幅 3%-5%
    2. 量比 1-5
    3. 市值 50-200亿
    4. 股价在分时均线之上
    """
    
    def __init__(self, config: Optional[EndstockConfig] = None):
        self.config = config or EndstockConfig()
    
    def screen(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """
        尾盘选股筛选
        
        Args:
            market_data: 市场数据DataFrame
                - price_change: 涨幅
                - volume_ratio: 量比
                - market_cap: 市值
                - above_ma: 是否在均线上方
        
        Returns:
            筛选后的DataFrame
        """
        filtered = market_data.copy()
        initial_count = len(filtered)
        
        # 涨幅筛选
        if 'price_change' in filtered.columns:
            filtered = filtered[
                (filtered['price_change'] >= self.config.price_change_min) &
                (filtered['price_change'] <= self.config.price_change_max)
            ]
            logger.debug(f"After price change filter: {len(filtered)}")
        
        # 量比筛选
        if 'volume_ratio' in filtered.columns:
            filtered = filtered[
                (filtered['volume_ratio'] >= self.config.volume_ratio_min) &
                (filtered['volume_ratio'] <= self.config.volume_ratio_max)
            ]
            logger.debug(f"After volume ratio filter: {len(filtered)}")
        
        # 市值筛选
        if 'market_cap' in filtered.columns:
            filtered = filtered[
                (filtered['market_cap'] >= self.config.market_cap_min) &
                (filtered['market_cap'] <= self.config.market_cap_max)
            ]
            logger.debug(f"After market cap filter: {len(filtered)}")
        
        # 均线筛选
        if self.config.above_ma and 'above_ma' in filtered.columns:
            filtered = filtered[filtered['above_ma'] == True]
            logger.debug(f"After MA filter: {len(filtered)}")
        
        logger.info(f"Endstock screening: {initial_count} -> {len(filtered)}")
        return filtered
    
    def execute(self, market_data: pd.DataFrame, current_time: str) -> List[Dict[str, Any]]:
        """
        执行尾盘选股
        
        Args:
            market_data: 市场数据
            current_time: 当前时间 (HH:MM)
        
        Returns:
            选股结果列表
        """
        # 检查是否在尾盘时间 (14:30后)
        if current_time < '14:30':
            logger.info("Not in endstock time window yet")
            return []
        
        selected = self.screen(market_data)
        
        signals = []
        for _, row in selected.iterrows():
            signals.append({
                'code': row['code'],
                'signal_type': 'endstock_pick',
                'confidence': 0.75,
                'reason': f"涨幅{row['price_change']:.1f}%, 量比{row['volume_ratio']:.1f}",
                'suggested_position': 0.1  # 建议仓位10%
            })
        
        return signals
    
    def rank_stocks(self, stocks: pd.DataFrame) -> pd.DataFrame:
        """
        对选股结果排序
        
        排序规则:
        1. 量比适中 (2-3最佳)
        2. 涨幅适中 (3-4%最佳)
        3. 市值适中 (100亿左右最佳)
        """
        df = stocks.copy()
        
        # 计算得分
        if 'volume_ratio' in df.columns:
            # 量比越接近2.5分越高
            df['volume_score'] = 100 - abs(df['volume_ratio'] - 2.5) * 20
        else:
            df['volume_score'] = 50
        
        if 'price_change' in df.columns:
            # 涨幅越接近3.5分越高
            df['change_score'] = 100 - abs(df['price_change'] - 3.5) * 20
        else:
            df['change_score'] = 50
        
        if 'market_cap' in df.columns:
            # 市值越接近100亿分越高
            df['cap_score'] = 100 - abs(df['market_cap'] - 100) * 0.5
        else:
            df['cap_score'] = 50
        
        # 综合得分
        df['total_score'] = (
            df['volume_score'] * 0.4 +
            df['change_score'] * 0.4 +
            df['cap_score'] * 0.2
        )
        
        return df.sort_values('total_score', ascending=False)
