#!/usr/bin/env python3
"""
凯利公式仓位计算器

根据胜率和盈亏比计算最优仓位
"""
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class KellyResult:
    """凯利公式计算结果"""
    kelly_fraction: float  # 凯利比例
    recommended_position: float  # 推荐仓位
    max_position: float  # 最大仓位限制


class KellyCalculator:
    """凯利公式计算器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.max_single_position = self.config.get('max_single_position', 0.20)
        self.half_kelly = self.config.get('half_kelly', True)
    
    def calculate(self, win_rate: float, win_loss_ratio: float) -> KellyResult:
        """
        计算凯利公式
        
        公式: f = (p*b - q) / b
        f: 最优仓位比例
        p: 胜率
        q: 败率 (1-p)
        b: 盈亏比
        
        Args:
            win_rate: 胜率 (0-1)
            win_loss_ratio: 盈亏比 (平均盈利/平均亏损)
        
        Returns:
            KellyResult 计算结果
        """
        p = win_rate
        q = 1 - p
        b = win_loss_ratio
        
        # 凯利公式
        if b <= 0:
            kelly = 0
        else:
            kelly = (p * b - q) / b
        
        # 应用半凯利策略
        if self.half_kelly:
            kelly = kelly * 0.5
        
        # 限制最大仓位
        recommended = min(kelly, self.max_single_position)
        
        # 如果凯利值为负，建议不交易
        if kelly < 0:
            recommended = 0
        
        return KellyResult(
            kelly_fraction=kelly,
            recommended_position=recommended,
            max_position=self.max_single_position
        )
    
    def calculate_batch(
        self,
        strategies: Dict[str, Dict[str, float]]
    ) -> Dict[str, KellyResult]:
        """
        批量计算多个策略的仓位
        
        Args:
            strategies: {策略名: {'win_rate': 胜率, 'win_loss_ratio': 盈亏比}}
        
        Returns:
            {策略名: KellyResult}
        """
        results = {}
        for name, params in strategies.items():
            results[name] = self.calculate(
                win_rate=params.get('win_rate', 0.5),
                win_loss_ratio=params.get('win_loss_ratio', 1.0)
            )
        return results
