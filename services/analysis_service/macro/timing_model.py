#!/usr/bin/env python3
"""
宏观择时模型
基于宏观指标生成市场择时信号
"""
import logging
from typing import Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


class Signal(Enum):
    """择时信号"""
    BULLISH = "bullish"      # 看涨
    BEARISH = "bearish"      # 看跌
    NEUTRAL = "neutral"      # 中性


class MacroTimingModel:
    """
    宏观择时模型
    
    基于以下指标生成择时信号:
    1. Shibor趋势 - 利率下行=利好股市
    2. 流动性评分 - 综合流动性状况
    3. M2增速 - 货币供应量变化
    """
    
    def __init__(self):
        self.threshold_bullish = 70  # 看涨阈值
        self.threshold_bearish = 30  # 看跌阈值
    
    def generate_signal(self, macro_data: Dict[str, Any]) -> str:
        """
        生成择时信号
        
        Args:
            macro_data: 宏观数据字典
                - shibor_trend: Shibor趋势 ('up', 'down', 'neutral')
                - liquidity_score: 流动性评分 (0-100)
                - m2_growth: M2增速 (可选)
        
        Returns:
            信号值: 'bullish', 'bearish', 'neutral'
        """
        liquidity_score = macro_data.get('liquidity_score', 50)
        shibor_trend = macro_data.get('shibor_trend', 'neutral')
        
        # 计算综合得分
        score = liquidity_score
        
        # Shibor趋势调整
        if shibor_trend == 'down':
            score += 10  # 利率下行利好
        elif shibor_trend == 'up':
            score -= 10  # 利率上行利空
        
        # 限制范围
        score = max(0, min(100, score))
        
        # 生成信号
        if score >= self.threshold_bullish and shibor_trend in ['down', 'neutral']:
            signal = Signal.BULLISH.value
        elif score <= self.threshold_bearish or shibor_trend == 'up':
            signal = Signal.BEARISH.value
        else:
            signal = Signal.NEUTRAL.value
        
        logger.info(f"Macro timing signal: {signal} (score={score:.1f}, trend={shibor_trend})")
        return signal
    
    def calculate_liquidity_score(self, indicators: Dict[str, float]) -> float:
        """
        计算流动性评分
        
        Args:
            indicators: 指标字典
                - shibor_1w: 1周Shibor
                - shibor_1m: 1月Shibor
                - m2_growth: M2增速 (可选)
        
        Returns:
            流动性评分 (0-100)
        """
        score = 50  # 基础分
        
        # Shibor评分 (越低越好)
        shibor_1w = indicators.get('shibor_1w', 2.5)
        shibor_score = max(0, 100 - shibor_1w * 30)
        
        # M2增速评分 (适中最好，8-12%为理想区间)
        m2_growth = indicators.get('m2_growth', 9.0)
        if 8 <= m2_growth <= 12:
            m2_score = 100
        elif m2_growth < 8:
            m2_score = max(0, 50 + (m2_growth - 5) * 10)
        else:
            m2_score = max(0, 100 - (m2_growth - 12) * 5)
        
        # 加权平均
        score = shibor_score * 0.6 + m2_score * 0.4
        
        return min(100, max(0, score))
    
    def get_market_assessment(self, macro_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取市场评估报告
        
        Args:
            macro_data: 宏观数据
        
        Returns:
            评估报告
        """
        signal = self.generate_signal(macro_data)
        liquidity_score = macro_data.get('liquidity_score', 50)
        
        assessments = {
            Signal.BULLISH.value: {
                'recommendation': '积极配置',
                'position_suggestion': '70-80%',
                'reason': '流动性宽松，利率下行，适合增加权益仓位'
            },
            Signal.BEARISH.value: {
                'recommendation': '防御为主',
                'position_suggestion': '30-40%',
                'reason': '流动性收紧，利率上行，建议降低仓位'
            },
            Signal.NEUTRAL.value: {
                'recommendation': '均衡配置',
                'position_suggestion': '50-60%',
                'reason': '流动性中性，维持均衡仓位'
            }
        }
        
        assessment = assessments.get(signal, assessments[Signal.NEUTRAL.value])
        assessment['signal'] = signal
        assessment['liquidity_score'] = liquidity_score
        assessment['shibor_trend'] = macro_data.get('shibor_trend', 'neutral')
        
        return assessment
