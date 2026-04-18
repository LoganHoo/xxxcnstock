#!/usr/bin/env python3
"""
宏观指标计算模块
计算Shibor趋势、流动性指标等
"""
import pandas as pd
import numpy as np
from typing import Dict, Any


def calculate_shibor_trend(shibor_series: pd.Series, window: int = 5) -> str:
    """
    计算Shibor趋势
    
    Args:
        shibor_series: Shibor利率序列
        window: 趋势计算窗口
    
    Returns:
        趋势方向: 'up', 'down', 'neutral'
    """
    if len(shibor_series) < window * 2:
        return 'neutral'
    
    recent_mean = shibor_series.tail(window).mean()
    previous_mean = shibor_series.head(window).mean()
    
    change_pct = (recent_mean - previous_mean) / previous_mean
    
    if change_pct < -0.02:  # 下降超过2%
        return 'down'
    elif change_pct > 0.02:  # 上升超过2%
        return 'up'
    else:
        return 'neutral'


def calculate_liquidity_indicator(indicators: Dict[str, float]) -> Dict[str, Any]:
    """
    计算流动性指标
    
    Args:
        indicators: 指标字典
            - shibor_1w: 1周Shibor
            - deposit_rate: 存款利率
            - loan_rate: 贷款利率
    
    Returns:
        流动性指标字典
    """
    shibor_1w = indicators.get('shibor_1w', 2.5)
    deposit_rate = indicators.get('deposit_rate', 1.5)
    loan_rate = indicators.get('loan_rate', 4.5)
    
    # 计算存贷利差
    spread = loan_rate - deposit_rate
    
    # 流动性评分 (0-100)
    # Shibor越低，流动性越好
    liquidity_score = max(0, min(100, 100 - shibor_1w * 25))
    
    # 资金成本评级
    if shibor_1w < 2.0:
        cost_level = 'low'
    elif shibor_1w < 3.0:
        cost_level = 'medium'
    else:
        cost_level = 'high'
    
    return {
        'liquidity_score': liquidity_score,
        'funding_cost_level': cost_level,
        'spread': spread,
        'shibor_1w': shibor_1w
    }


def calculate_interest_rate_spread(
    long_term_rate: float,
    short_term_rate: float
) -> Dict[str, Any]:
    """
    计算期限利差
    
    Args:
        long_term_rate: 长期利率
        short_term_rate: 短期利率
    
    Returns:
        利差分析结果
    """
    spread = long_term_rate - short_term_rate
    
    # 期限结构分析
    if spread > 1.0:
        structure = 'steep'  # 陡峭 - 经济扩张预期
        signal = 'bullish'
    elif spread > 0.3:
        structure = 'normal'  # 正常
        signal = 'neutral'
    elif spread > 0:
        structure = 'flat'  # 平坦
        signal = 'caution'
    else:
        structure = 'inverted'  # 倒挂 - 经济衰退预警
        signal = 'bearish'
    
    return {
        'spread': spread,
        'structure': structure,
        'signal': signal
    }


def calculate_credit_impulse(m2_growth: pd.Series, gdp_growth: float) -> float:
    """
    计算信贷脉冲
    
    信贷脉冲 = M2增速 - 名义GDP增速
    正值表示流动性宽松，负值表示收紧
    
    Args:
        m2_growth: M2增速序列
        gdp_growth: GDP增速
    
    Returns:
        信贷脉冲值
    """
    if len(m2_growth) == 0:
        return 0.0
    
    current_m2 = m2_growth.iloc[-1]
    credit_impulse = current_m2 - gdp_growth
    
    return credit_impulse


def analyze_policy_stance(indicators: Dict[str, Any]) -> str:
    """
    分析货币政策立场
    
    Args:
        indicators: 指标字典
            - shibor_trend: Shibor趋势
            - rrr_changes: 存款准备金率变化
            - policy_rate_changes: 政策利率变化
    
    Returns:
        政策立场: 'easing', 'neutral', 'tightening'
    """
    score = 0
    
    # Shibor趋势
    shibor_trend = indicators.get('shibor_trend', 'neutral')
    if shibor_trend == 'down':
        score += 1
    elif shibor_trend == 'up':
        score -= 1
    
    # 存款准备金率变化
    rrr_changes = indicators.get('rrr_changes', 0)
    if rrr_changes < 0:
        score += 1  # 降准
    elif rrr_changes > 0:
        score -= 1  # 升准
    
    # 政策利率变化
    policy_changes = indicators.get('policy_rate_changes', 0)
    if policy_changes < 0:
        score += 1  # 降息
    elif policy_changes > 0:
        score -= 1  # 加息
    
    # 判断立场
    if score >= 2:
        return 'easing'  # 宽松
    elif score <= -2:
        return 'tightening'  # 紧缩
    else:
        return 'neutral'  # 中性
