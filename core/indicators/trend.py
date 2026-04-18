#!/usr/bin/env python3
"""
趋势指标

包含EMA、MACD等趋势跟踪指标
"""
import pandas as pd
import numpy as np


def calculate_ema(prices: pd.Series, period: int = 20) -> pd.Series:
    """
    计算指数移动平均线
    
    Args:
        prices: 价格序列
        period: 周期
    
    Returns:
        EMA序列
    """
    return prices.ewm(span=period, adjust=False).mean()


def calculate_macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> tuple:
    """
    计算MACD指标
    
    Args:
        prices: 价格序列
        fast: 快线周期
        slow: 慢线周期
        signal: 信号线周期
    
    Returns:
        (macd_line, signal_line, histogram)
    """
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram


def detect_macd_cross(
    macd_line: pd.Series,
    signal_line: pd.Series,
    cross_type: str = 'golden'
) -> bool:
    """
    检测MACD交叉
    
    Args:
        macd_line: MACD线
        signal_line: 信号线
        cross_type: 'golden' (金叉) 或 'death' (死叉)
    
    Returns:
        是否发生交叉
    """
    if len(macd_line) < 2 or len(signal_line) < 2:
        return False
    
    # 前一天
    prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
    # 今天
    curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
    
    if cross_type == 'golden':
        # 金叉: 前一天MACD<信号线，今天MACD>信号线
        return prev_diff < 0 and curr_diff > 0
    elif cross_type == 'death':
        # 死叉: 前一天MACD>信号线，今天MACD<信号线
        return prev_diff > 0 and curr_diff < 0
    
    return False
