#!/usr/bin/env python3
"""
技术指标计算模块
计算常用技术指标
"""
import pandas as pd
import numpy as np
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """
    技术指标计算器
    
    支持指标:
    - EMA (指数移动平均)
    - MACD (指数平滑异同平均线)
    - RSI (相对强弱指标)
    - KDJ (随机指标)
    - BOLL (布林带)
    """
    
    @staticmethod
    def calculate_ema(prices: pd.Series, period: int = 20) -> pd.Series:
        """
        计算EMA
        
        Args:
            prices: 价格序列
            period: 周期
        
        Returns:
            EMA序列
        """
        return prices.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def calculate_macd(
        prices: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        计算MACD
        
        Args:
            prices: 价格序列
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期
        
        Returns:
            (MACD线, 信号线, 柱状图)
        """
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
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
            cross_type: 'golden'(金叉) 或 'death'(死叉)
        
        Returns:
            是否发生交叉
        """
        if len(macd_line) < 2:
            return False
        
        # 今天和昨天的差值
        diff_today = macd_line.iloc[-1] - signal_line.iloc[-1]
        diff_yesterday = macd_line.iloc[-2] - signal_line.iloc[-2]
        
        if cross_type == 'golden':
            # 金叉: 昨天MACD<信号线，今天MACD>信号线
            return diff_yesterday < 0 and diff_today > 0
        else:  # death
            # 死叉: 昨天MACD>信号线，今天MACD<信号线
            return diff_yesterday > 0 and diff_today < 0
    
    @staticmethod
    def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """
        计算RSI
        
        Args:
            prices: 价格序列
            period: 周期
        
        Returns:
            RSI序列
        """
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def calculate_kdj(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        n: int = 9,
        m1: int = 3,
        m2: int = 3
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        计算KDJ指标
        
        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            n: RSV周期
            m1: K平滑系数
            m2: D平滑系数
        
        Returns:
            (K值, D值, J值)
        """
        lowest_low = low.rolling(window=n).min()
        highest_high = high.rolling(window=n).max()
        
        rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
        
        k = rsv.ewm(com=m1-1, adjust=False).mean()
        d = k.ewm(com=m2-1, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return k, d, j
    
    @staticmethod
    def calculate_bollinger_bands(
        prices: pd.Series,
        period: int = 20,
        std_dev: float = 2.0
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        计算布林带
        
        Args:
            prices: 价格序列
            period: 周期
            std_dev: 标准差倍数
        
        Returns:
            (上轨, 中轨, 下轨)
        """
        middle = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        
        return upper, middle, lower
    
    @staticmethod
    def calculate_atr(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14
    ) -> pd.Series:
        """
        计算ATR (平均真实波幅)
        
        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            period: 周期
        
        Returns:
            ATR序列
        """
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        return atr
