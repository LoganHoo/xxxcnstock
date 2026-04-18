#!/usr/bin/env python3
"""
K线形态识别模块
识别常见的K线形态
"""
import pandas as pd
import numpy as np
from typing import Dict, List


class PatternRecognizer:
    """
    K线形态识别器
    
    支持形态:
    - 早晨之星 (Morning Star)
    - 黄昏之星 (Evening Star)
    - 锤子线 (Hammer)
    - 射击之星 (Shooting Star)
    - 看涨吞没 (Bullish Engulfing)
    - 看跌吞没 (Bearish Engulfing)
    - 红三兵 (Three White Soldiers)
    """
    
    BULLISH_PATTERNS = [
        'morning_star',
        'three_white_soldiers',
        'bullish_engulfing',
        'hammer',
    ]
    
    BEARISH_PATTERNS = [
        'evening_star',
        'two_crows',
        'bearish_engulfing',
        'shooting_star',
    ]
    
    def detect_morning_star(self, candles: pd.DataFrame) -> bool:
        """
        检测早晨之星形态
        
        形态特征:
        1. 第一根: 长阴线
        2. 第二根: 小实体(星线)，向下跳空
        3. 第三根: 长阳线，收盘价深入第一根实体
        """
        if len(candles) < 3:
            return False
        
        first = candles.iloc[-3]
        second = candles.iloc[-2]
        third = candles.iloc[-1]
        
        # 第一根阴线
        first_bearish = first['close'] < first['open']
        first_body = abs(first['close'] - first['open'])
        
        # 第二根小实体
        second_body = abs(second['close'] - second['open'])
        second_range = second['high'] - second['low']
        second_small = second_body < second_range * 0.3 if second_range > 0 else False
        
        # 第三根阳线
        third_bullish = third['close'] > third['open']
        third_body = abs(third['close'] - third['open'])
        
        # 第三根收盘价深入第一根实体
        first_mid = (first['open'] + first['close']) / 2
        third_strong = third['close'] > first_mid
        
        return first_bearish and second_small and third_bullish and third_strong
    
    def detect_evening_star(self, candles: pd.DataFrame) -> bool:
        """
        检测黄昏之星形态
        
        形态特征:
        1. 第一根: 长阳线
        2. 第二根: 小实体(星线)，向上跳空
        3. 第三根: 长阴线，收盘价深入第一根实体
        """
        if len(candles) < 3:
            return False
        
        first = candles.iloc[-3]
        second = candles.iloc[-2]
        third = candles.iloc[-1]
        
        # 第一根阳线
        first_bullish = first['close'] > first['open']
        first_body = abs(first['close'] - first['open'])
        
        # 第二根小实体
        second_body = abs(second['close'] - second['open'])
        second_range = second['high'] - second['low']
        second_small = second_body < second_range * 0.3 if second_range > 0 else False
        
        # 第三根阴线
        third_bearish = third['close'] < third['open']
        third_body = abs(third['close'] - third['open'])
        
        # 第三根收盘价深入第一根实体
        first_mid = (first['open'] + first['close']) / 2
        third_strong = third['close'] < first_mid
        
        return first_bullish and second_small and third_bearish and third_strong
    
    def detect_hammer(self, candles: pd.DataFrame) -> bool:
        """
        检测锤子线形态
        
        形态特征:
        1. 小实体在上方
        2. 长下影线 (至少2倍实体)
        3. 上影线很短
        """
        if len(candles) < 1:
            return False
        
        candle = candles.iloc[-1]
        
        open_price = candle['open']
        close = candle['close']
        high = candle['high']
        low = candle['low']
        
        body = abs(close - open_price)
        upper_shadow = high - max(open_price, close)
        lower_shadow = min(open_price, close) - low
        
        # 长下影线，短上影线
        if body == 0:
            return False
        
        return lower_shadow >= body * 2 and upper_shadow <= body
    
    def detect_shooting_star(self, candles: pd.DataFrame) -> bool:
        """
        检测射击之星形态
        
        形态特征:
        1. 小实体在下方
        2. 长上影线 (至少2倍实体)
        3. 下影线很短
        """
        if len(candles) < 1:
            return False
        
        candle = candles.iloc[-1]
        
        open_price = candle['open']
        close = candle['close']
        high = candle['high']
        low = candle['low']
        
        body = abs(close - open_price)
        upper_shadow = high - max(open_price, close)
        lower_shadow = min(open_price, close) - low
        
        # 长上影线，短下影线
        if body == 0:
            return False
        
        return upper_shadow >= body * 2 and lower_shadow <= body
    
    def detect_bullish_engulfing(self, candles: pd.DataFrame) -> bool:
        """
        检测看涨吞没形态
        
        形态特征:
        1. 第一根阴线
        2. 第二根阳线，实体完全吞没第一根
        """
        if len(candles) < 2:
            return False
        
        first = candles.iloc[-2]
        second = candles.iloc[-1]
        
        # 第一根阴线
        first_bearish = first['close'] < first['open']
        
        # 第二根阳线
        second_bullish = second['close'] > second['open']
        
        # 第二根吞没第一根
        engulfing = (
            second['open'] <= first['close'] and
            second['close'] >= first['open']
        )
        
        return first_bearish and second_bullish and engulfing
    
    def detect_bearish_engulfing(self, candles: pd.DataFrame) -> bool:
        """
        检测看跌吞没形态
        
        形态特征:
        1. 第一根阳线
        2. 第二根阴线，实体完全吞没第一根
        """
        if len(candles) < 2:
            return False
        
        first = candles.iloc[-2]
        second = candles.iloc[-1]
        
        # 第一根阳线
        first_bullish = first['close'] > first['open']
        
        # 第二根阴线
        second_bearish = second['close'] < second['open']
        
        # 第二根吞没第一根
        engulfing = (
            second['open'] >= first['close'] and
            second['close'] <= first['open']
        )
        
        return first_bullish and second_bearish and engulfing
    
    def detect_three_white_soldiers(self, candles: pd.DataFrame) -> bool:
        """
        检测红三兵形态
        
        形态特征:
        1. 连续三根阳线
        2. 每根收盘价高于前一根
        3. 每根开盘价在前一根实体范围内
        """
        if len(candles) < 3:
            return False
        
        first = candles.iloc[-3]
        second = candles.iloc[-2]
        third = candles.iloc[-1]
        
        # 三根都是阳线
        first_bullish = first['close'] > first['open']
        second_bullish = second['close'] > second['open']
        third_bullish = third['close'] > third['open']
        
        # 收盘价递增
        ascending = third['close'] > second['close'] > first['close']
        
        # 开盘价在前一根实体范围内
        second_open_in_first = first['open'] < second['open'] < first['close']
        third_open_in_second = second['open'] < third['open'] < second['close']
        
        return (
            first_bullish and second_bullish and third_bullish and
            ascending and second_open_in_first and third_open_in_second
        )
    
    def detect_all_patterns(self, candles: pd.DataFrame) -> Dict[str, bool]:
        """检测所有形态"""
        return {
            'morning_star': self.detect_morning_star(candles),
            'evening_star': self.detect_evening_star(candles),
            'hammer': self.detect_hammer(candles),
            'shooting_star': self.detect_shooting_star(candles),
            'bullish_engulfing': self.detect_bullish_engulfing(candles),
            'bearish_engulfing': self.detect_bearish_engulfing(candles),
            'three_white_soldiers': self.detect_three_white_soldiers(candles),
        }
    
    def get_bullish_signals(self, candles: pd.DataFrame) -> List[str]:
        """获取看涨信号列表"""
        all_patterns = self.detect_all_patterns(candles)
        return [p for p in self.BULLISH_PATTERNS if all_patterns.get(p, False)]
    
    def get_bearish_signals(self, candles: pd.DataFrame) -> List[str]:
        """获取看跌信号列表"""
        all_patterns = self.detect_all_patterns(candles)
        return [p for p in self.BEARISH_PATTERNS if all_patterns.get(p, False)]
