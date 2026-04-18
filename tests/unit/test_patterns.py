#!/usr/bin/env python3
"""
K线形态识别模块测试
"""
import pytest
import pandas as pd


class TestPatternRecognizer:
    """形态识别器测试"""
    
    def test_morning_star_detection(self):
        """测试早晨之星形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 早晨之星形态数据
        # 第一根: 阴线 (open > close)
        # 第二根: 小实体星线
        # 第三根: 阳线 (close > open)，收盘价深入第一根实体
        candles = pd.DataFrame({
            'open': [105, 92, 90],   # 第一根开盘105收盘98(阴线)，第二根小实体，第三根阳线收盘102
            'high': [106, 94, 104],
            'low': [97, 89, 89],
            'close': [98, 91, 102]   # 第三根收盘102 > 第一根中间价(105+98)/2=101.5
        })
        
        result = recognizer.detect_morning_star(candles)
        assert result == True
    
    def test_evening_star_detection(self):
        """测试黄昏之星形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 黄昏之星形态数据
        candles = pd.DataFrame({
            'open': [100, 110, 105],
            'high': [105, 115, 108],
            'low': [98, 102, 95],
            'close': [102, 112, 98]
        })
        
        result = recognizer.detect_evening_star(candles)
        assert result == True
    
    def test_hammer_detection(self):
        """测试锤子线形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 锤子线形态: 小实体在上方，长下影线
        # open=100, close=101 (小实体1), low=95 (下影线6), high=102 (上影线1)
        candles = pd.DataFrame({
            'open': [100],
            'high': [102],
            'low': [94],    # 下影线 = 100-94 = 6，是实体(1)的6倍
            'close': [101]
        })
        
        result = recognizer.detect_hammer(candles)
        assert result == True
    
    def test_shooting_star_detection(self):
        """测试射击之星形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 射击之星形态: 小实体在下方，长上影线
        # open=100, close=99 (小实体1), high=106 (上影线6), low=98 (下影线1)
        candles = pd.DataFrame({
            'open': [100],
            'high': [106],   # 上影线 = 106-100 = 6，是实体(1)的6倍
            'low': [98],
            'close': [99]
        })
        
        result = recognizer.detect_shooting_star(candles)
        assert result == True
    
    def test_bullish_engulfing_detection(self):
        """测试看涨吞没形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 看涨吞没形态
        candles = pd.DataFrame({
            'open': [105, 98],
            'high': [105, 108],
            'low': [100, 97],
            'close': [100, 108]
        })
        
        result = recognizer.detect_bullish_engulfing(candles)
        assert result == True
    
    def test_bearish_engulfing_detection(self):
        """测试看跌吞没形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 看跌吞没形态
        candles = pd.DataFrame({
            'open': [100, 105],
            'high': [105, 105],
            'low': [98, 95],
            'close': [105, 95]
        })
        
        result = recognizer.detect_bearish_engulfing(candles)
        assert result == True
    
    def test_three_white_soldiers_detection(self):
        """测试红三兵形态检测"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 红三兵形态: 三根阳线，收盘价递增，开盘价在前一根实体范围内
        # 第一根: open=100, close=102
        # 第二根: open=101(在第一根100-102范围内), close=104
        # 第三根: open=103(在第二根101-104范围内), close=106
        candles = pd.DataFrame({
            'open': [100, 101, 103],
            'high': [103, 105, 107],
            'low': [99, 100, 102],
            'close': [102, 104, 106]
        })
        
        result = recognizer.detect_three_white_soldiers(candles)
        assert result == True
    
    def test_detect_all_patterns(self):
        """测试检测所有形态"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        candles = pd.DataFrame({
            'open': [100, 90, 95],
            'high': [105, 95, 110],
            'low': [98, 85, 94],
            'close': [102, 88, 108]
        })
        
        results = recognizer.detect_all_patterns(candles)
        
        assert isinstance(results, dict)
        assert 'morning_star' in results
        assert 'evening_star' in results
    
    def test_insufficient_data(self):
        """测试数据不足情况"""
        from core.indicators.patterns import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 只有1根K线
        candles = pd.DataFrame({
            'open': [100],
            'high': [105],
            'low': [98],
            'close': [102]
        })
        
        result = recognizer.detect_morning_star(candles)
        assert result == False
        
        result = recognizer.detect_evening_star(candles)
        assert result == False
