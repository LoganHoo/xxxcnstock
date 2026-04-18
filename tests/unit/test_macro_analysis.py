#!/usr/bin/env python3
"""
宏观分析模块测试
"""
import pytest
import pandas as pd
from datetime import datetime


class TestMacroDataCollector:
    """宏观数据收集器测试"""
    
    def test_shibor_data_collection(self):
        """测试Shibor数据收集"""
        from services.analysis_service.macro.data_collector import MacroDataCollector
        collector = MacroDataCollector()
        
        # 测试获取Shibor数据
        data = collector.fetch_shibor()
        
        assert isinstance(data, pd.DataFrame)
        assert 'shibor_1w' in data.columns or len(data.columns) >= 1
    
    def test_macro_indicators_collection(self):
        """测试宏观指标收集"""
        from services.analysis_service.macro.data_collector import MacroDataCollector
        collector = MacroDataCollector()
        
        indicators = collector.fetch_macro_indicators()
        
        assert isinstance(indicators, dict)
        assert 'shibor_trend' in indicators or 'liquidity_score' in indicators


class TestMacroTimingModel:
    """宏观择时模型测试"""
    
    def test_bullish_signal_generation(self):
        """测试看涨信号生成"""
        from services.analysis_service.macro.timing_model import MacroTimingModel, Signal
        
        model = MacroTimingModel()
        signal = model.generate_signal({
            'shibor_trend': 'down',
            'liquidity_score': 75
        })
        
        assert signal == Signal.BULLISH.value
    
    def test_bearish_signal_generation(self):
        """测试看跌信号生成"""
        from services.analysis_service.macro.timing_model import MacroTimingModel, Signal
        
        model = MacroTimingModel()
        signal = model.generate_signal({
            'shibor_trend': 'up',
            'liquidity_score': 25
        })
        
        assert signal == Signal.BEARISH.value
    
    def test_neutral_signal_generation(self):
        """测试中性和信号生成"""
        from services.analysis_service.macro.timing_model import MacroTimingModel, Signal
        
        model = MacroTimingModel()
        signal = model.generate_signal({
            'shibor_trend': 'neutral',
            'liquidity_score': 50
        })
        
        assert signal == Signal.NEUTRAL.value
    
    def test_liquidity_score_calculation(self):
        """测试流动性评分计算"""
        from services.analysis_service.macro.timing_model import MacroTimingModel
        
        model = MacroTimingModel()
        score = model.calculate_liquidity_score({
            'shibor_1w': 2.5,
            'shibor_1m': 2.8,
            'm2_growth': 8.5
        })
        
        assert 0 <= score <= 100


class TestMacroIndicators:
    """宏观指标测试"""
    
    def test_shibor_trend_calculation(self):
        """测试Shibor趋势计算"""
        from services.analysis_service.macro.indicators import calculate_shibor_trend
        
        shibor_data = pd.Series([2.5, 2.4, 2.3, 2.2])
        trend = calculate_shibor_trend(shibor_data)
        
        assert trend in ['up', 'down', 'neutral']
    
    def test_liquidity_indicator_calculation(self):
        """测试流动性指标计算"""
        from services.analysis_service.macro.indicators import calculate_liquidity_indicator
        
        indicators = {
            'shibor_1w': 2.5,
            'deposit_rate': 1.5,
            'loan_rate': 4.5
        }
        
        result = calculate_liquidity_indicator(indicators)
        
        assert isinstance(result, dict)
        assert 'liquidity_score' in result
