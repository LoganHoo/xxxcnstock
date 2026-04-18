#!/usr/bin/env python3
"""
回测引擎测试
"""
import pytest
import pandas as pd
import numpy as np


class TestBacktraderAdapter:
    """Backtrader适配器测试"""
    
    def test_adapter_creation(self):
        """测试适配器创建"""
        from services.backtest_service.engine.backtrader_adapter import BacktraderAdapter
        adapter = BacktraderAdapter()
        assert adapter is not None
        assert adapter.cerebro is not None
    
    def test_adapter_with_config(self):
        """测试带配置的适配器"""
        from services.backtest_service.engine.backtrader_adapter import BacktraderAdapter
        config = {
            'initial_cash': 200000.0,
            'commission': 0.0003
        }
        adapter = BacktraderAdapter(config)
        assert adapter.config['initial_cash'] == 200000.0


class TestDataFeeder:
    """数据供给器测试"""
    
    def test_prepare_data(self):
        """测试数据准备"""
        from services.backtest_service.engine.data_feeder import DataFeeder
        feeder = DataFeeder()
        
        df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [99, 100, 101],
            'close': [102, 103, 104],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2024-01-01', periods=3))
        
        data = feeder.prepare_data(df, name='TEST')
        assert data is not None
        assert data._name == 'TEST'
    
    def test_resample_data(self):
        """测试数据重采样"""
        from services.backtest_service.engine.data_feeder import DataFeeder
        feeder = DataFeeder()
        
        # 创建日数据
        df = pd.DataFrame({
            'open': [100] * 10,
            'high': [105] * 10,
            'low': [99] * 10,
            'close': [102] * 10,
            'volume': [1000] * 10
        }, index=pd.date_range('2024-01-01', periods=10))
        
        # 重采样为周数据
        weekly = feeder.resample_data(df, 'W')
        assert len(weekly) > 0


class TestStrategyWrapper:
    """策略包装器测试"""
    
    def test_wrapper_creation(self):
        """测试包装器创建"""
        import backtrader as bt
        from services.backtest_service.strategy_wrapper import StrategyWrapper
        
        # 策略包装器是Backtrader策略的子类
        assert issubclass(StrategyWrapper, bt.Strategy)


class TestResultAnalyzer:
    """结果分析器测试"""
    
    def test_analyzer_creation(self):
        """测试分析器创建"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        assert analyzer is not None
    
    def test_total_return_calculation(self):
        """测试总收益计算"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        returns = [0.01, 0.02, -0.01, 0.015]
        total_return = analyzer._calc_total_return(returns)
        
        assert total_return > 0
    
    def test_sharpe_ratio_calculation(self):
        """测试Sharpe比率计算"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        returns = [0.01, 0.02, -0.005, 0.015, 0.01]
        sharpe = analyzer._calc_sharpe_ratio(returns)
        
        assert isinstance(sharpe, float)
    
    def test_max_drawdown_calculation(self):
        """测试最大回撤计算"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        # 先涨后跌的收益率序列
        returns = [0.05, 0.03, -0.02, -0.05, -0.03, 0.02]
        drawdown = analyzer._calc_max_drawdown(returns)
        
        assert drawdown > 0
        assert drawdown < 1
    
    def test_win_rate_calculation(self):
        """测试胜率计算"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        trades = [
            {'pnl': 100},
            {'pnl': -50},
            {'pnl': 80},
            {'pnl': -30}
        ]
        
        win_rate = analyzer._calc_win_rate(trades)
        assert win_rate == 0.5  # 2胜2负
    
    def test_profit_loss_ratio(self):
        """测试盈亏比计算"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        trades = [
            {'pnl': 100},
            {'pnl': 120},
            {'pnl': -50},
            {'pnl': -30}
        ]
        
        ratio = analyzer._calc_profit_loss_ratio(trades)
        assert ratio > 0
    
    def test_generate_report(self):
        """测试报告生成"""
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        results = {
            'returns': [0.01, 0.02, -0.01],
            'trades': [
                {'pnl': 100, 'pnl_pct': 0.05},
                {'pnl': -50, 'pnl_pct': -0.02}
            ]
        }
        
        report = analyzer.generate_report(results)
        assert '总收益率' in report
        assert 'Sharpe比率' in report
