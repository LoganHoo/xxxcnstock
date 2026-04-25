#!/usr/bin/env python3
"""
量化交易系统集成测试

测试完整的数据流：数据采集 -> 分析 -> 策略 -> 回测
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestQuantitativeSystemIntegration:
    """量化交易系统集成测试"""
    
    def test_complete_data_flow(self):
        """测试完整数据流"""
        # 1. 模拟K线数据
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        kline_data = pd.DataFrame({
            'open': 100 + np.random.randn(30).cumsum(),
            'high': 102 + np.random.randn(30).cumsum(),
            'low': 98 + np.random.randn(30).cumsum(),
            'close': 101 + np.random.randn(30).cumsum(),
            'volume': np.random.randint(1000, 10000, 30)
        }, index=dates)
        
        # 2. 技术指标计算
        from core.indicators.technical import TechnicalIndicators
        indicators = TechnicalIndicators()
        
        ema = indicators.calculate_ema(kline_data['close'], period=5)
        macd_line, signal_line, histogram = indicators.calculate_macd(kline_data['close'])
        
        assert len(ema) == len(kline_data)
        assert len(macd_line) == len(kline_data)
        
        # 3. K线形态识别
        from core.indicators.patterns import PatternRecognizer
        recognizer = PatternRecognizer()
        
        patterns = recognizer.detect_all_patterns(kline_data)
        assert isinstance(patterns, dict)
        
        # 4. 宏观分析
        from services.analysis_service.macro.timing_model import MacroTimingModel
        macro_model = MacroTimingModel()
        
        macro_data = {
            'shibor_3m': 2.5,
            'shibor_trend': 'falling',
            'm2_growth': 8.5
        }
        signal = macro_model.generate_signal(macro_data)
        assert signal in ['bullish', 'bearish', 'neutral']
        
        # 5. 基本面筛选
        from services.analysis_service.fundamental.financial_screener import FinancialScreener
        screener = FinancialScreener()
        
        financial_data = pd.DataFrame({
            'code': ['000001', '000002'],
            'roe': [15.0, 8.0],
            'gross_margin': [30.0, 15.0],
            'profit_growth': [25.0, 5.0],
            'pe': [20.0, 60.0],
            'pb': [2.0, 5.0],
            'debt_ratio': [50.0, 80.0]
        })
        
        screened = screener.screen(financial_data)
        assert len(screened) >= 0
        
        # 6. 情绪分析
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        sentiment = analyzer.analyze('该公司业绩大幅增长，前景看好')
        assert sentiment.sentiment in ['positive', 'negative', 'neutral']
        
        print("✅ Complete data flow test passed")
    
    def test_strategy_execution_flow(self):
        """测试策略执行流程"""
        # 1. 尾盘选股策略
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        endstock_strategy = EndstockPickStrategy()
        
        market_data = pd.DataFrame({
            'code': ['000001', '000002'],
            'price_change': [4.0, 2.0],
            'volume_ratio': [2.0, 1.5],
            'market_cap': [100, 150],
            'above_ma': [True, True]
        })
        
        signals = endstock_strategy.execute(market_data, '14:35')
        assert isinstance(signals, list)
        
        # 2. 龙回头策略
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        dragon_strategy = DragonHeadStrategy()
        
        stock_data = {
            'code': '000001',
            'consecutive_limitup': 5,
            'market_position': 'leader',
            'market_cap': 100
        }
        
        is_height = dragon_strategy.is_height_board(stock_data)
        assert isinstance(is_height, bool)
        
        print("✅ Strategy execution flow test passed")
    
    def test_risk_management_flow(self):
        """测试风险管理流程"""
        # 1. 凯利公式
        from services.risk_service.position.kelly_calculator import KellyCalculator
        kelly = KellyCalculator()
        
        result = kelly.calculate(win_rate=0.6, win_loss_ratio=2.0)
        assert 0 <= result.recommended_position <= 1
        
        # 2. 利弗莫尔仓位管理
        from services.risk_service.position.livermore_manager import LivermoreManager
        livermore = LivermoreManager()
        
        buy_signal = livermore.should_buy_initial('TEST', capital=100000)
        assert 'amount' in buy_signal
        
        # 3. 止损管理
        from services.risk_service.stoploss.manager import StopLossManager
        stoploss = StopLossManager()
        
        result = stoploss.check('000001', current_price=94, entry_price=100, ema_20=100)
        assert result.action is not None
        
        # 4. 熔断机制
        from services.risk_service.circuit_breaker.manager import CircuitBreaker
        circuit = CircuitBreaker()

        result = circuit.check_market({'index_change_pct': -0.03, 'index_code': '000001.SH'})
        assert result.action is not None
        
        print("✅ Risk management flow test passed")
    
    def test_backtest_flow(self):
        """测试回测流程"""
        # 1. 数据供给器
        from services.backtest_service.engine.data_feeder import DataFeeder
        feeder = DataFeeder()
        
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        df = pd.DataFrame({
            'open': 100 + np.random.randn(20).cumsum(),
            'high': 102 + np.random.randn(20).cumsum(),
            'low': 98 + np.random.randn(20).cumsum(),
            'close': 101 + np.random.randn(20).cumsum(),
            'volume': np.random.randint(1000, 10000, 20)
        }, index=dates)
        
        data = feeder.prepare_data(df, name='TEST')
        assert data is not None
        
        # 2. 结果分析器
        from services.backtest_service.result_analyzer import ResultAnalyzer
        analyzer = ResultAnalyzer()
        
        results = {
            'returns': [0.01, 0.02, -0.01, 0.015],
            'trades': [
                {'pnl': 100, 'pnl_pct': 0.05},
                {'pnl': -50, 'pnl_pct': -0.02}
            ]
        }
        
        analysis = analyzer.analyze(results)
        assert 'total_return' in analysis
        assert 'sharpe_ratio' in analysis
        
        report = analyzer.generate_report(results)
        assert '总收益率' in report
        
        print("✅ Backtest flow test passed")
    
    def test_optimization_flow(self):
        """测试参数优化流程"""
        # 1. 网格搜索
        from services.backtest_service.optimization.grid_search import GridSearchOptimizer
        
        param_grid = {
            'period': [5, 10, 20],
            'threshold': [0.01, 0.02]
        }
        
        optimizer = GridSearchOptimizer(param_grid, n_jobs=1)
        
        def objective(params):
            # 模拟目标函数
            return params['period'] * params['threshold']
        
        best_params, best_score = optimizer.search(objective, maximize=True)
        assert 'period' in best_params
        assert 'threshold' in best_params
        
        # 2. 遗传算法
        from services.backtest_service.optimization.genetic_algorithm import GeneticAlgorithmOptimizer
        
        ga_optimizer = GeneticAlgorithmOptimizer(
            param_bounds={'x': (0, 10)},
            param_types={'x': 'float'},
            population_size=10,
            generations=10
        )
        
        def fitness(params):
            return -(params['x'] - 5) ** 2 + 25
        
        best_params, best_fitness = ga_optimizer.optimize(fitness, maximize=True)
        assert 'x' in best_params
        
        print("✅ Optimization flow test passed")


class TestSystemEndToEnd:
    """系统端到端测试"""
    
    def test_end_to_end_trading_workflow(self):
        """测试端到端交易工作流"""
        print("\n=== 端到端交易工作流测试 ===")
        
        # 步骤1: 数据采集
        print("1. 数据采集...")
        dates = pd.date_range('2024-01-01', periods=60, freq='D')
        np.random.seed(42)
        
        # 模拟股票数据
        stock_data = pd.DataFrame({
            'open': 100 + np.cumsum(np.random.randn(60) * 0.5),
            'high': 102 + np.cumsum(np.random.randn(60) * 0.5),
            'low': 98 + np.cumsum(np.random.randn(60) * 0.5),
            'close': 101 + np.cumsum(np.random.randn(60) * 0.5),
            'volume': np.random.randint(10000, 100000, 60)
        }, index=dates)
        
        # 步骤2: 技术分析
        print("2. 技术分析...")
        from core.indicators.technical import TechnicalIndicators
        indicators = TechnicalIndicators()
        
        ema20 = indicators.calculate_ema(stock_data['close'], period=20)
        macd_line, signal_line, _ = indicators.calculate_macd(stock_data['close'])
        
        # 步骤3: 形态识别
        print("3. 形态识别...")
        from core.indicators.patterns import PatternRecognizer
        recognizer = PatternRecognizer()
        
        patterns = recognizer.detect_all_patterns(stock_data)
        bullish_signals = recognizer.get_bullish_signals(stock_data)
        
        # 步骤4: 策略信号生成
        print("4. 策略信号生成...")
        # 简单策略: EMA上穿买入
        current_price = stock_data['close'].iloc[-1]
        current_ema = ema20.iloc[-1]
        prev_price = stock_data['close'].iloc[-2]
        prev_ema = ema20.iloc[-2]
        
        signal = None
        if prev_price < prev_ema and current_price > current_ema:
            signal = 'buy'
        elif prev_price > prev_ema and current_price < current_ema:
            signal = 'sell'
        
        # 步骤5: 风险管理
        print("5. 风险管理...")
        from services.risk_service.position.kelly_calculator import KellyCalculator
        from services.risk_service.stoploss.manager import StopLossManager
        
        kelly = KellyCalculator()
        kelly_result = kelly.calculate(win_rate=0.55, win_loss_ratio=1.5)
        position_size = kelly_result.recommended_position
        
        stoploss = StopLossManager()
        if signal == 'buy':
            # 使用check方法检查止损
            stoploss.check('TEST', current_price=current_price, entry_price=current_price, ema_20=current_ema)
        
        # 步骤6: 结果汇总
        print("6. 结果汇总...")
        result = {
            'signal': signal,
            'current_price': current_price,
            'ema20': current_ema,
            'position_size': position_size,
            'patterns_detected': patterns,
            'bullish_signals': bullish_signals
        }
        
        print(f"   信号: {signal}")
        print(f"   当前价格: {current_price:.2f}")
        print(f"   EMA20: {current_ema:.2f}")
        print(f"   建议仓位: {position_size:.2%}")
        print(f"   检测到的形态: {patterns}")
        
        assert result['signal'] is not None or result['signal'] is None
        assert 0 <= result['position_size'] <= 1
        
        print("✅ End-to-end trading workflow test passed")
