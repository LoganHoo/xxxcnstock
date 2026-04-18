#!/usr/bin/env python3
"""
实现验证测试

验证已实现的组件可以正常工作
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestCoreIndicators:
    """核心指标测试"""
    
    def test_ema_calculation(self):
        """测试EMA计算"""
        from core.indicators.trend import calculate_ema
        
        prices = pd.Series([10, 11, 12, 11, 13, 14, 15])
        ema = calculate_ema(prices, period=5)
        
        assert len(ema) == len(prices)
        assert not ema.isna().all()
        assert ema.iloc[-1] > 0
    
    def test_macd_calculation(self):
        """测试MACD计算"""
        from core.indicators.trend import calculate_macd
        
        prices = pd.Series([10 + i * 0.1 for i in range(50)])
        macd_line, signal_line, histogram = calculate_macd(prices)
        
        assert len(macd_line) == len(prices)
        assert len(signal_line) == len(prices)
        assert len(histogram) == len(prices)
    
    def test_macd_golden_cross_detection(self):
        """测试MACD金叉检测"""
        from core.indicators.trend import detect_macd_cross
        
        # 模拟金叉: 前一天MACD<信号线，今天MACD>信号线
        macd_line = pd.Series([0.5, 0.6])
        signal_line = pd.Series([0.6, 0.5])
        
        result = detect_macd_cross(macd_line, signal_line, 'golden')
        assert result == True
    
    def test_macd_death_cross_detection(self):
        """测试MACD死叉检测"""
        from core.indicators.trend import detect_macd_cross
        
        # 模拟死叉: 前一天MACD>信号线，今天MACD<信号线
        macd_line = pd.Series([0.6, 0.5])
        signal_line = pd.Series([0.5, 0.6])
        
        result = detect_macd_cross(macd_line, signal_line, 'death')
        assert result == True


class TestDataSource:
    """数据源测试"""
    
    def test_base_provider_is_abstract(self):
        """测试基类是抽象的"""
        from services.data_service.datasource.base import DataSourceProvider
        
        with pytest.raises(TypeError):
            DataSourceProvider()
    
    def test_tushare_provider_initialization(self):
        """测试Tushare提供者初始化"""
        from services.data_service.datasource.tushare_provider import TushareProvider
        
        # 应该需要token
        with pytest.raises(ValueError):
            TushareProvider()
    
    def test_data_validator_creation(self):
        """测试数据验证器创建"""
        from services.data_service.quality.validator import DataValidator, ValidationResult
        
        validator = DataValidator()
        assert validator is not None
    
    def test_validate_price_with_normal_data(self):
        """测试正常价格数据验证"""
        from services.data_service.quality.validator import DataValidator
        
        df = pd.DataFrame({
            'open': [10.0, 10.5, 11.0],
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        result = validator.validate_price(df)
        
        assert result.is_valid == True
    
    def test_validate_price_with_negative_value(self):
        """测试负价格数据验证"""
        from services.data_service.quality.validator import DataValidator
        
        df = pd.DataFrame({
            'open': [10.0, -5.0, 11.0],
            'high': [10.8, 11.0, 11.5],
            'low': [9.8, 10.2, 10.8],
            'close': [10.5, 10.8, 11.2]
        })
        validator = DataValidator()
        result = validator.validate_price(df)
        
        assert result.is_valid == False
        assert len(result.errors) > 0


class TestStrategy:
    """策略测试"""
    
    def test_limitup_callback_strategy_creation(self):
        """测试涨停回调策略创建"""
        from services.strategy_service.limitup_callback.strategy import LimitupCallbackStrategy
        
        strategy = LimitupCallbackStrategy()
        assert strategy is not None
    
    def test_step1_filter_excludes_high_limitup_stocks(self):
        """测试第一步筛选排除高连板股票"""
        from services.strategy_service.limitup_callback.strategy import LimitupCallbackStrategy
        
        market_data = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'limitup_days': [2, 4, 1],
            'turnover': [15, 10, 12],
            'roe': [15, 12, 10]
        })
        
        strategy = LimitupCallbackStrategy()
        filtered = strategy.step1_filter(market_data)
        
        assert len(filtered) == 2
        assert '000002' not in filtered['code'].values
    
    def test_step1_filter_excludes_high_turnover_stocks(self):
        """测试第一步筛选排除高换手率股票"""
        from services.strategy_service.limitup_callback.strategy import LimitupCallbackStrategy
        
        market_data = pd.DataFrame({
            'code': ['000001', '000002'],
            'limitup_days': [2, 2],
            'turnover': [15, 25],
            'roe': [15, 12]
        })
        
        strategy = LimitupCallbackStrategy()
        filtered = strategy.step1_filter(market_data)
        
        assert len(filtered) == 1
        assert '000002' not in filtered['code'].values
    
    def test_buy_signal_creation(self):
        """测试买入信号创建"""
        from services.strategy_service.limitup_callback.signals import BuySignal
        
        signal = BuySignal(
            code='000001',
            name='Test Stock',
            trigger_price=10.5,
            stoploss_price=9.5,
            confidence=0.8
        )
        
        assert signal.code == '000001'
        assert signal.trigger_price == 10.5
        assert signal.timestamp is not None


class TestRisk:
    """风控测试"""
    
    def test_kelly_calculator_creation(self):
        """测试凯利公式计算器创建"""
        from services.risk_service.position.kelly_calculator import KellyCalculator
        
        calculator = KellyCalculator()
        assert calculator is not None
    
    def test_kelly_calculate_with_high_win_rate(self):
        """测试高胜率下的凯利计算"""
        from services.risk_service.position.kelly_calculator import KellyCalculator
        
        calculator = KellyCalculator()
        result = calculator.calculate(win_rate=0.6, win_loss_ratio=2.0)
        
        assert result.kelly_fraction > 0
        assert result.recommended_position > 0
        assert result.recommended_position <= 0.2  # 最大20%
    
    def test_kelly_calculate_with_low_win_rate(self):
        """测试低胜率下的凯利计算"""
        from services.risk_service.position.kelly_calculator import KellyCalculator
        
        calculator = KellyCalculator()
        result = calculator.calculate(win_rate=0.3, win_loss_ratio=1.0)
        
        # 低胜率应该建议不交易
        assert result.recommended_position == 0
    
    def test_livermore_manager_creation(self):
        """测试利弗莫尔管理器创建"""
        from services.risk_service.position.livermore_manager import LivermoreManager
        
        manager = LivermoreManager()
        assert manager is not None
    
    def test_livermore_should_buy_initial(self):
        """测试利弗莫尔初始仓位判断"""
        from services.risk_service.position.livermore_manager import LivermoreManager
        
        manager = LivermoreManager()
        result = manager.should_buy_initial('000001', capital=100000)
        
        assert result['should_buy'] == True
        assert result['amount'] == 20000  # 20% of 100000
    
    def test_stoploss_manager_creation(self):
        """测试止盈止损管理器创建"""
        from services.risk_service.stoploss.manager import StopLossManager
        
        manager = StopLossManager()
        assert manager is not None
    
    def test_stoploss_check_take_profit_20_percent(self):
        """测试20%止盈检查"""
        from services.risk_service.stoploss.manager import StopLossManager, ActionType
        
        manager = StopLossManager()
        result = manager.check(
            code='000001',
            current_price=12.0,
            entry_price=10.0,
            ema_20=11.0
        )
        
        assert result.action == ActionType.TAKE_PROFIT_2
    
    def test_circuit_breaker_creation(self):
        """测试熔断器创建"""
        from services.risk_service.circuit_breaker.manager import CircuitBreaker
        
        breaker = CircuitBreaker()
        assert breaker is not None
    
    def test_circuit_breaker_market_drop_2_percent(self):
        """测试大盘跌2%熔断"""
        from services.risk_service.circuit_breaker.manager import CircuitBreaker, CircuitAction
        
        breaker = CircuitBreaker()
        result = breaker.check_market({'index_change_pct': -0.025})
        
        assert result.triggered == True
        assert result.action == CircuitAction.PAUSE_BUY
    
    def test_circuit_breaker_market_drop_5_percent(self):
        """测试大盘跌5%熔断"""
        from services.risk_service.circuit_breaker.manager import CircuitBreaker, CircuitAction
        
        breaker = CircuitBreaker()
        result = breaker.check_market({'index_change_pct': -0.055})
        
        assert result.triggered == True
        assert result.action == CircuitAction.CLOSE_ALL


class TestIntegration:
    """集成测试"""
    
    def test_full_pipeline_imports(self):
        """测试完整流程导入"""
        # 数据源
        from services.data_service.datasource.base import DataSourceProvider
        from services.data_service.datasource.tushare_provider import TushareProvider
        from services.data_service.datasource.manager import DataSourceManager
        from services.data_service.quality.validator import DataValidator
        
        # 指标
        from core.indicators.trend import calculate_ema, calculate_macd
        
        # 策略
        from services.strategy_service.limitup_callback.strategy import LimitupCallbackStrategy
        from services.strategy_service.limitup_callback.signals import BuySignal
        
        # 风控
        from services.risk_service.position.kelly_calculator import KellyCalculator
        from services.risk_service.position.livermore_manager import LivermoreManager
        from services.risk_service.stoploss.manager import StopLossManager
        from services.risk_service.circuit_breaker.manager import CircuitBreaker
        
        assert True  # 所有导入成功


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
