#!/usr/bin/env python3
"""
风控层单元测试

测试范围:
- KellyCalculator: 凯利公式计算器
- PositionManager: 仓位管理器
- StopLossManager: 止盈止损管理器
- CircuitBreaker: 熔断机制
"""
import pytest
import pandas as pd
import numpy as np
from unittest import mock
from datetime import datetime

# 被测模块
from services.risk_service.position.kelly_calculator import KellyCalculator
from services.risk_service.position.livermore_manager import LivermoreManager
from services.risk_service.stoploss.manager import StopLossManager
from services.risk_service.circuit_breaker.manager import CircuitBreaker


class TestKellyCalculator:
    """凯利公式计算器测试类"""
    
    def test_calculate_with_high_win_rate_and_good_ratio(self):
        """测试高胜率且盈亏比良好时 - 应返回较高仓位"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.6  # 60%胜率
        win_loss_ratio = 2.0  # 盈亏比2:1
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        # 凯利公式: f = (0.6*2 - 0.4)/2 = 0.4
        # 半凯利: 0.4 * 0.5 = 0.2 (20%)
        assert position > 0.15
        assert position <= 0.2  # 不超过20%
    
    def test_calculate_with_low_win_rate(self):
        """测试低胜率时 - 应返回较低仓位"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.4  # 40%胜率
        win_loss_ratio = 1.5
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        # 凯利公式: f = (0.4*1.5 - 0.6)/1.5 = 0
        # 应为0或很小的值
        assert position < 0.05
    
    def test_calculate_limits_max_position(self):
        """测试仓位上限限制 - 单票仓位不应超过20%"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.8  # 极高胜率
        win_loss_ratio = 3.0  # 极好盈亏比
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        # 即使条件极好，也不应超过20%
        assert position <= 0.2
    
    def test_calculate_with_zero_win_rate(self):
        """测试胜率为0时 - 应返回0"""
        # Arrange
        calculator = KellyCalculator()
        win_rate = 0.0
        win_loss_ratio = 2.0
        
        # Act
        position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        assert position == 0.0
    
    def test_calculate_with_invalid_input(self):
        """测试无效输入时 - 应抛出异常"""
        # Arrange
        calculator = KellyCalculator()
        
        # Act & Assert
        with pytest.raises(ValueError):
            calculator.calculate(win_rate=-0.1, win_loss_ratio=2.0)
        
        with pytest.raises(ValueError):
            calculator.calculate(win_rate=1.5, win_loss_ratio=2.0)
    
    def test_half_kelly_adjustment(self):
        """测试半凯利调整 - 应使用半凯利值"""
        # Arrange
        calculator = KellyCalculator(use_half_kelly=True)
        win_rate = 0.6
        win_loss_ratio = 2.0
        
        # Act
        half_position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Arrange
        calculator.use_half_kelly = False
        full_position = calculator.calculate(win_rate, win_loss_ratio)
        
        # Assert
        assert half_position == full_position * 0.5


class TestLivermoreManager:
    """利弗莫尔仓位管理测试类"""
    
    def test_initial_position_20_percent(self):
        """测试初始仓位20% - 应正确计算初始买入量"""
        # Arrange
        manager = LivermoreManager()
        total_capital = 1000000
        entry_price = 50.0
        
        # Act
        position = manager.calculate_initial_position(total_capital, entry_price)
        
        # Assert
        expected_value = total_capital * 0.2  # 20%
        expected_shares = int(expected_value / entry_price / 100) * 100  # 整手
        assert position['shares'] == expected_shares
        assert position['value'] == expected_shares * entry_price
    
    def test_add_position_on_10_percent_rise(self):
        """测试上涨10%加仓 - 应在价格上涨10%时触发加仓"""
        # Arrange
        manager = LivermoreManager()
        position = {
            'code': '000001',
            'entry_price': 50.0,
            'current_price': 55.0,  # 上涨10%
            'shares': 4000,
            'additions': 0
        }
        
        # Act
        should_add = manager.check_add_position(position)
        
        # Assert
        assert should_add == True
    
    def test_add_position_not_triggered_below_threshold(self):
        """测试未达涨幅时不加仓 - 应在价格涨幅<10%时不触发"""
        # Arrange
        manager = LivermoreManager()
        position = {
            'code': '000001',
            'entry_price': 50.0,
            'current_price': 54.0,  # 上涨8%，不足10%
            'shares': 4000,
            'additions': 0
        }
        
        # Act
        should_add = manager.check_add_position(position)
        
        # Assert
        assert should_add == False
    
    def test_close_all_on_10_percent_pullback(self):
        """测试最高价回落10%清仓 - 应在价格从高点回落10%时清仓"""
        # Arrange
        manager = LivermoreManager()
        position = {
            'code': '000001',
            'entry_price': 50.0,
            'highest_price': 60.0,  # 最高价60
            'current_price': 54.0,  # 从60回落到54，正好10%
            'shares': 4000
        }
        
        # Act
        should_close = manager.check_close_position(position)
        
        # Assert
        assert should_close == True
    
    def test_not_close_on_minor_pullback(self):
        """测试小幅回落不清仓 - 应在价格回落<10%时不触发"""
        # Arrange
        manager = LivermoreManager()
        position = {
            'code': '000001',
            'entry_price': 50.0,
            'highest_price': 60.0,
            'current_price': 56.0,  # 从60回落到56，约6.7%
            'shares': 4000
        }
        
        # Act
        should_close = manager.check_close_position(position)
        
        # Assert
        assert should_close == False
    
    def test_max_additions_limit(self):
        """测试最大加仓次数限制 - 不应超过3次加仓"""
        # Arrange
        manager = LivermoreManager()
        position = {
            'code': '000001',
            'entry_price': 50.0,
            'current_price': 66.55,  # 上涨33.1%，理论上应加仓3次
            'shares': 4000,
            'additions': 3  # 已加仓3次
        }
        
        # Act
        should_add = manager.check_add_position(position)
        
        # Assert
        assert should_add == False  # 不再加仓


class TestStopLossManager:
    """止盈止损管理器测试类"""
    
    def test_stoploss_triggered_at_ema20_down_3pct(self):
        """测试20日均线下3%触发止损 - 应在价格跌破EMA20*0.97时触发"""
        # Arrange
        manager = StopLossManager()
        position = {
            'code': '000001',
            'cost_price': 15.0,
            'current_price': 13.5,  # 下跌10%
            'ema_20': 14.0  # 20日均线
        }
        stoploss_price = position['ema_20'] * 0.97  # 13.58
        
        # Act
        should_stop = manager.check_stoploss(position)
        
        # Assert
        assert should_stop == True
        assert manager.stoploss_price == pytest.approx(stoploss_price, 0.01)
    
    def test_stoploss_not_triggered_above_threshold(self):
        """测试未达止损线时不触发 - 应在价格>EMA20*0.97时不触发"""
        # Arrange
        manager = StopLossManager()
        position = {
            'code': '000001',
            'cost_price': 15.0,
            'current_price': 14.0,  # 下跌6.7%
            'ema_20': 14.5  # 止损线=14.065
        }
        
        # Act
        should_stop = manager.check_stoploss(position)
        
        # Assert
        assert should_stop == False
    
    def test_take_profit_at_10_percent(self):
        """测试盈利10%减仓一半 - 应在盈利达到10%时触发减仓"""
        # Arrange
        manager = StopLossManager()
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 11.0,  # 盈利10%
            'quantity': 1000
        }
        
        # Act
        action = manager.check_take_profit(position)
        
        # Assert
        assert action is not None
        assert action['type'] == 'reduce_half'
        assert action['quantity'] == 500  # 减仓一半
        assert action['price'] == 11.0
    
    def test_take_profit_at_20_percent(self):
        """测试盈利20%清仓 - 应在盈利达到20%时触发清仓"""
        # Arrange
        manager = StopLossManager()
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 12.0,  # 盈利20%
            'quantity': 1000
        }
        
        # Act
        action = manager.check_take_profit(position)
        
        # Assert
        assert action is not None
        assert action['type'] == 'close_all'
        assert action['quantity'] == 1000  # 全部清仓
    
    def test_take_profit_not_triggered_below_10_percent(self):
        """测试盈利不足10%不触发 - 应在盈利<10%时不触发止盈"""
        # Arrange
        manager = StopLossManager()
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 10.8,  # 盈利8%，不足10%
            'quantity': 1000
        }
        
        # Act
        action = manager.check_take_profit(position)
        
        # Assert
        assert action is None
    
    def test_trailing_stoploss(self):
        """测试移动止损 - 应随价格上涨而上调止损线"""
        # Arrange
        manager = StopLossManager(use_trailing=True, trailing_pct=0.10)
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'highest_price': 13.0,  # 曾达到13，盈利30%
            'current_price': 11.7,  # 从高点回落10%
            'ema_20': 11.0
        }
        
        # Act
        should_stop = manager.check_stoploss(position)
        
        # Assert
        # 移动止损线 = 13.0 * 0.9 = 11.7
        assert should_stop == True
        assert manager.stoploss_price == pytest.approx(11.7, 0.01)
    
    def test_time_stoploss(self):
        """测试时间止损 - 应在持仓超过N天无盈利时触发"""
        # Arrange
        manager = StopLossManager(use_time_stop=True, max_hold_days=5)
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 10.1,  # 几乎无盈利
            'entry_date': datetime(2024, 1, 1),
            'ema_20': 10.0
        }
        current_date = datetime(2024, 1, 7)  # 持仓6天
        
        # Act
        should_stop = manager.check_time_stop(position, current_date)
        
        # Assert
        assert should_stop == True
        assert manager.stop_reason == 'time_stop'


class TestCircuitBreaker:
    """熔断机制测试类"""
    
    def test_pause_buy_when_market_drop_2pct(self):
        """测试大盘跌超2%暂停买入 - 应触发熔断暂停买入"""
        # Arrange
        breaker = CircuitBreaker()
        market_data = {
            'index_change_pct': -2.5,  # 下跌2.5%
            'index_code': '000001.SH'
        }
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True
        assert action['rule'] == 'market_drop_2pct'
        assert action['action'] == 'pause_buy'
        assert action['duration'] == '1d'
    
    def test_reduce_50pct_when_macd_death_cross(self):
        """测试MACD死叉减仓50% - 应触发减仓熔断"""
        # Arrange
        breaker = CircuitBreaker()
        market_data = {
            'macd_signal': 'death_cross',
            'index_code': '000001.SH'
        }
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True
        assert action['rule'] == 'macd_death_cross'
        assert action['action'] == 'reduce_50pct'
    
    def test_not_triggered_when_market_drop_below_2pct(self):
        """测试大盘跌幅<2%不熔断 - 应在跌幅<2%时不触发"""
        # Arrange
        breaker = CircuitBreaker()
        market_data = {
            'index_change_pct': -1.5,  # 下跌1.5%
            'index_code': '000001.SH'
        }
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == False
    
    def test_stop_loss_on_limit_down(self):
        """测试跌停板止损 - 应在股票跌停时触发止损"""
        # Arrange
        breaker = CircuitBreaker()
        stock_data = {
            'code': '000001',
            'current_price': 9.0,
            'limit_down_price': 9.0,  # 跌停价
            'is_limit_down': True
        }
        
        # Act
        action = breaker.check_stock(stock_data)
        
        # Assert
        assert action['triggered'] == True
        assert action['rule'] == 'limit_down'
        assert action['action'] == 'stop_loss'
    
    def test_multiple_rules_priority(self):
        """测试多规则优先级 - 应按优先级执行最高级规则"""
        # Arrange
        breaker = CircuitBreaker()
        market_data = {
            'index_change_pct': -5.0,  # 暴跌5%，触发最高级熔断
            'macd_signal': 'death_cross',
            'index_code': '000001.SH'
        }
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True
        # 暴跌5%应触发清仓，而非减仓50%
        assert action['action'] == 'close_all'
    
    def test_cooldown_period(self):
        """测试熔断冷却期 - 应在冷却期内不再触发"""
        # Arrange
        breaker = CircuitBreaker()
        
        # 第一次触发熔断
        market_data = {'index_change_pct': -3.0}
        action1 = breaker.check(market_data)
        assert action1['triggered'] == True
        
        # Act - 冷却期内再次检查
        action2 = breaker.check(market_data)
        
        # Assert
        assert action2['triggered'] == False
        assert action2['reason'] == 'in_cooldown'
    
    def test_reset_after_cooldown(self):
        """测试冷却期结束后重置 - 应在冷却期结束后可再次触发"""
        # Arrange
        breaker = CircuitBreaker(cooldown_minutes=5)
        
        # 触发熔断
        market_data = {'index_change_pct': -3.0}
        breaker.check(market_data)
        
        # 模拟冷却期结束
        breaker.last_triggered = datetime.now() - pd.Timedelta(minutes=6)
        
        # Act
        action = breaker.check(market_data)
        
        # Assert
        assert action['triggered'] == True  # 可再次触发


class TestRiskIntegration:
    """风控集成测试类"""
    
    def test_full_risk_check_pipeline(self):
        """测试完整风控检查流程"""
        # Arrange
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 11.5,  # 盈利15%
            'quantity': 1000,
            'ema_20': 10.5,
            'highest_price': 12.0
        }
        market_data = {
            'index_change_pct': -1.0,  # 市场正常
            'macd_signal': 'golden_cross'
        }
        
        stoploss_mgr = StopLossManager()
        circuit_breaker = CircuitBreaker()
        
        # Act
        # 1. 检查止盈
        take_profit_action = stoploss_mgr.check_take_profit(position)
        
        # 2. 检查止损
        stoploss_triggered = stoploss_mgr.check_stoploss(position)
        
        # 3. 检查熔断
        circuit_action = circuit_breaker.check(market_data)
        
        # Assert
        # 盈利15%，应触发10%减仓
        assert take_profit_action is not None
        assert take_profit_action['type'] == 'reduce_half'
        
        # 未触发止损
        assert stoploss_triggered == False
        
        # 未触发熔断
        assert circuit_action['triggered'] == False
    
    def test_emergency_liquidation_scenario(self):
        """测试紧急清仓场景 - 多风险同时触发"""
        # Arrange
        position = {
            'code': '000001',
            'cost_price': 10.0,
            'current_price': 8.0,  # 亏损20%
            'quantity': 1000,
            'ema_20': 9.0,
            'highest_price': 12.0
        }
        market_data = {
            'index_change_pct': -5.0,  # 市场暴跌
            'macd_signal': 'death_cross'
        }
        
        stoploss_mgr = StopLossManager()
        circuit_breaker = CircuitBreaker()
        
        # Act
        stoploss_triggered = stoploss_mgr.check_stoploss(position)
        circuit_action = circuit_breaker.check(market_data)
        
        # Assert
        # 价格8.0 < EMA20*0.97=8.73，触发止损
        assert stoploss_triggered == True
        
        # 市场暴跌5%，触发熔断清仓
        assert circuit_action['triggered'] == True
        assert circuit_action['action'] == 'close_all'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
