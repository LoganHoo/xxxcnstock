#!/usr/bin/env python3
"""
策略层单元测试

测试范围:
- LimitupCallbackStrategy: 涨停回调战法
- EndstockPickStrategy: 尾盘选股
- DragonHeadStrategy: 龙回头策略
"""
import pytest
import pandas as pd
import numpy as np
from unittest import mock
from datetime import datetime, time

# 被测模块
from services.strategy_service.limitup_callback.strategy import LimitupCallbackStrategy, BuySignal
from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
from services.strategy_service.dragon_head.strategy import DragonHeadStrategy


class TestLimitupCallbackStrategy:
    """涨停回调战法策略测试类"""
    
    def test_step1_filter_excludes_high_limitup_stocks(self):
        """测试Step1筛除三连板以上股票 - 应排除limitup_days>3的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'name': ['Stock A', 'Stock B', 'Stock C', 'Stock D'],
            'limitup_days': [2, 4, 1, 3],  # 000002是4连板，应被排除
            'turnover': [15, 18, 12, 19],
            'roe': [15, 12, 10, 14]
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step1_filter(stocks)
        
        # Assert
        assert len(result) == 3
        assert '000002' not in result['code'].values
        assert set(result['code'].values) == {'000001', '000003', '000004'}
    
    def test_step1_filter_excludes_high_turnover_stocks(self):
        """测试Step1筛除高换手率股票 - 应排除turnover>20%的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'limitup_days': [1, 1, 2],
            'turnover': [15, 25, 18],  # 000002换手率25%>20%
            'roe': [15, 12, 14]
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step1_filter(stocks)
        
        # Assert
        assert len(result) == 2
        assert '000002' not in result['code'].values
    
    def test_step1_filter_excludes_loss_making_stocks(self):
        """测试Step1筛除业绩亏损股 - 应排除roe<=0的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'limitup_days': [1, 1, 1],
            'turnover': [15, 12, 18],
            'roe': [15, -5, 0]  # 000002亏损，000003 ROE=0
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step1_filter(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_step2_confirm_macd_golden_cross(self):
        """测试Step2确认月线MACD金叉 - 应保留macd_monthly为golden_cross的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'macd_monthly': ['golden_cross', 'death_cross', 'flat'],
            'close': [15, 20, 25],
            'ema_60_monthly': [14, 22, 20]  # 000002股价<60月线
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step2_confirm(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_step2_confirm_price_above_ema60(self):
        """测试Step2确认股价>60月线 - 应排除close<=ema_60_monthly的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'macd_monthly': ['golden_cross', 'golden_cross', 'golden_cross'],
            'close': [15, 18, 20],
            'ema_60_monthly': [14, 20, 25]  # 000002股价=60月线，000003股价<60月线
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        result = strategy.step2_confirm(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_step3_timing_at_ema20_with_volume_surge(self):
        """测试Step3在20日均线且放量时触发 - 应生成买入信号"""
        # Arrange
        stocks = [
            {
                'code': '000001',
                'name': 'Stock A',
                'close': 15.0,
                'open': 14.5,
                'ema_20': 15.0,
                'volume': 150000,
                'volume_20_avg': 100000  # 放量50%
            }
        ]
        strategy = LimitupCallbackStrategy()
        
        # Act
        signals = strategy.step3_timing(stocks)
        
        # Assert
        assert len(signals) == 1
        assert isinstance(signals[0], BuySignal)
        assert signals[0].code == '000001'
        assert signals[0].trigger_price == 15.0
    
    def test_step3_timing_not_triggered_when_price_far_from_ema20(self):
        """测试Step3股价远离20日均线时不触发"""
        # Arrange
        stocks = [
            {
                'code': '000001',
                'close': 18.0,  # 远离20日均线
                'open': 17.5,
                'ema_20': 15.0,
                'volume': 150000,
                'volume_20_avg': 100000
            }
        ]
        strategy = LimitupCallbackStrategy()
        
        # Act
        signals = strategy.step3_timing(stocks)
        
        # Assert
        assert len(signals) == 0
    
    def test_step3_timing_not_triggered_without_volume_surge(self):
        """测试Step3未放量时不触发"""
        # Arrange
        stocks = [
            {
                'code': '000001',
                'close': 15.0,
                'open': 14.5,
                'ema_20': 15.0,
                'volume': 105000,  # 未明显放量
                'volume_20_avg': 100000
            }
        ]
        strategy = LimitupCallbackStrategy()
        
        # Act
        signals = strategy.step3_timing(stocks)
        
        # Assert
        assert len(signals) == 0
    
    def test_step3_timing_not_triggered_on_red_candle(self):
        """测试Step3阴线时不触发"""
        # Arrange
        stocks = [
            {
                'code': '000001',
                'close': 14.5,  # 收盘价<开盘价，阴线
                'open': 15.0,
                'ema_20': 15.0,
                'volume': 150000,
                'volume_20_avg': 100000
            }
        ]
        strategy = LimitupCallbackStrategy()
        
        # Act
        signals = strategy.step3_timing(stocks)
        
        # Assert
        assert len(signals) == 0
    
    def test_full_execute_pipeline(self):
        """测试完整执行流程 - 应返回有效买入信号"""
        # Arrange
        market_data = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'name': ['Stock A', 'Stock B', 'Stock C', 'Stock D'],
            'limitup_days': [2, 4, 1, 2],
            'turnover': [15, 18, 12, 16],
            'roe': [15, 12, 10, 14],
            'macd_monthly': ['golden_cross', 'golden_cross', 'death_cross', 'golden_cross'],
            'close': [15.0, 20.0, 25.0, 18.0],
            'open': [14.5, 19.5, 24.5, 17.5],
            'ema_20': [15.0, 19.0, 24.0, 17.0],
            'ema_60_monthly': [14.0, 22.0, 20.0, 16.0],
            'volume': [150000, 180000, 120000, 160000],
            'volume_20_avg': [100000, 150000, 100000, 150000]
        })
        strategy = LimitupCallbackStrategy()
        
        # Act
        signals = strategy.execute(market_data)
        
        # Assert
        assert isinstance(signals, list)
        # 只有000001满足所有条件
        assert len(signals) == 1
        assert signals[0].code == '000001'
    
    def test_calculate_position_size(self):
        """测试仓位计算 - 应返回合理仓位比例"""
        # Arrange
        strategy = LimitupCallbackStrategy()
        signal = BuySignal(
            code='000001',
            confidence=0.8,
            market_condition='bullish'
        )
        
        # Act
        position_size = strategy.calculate_position_size(signal, total_capital=1000000)
        
        # Assert
        assert position_size > 0
        assert position_size <= 200000  # 单票最多20%
    
    def test_calculate_stoploss_price(self):
        """测试止损价计算 - 应为20日均线下3%"""
        # Arrange
        strategy = LimitupCallbackStrategy()
        stock_data = {
            'ema_20': 15.0
        }
        
        # Act
        stoploss = strategy.calculate_stoploss(stock_data)
        
        # Assert
        assert stoploss == 15.0 * 0.97  # 20日均线下3%
    
    def test_calculate_take_profit_levels(self):
        """测试止盈价计算 - 应返回10%和20%两档"""
        # Arrange
        strategy = LimitupCallbackStrategy()
        entry_price = 15.0
        
        # Act
        take_profits = strategy.calculate_take_profit(entry_price)
        
        # Assert
        assert len(take_profits) == 2
        assert take_profits[0]['level'] == 1
        assert take_profits[0]['price'] == entry_price * 1.10  # 10%
        assert take_profits[1]['level'] == 2
        assert take_profits[1]['price'] == entry_price * 1.20  # 20%


class TestEndstockPickStrategy:
    """尾盘选股策略测试类"""
    
    def test_screen_time_at_1430(self):
        """测试14:30后开始筛选 - 应在正确时间触发"""
        # Arrange
        strategy = EndstockPickStrategy()
        
        # Act & Assert
        assert strategy.should_run_at(time(14, 30)) == True
        assert strategy.should_run_at(time(14, 29)) == False
        assert strategy.should_run_at(time(15, 0)) == True
    
    def test_filter_by_price_change_range(self):
        """测试涨幅范围筛选 - 应筛选3%<涨幅<5%的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'price_change_pct': [2.5, 4.0, 6.0, 3.5]  # 000001<3%, 000003>5%
        })
        strategy = EndstockPickStrategy()
        
        # Act
        result = strategy.filter_by_price_change(stocks, min_pct=3, max_pct=5)
        
        # Assert
        assert len(result) == 2
        assert set(result['code'].values) == {'000002', '000004'}
    
    def test_filter_by_volume_ratio(self):
        """测试量比筛选 - 应筛选1<量比<5的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'volume_ratio': [0.8, 2.5, 6.0, 3.0]  # 000001<1, 000003>5
        })
        strategy = EndstockPickStrategy()
        
        # Act
        result = strategy.filter_by_volume_ratio(stocks, min_ratio=1, max_ratio=5)
        
        # Assert
        assert len(result) == 2
        assert set(result['code'].values) == {'000002', '000004'}
    
    def test_filter_by_market_cap(self):
        """测试市值筛选 - 应筛选50亿<市值<200亿的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'market_cap': [30, 80, 250, 150]  # 000001<50亿, 000003>200亿
        })
        strategy = EndstockPickStrategy()
        
        # Act
        result = strategy.filter_by_market_cap(stocks, min_cap=50, max_cap=200)
        
        # Assert
        assert len(result) == 2
        assert set(result['code'].values) == {'000002', '000004'}
    
    def test_filter_above_ma_line(self):
        """测试股价在分时均线之上筛选"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002'],
            'close': [15.0, 12.0],
            'intraday_ma': [14.5, 12.5]  # 000002股价<均线
        })
        strategy = EndstockPickStrategy()
        
        # Act
        result = strategy.filter_above_ma(stocks)
        
        # Assert
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'
    
    def test_full_screen_pipeline(self):
        """测试完整筛选流程"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004', '000005'],
            'price_change_pct': [4.0, 2.0, 4.5, 3.5, 5.5],
            'volume_ratio': [2.5, 3.0, 0.8, 3.5, 2.0],
            'market_cap': [80, 60, 100, 30, 150],
            'close': [15.0, 12.0, 18.0, 20.0, 25.0],
            'intraday_ma': [14.5, 12.5, 17.5, 20.5, 24.0]
        })
        strategy = EndstockPickStrategy()
        
        # Act
        result = strategy.execute(stocks)
        
        # Assert
        # 只有000001满足所有条件:
        # - 涨幅3-5%: 4.0 ✓
        # - 量比1-5: 2.5 ✓
        # - 市值50-200亿: 80 ✓
        # - 股价>均线: 15.0 > 14.5 ✓
        assert len(result) == 1
        assert result['code'].iloc[0] == '000001'


class TestDragonHeadStrategy:
    """龙回头策略测试类"""
    
    def test_identify_height_limitup_stocks(self):
        """测试识别高度板股票 - 应识别连续涨停的股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'consecutive_limitup': [5, 2, 8],  # 000001和000003是高度板
            'is_limitup': [True, True, True]
        })
        strategy = DragonHeadStrategy()
        
        # Act
        height_stocks = strategy.identify_height_stocks(stocks, min_consecutive=3)
        
        # Assert
        assert len(height_stocks) == 2
        assert '000002' not in height_stocks['code'].values
    
    def test_identify_compensation_limitup_stocks(self):
        """测试识别补涨板股票"""
        # Arrange
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'sector': ['AI', 'AI', 'AI'],
            'is_limitup': [True, True, False],
            'sector_leader': [True, False, False],  # 000001是板块龙头
            'limitup_order': [1, 3, 0]  # 涨停顺序
        })
        strategy = DragonHeadStrategy()
        
        # Act
        compensation_stocks = strategy.identify_compensation_stocks(stocks)
        
        # Assert
        assert len(compensation_stocks) == 1
        assert compensation_stocks['code'].iloc[0] == '000002'
    
    def test_detect_dragon_head_pullback(self):
        """测试检测龙回头回踩 - 应识别龙头股的回调买入点"""
        # Arrange
        stock_history = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10),
            'close': [10, 11, 12, 13, 14, 13.5, 13, 12.5, 13, 13.5],  # 上涨后回调
            'high': [10.5, 11.5, 12.5, 13.5, 14.5, 14, 13.5, 13, 13.5, 14],
            'volume': [10000, 12000, 15000, 18000, 20000, 15000, 12000, 10000, 13000, 15000]
        })
        strategy = DragonHeadStrategy()
        
        # Act
        is_pullback = strategy.detect_pullback(stock_history, pullback_pct=15)
        
        # Assert
        assert is_pullback == True
    
    def test_calculate_pullback_buy_point(self):
        """测试计算回踩买入点"""
        # Arrange
        stock_data = {
            'code': '000001',
            'high_price': 15.0,  # 近期高点
            'current_price': 12.75,  # 当前价格(下跌15%)
            'ema_20': 13.0,
            'volume': 150000,
            'volume_avg': 100000
        }
        strategy = DragonHeadStrategy()
        
        # Act
        buy_point = strategy.calculate_buy_point(stock_data, pullback_pct=15)
        
        # Assert
        assert buy_point is not None
        assert buy_point['trigger_price'] <= stock_data['high_price'] * 0.85
        assert buy_point['stoploss'] == stock_data['ema_20'] * 0.97
    
    def test_not_buy_when_pullback_too_deep(self):
        """测试回调过深时不买入 - 应放弃信号"""
        # Arrange
        stock_data = {
            'code': '000001',
            'high_price': 15.0,
            'current_price': 11.25,  # 下跌25%，超过20%阈值
            'ema_20': 13.0
        }
        strategy = DragonHeadStrategy()
        
        # Act
        buy_point = strategy.calculate_buy_point(stock_data, max_pullback=20)
        
        # Assert
        assert buy_point is None  # 回调过深，放弃


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
