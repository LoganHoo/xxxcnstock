#!/usr/bin/env python3
"""
龙回头策略测试
"""
import pytest
import pandas as pd


class TestDragonHeadStrategy:
    """龙回头策略测试"""
    
    def test_strategy_creation(self):
        """测试策略创建"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        assert strategy is not None
    
    def test_height_board_detection(self):
        """测试高度板检测"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        # 是高度板
        stock_data = {
            'code': '000001',
            'consecutive_limitup': 5,
            'market_position': 'leader',
            'market_cap': 100
        }
        
        is_height_board = strategy.is_height_board(stock_data)
        assert is_height_board == True
    
    def test_not_height_board(self):
        """测试非高度板"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        # 不是高度板(连板不够)
        stock_data = {
            'code': '000002',
            'consecutive_limitup': 2,
            'market_position': 'leader',
            'market_cap': 100
        }
        
        is_height_board = strategy.is_height_board(stock_data)
        assert is_height_board == False
    
    def test_pullback_buy_signal(self):
        """测试回调买入信号"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        # 模拟涨停后回调10%的价格数据
        price_data = pd.DataFrame({
            'close': [100, 110, 110, 105, 102, 101],  # 从110回调到101(约8%)
            'volume': [1000, 2000, 1500, 1200, 1000, 1100]
        })
        
        is_pullback = strategy.is_pullback_buy(price_data)
        assert is_pullback == True
    
    def test_excessive_pullback(self):
        """测试回调过大"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        # 回调20%，超过15%阈值
        price_data = pd.DataFrame({
            'close': [100, 110, 110, 100, 92, 88],
            'volume': [1000, 2000, 1500, 1200, 1000, 1100]
        })
        
        is_pullback = strategy.is_pullback_buy(price_data)
        assert is_pullback == False
    
    def test_find_opportunities(self):
        """测试寻找机会"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        market_data = [
            {
                'code': '000001',
                'consecutive_limitup': 5,
                'market_position': 'leader',
                'market_cap': 100
            },
            {
                'code': '000002',
                'consecutive_limitup': 2,
                'market_position': 'follower',
                'market_cap': 50
            }
        ]
        
        # 模拟价格历史
        price_history = {
            '000001': pd.DataFrame({
                'close': [100, 110, 110, 105, 102, 101],
                'volume': [1000, 2000, 1500, 1200, 1000, 1100]
            })
        }
        
        opportunities = strategy.find_dragon_head_opportunities(market_data, price_history)
        
        assert len(opportunities) == 1
        assert opportunities[0]['code'] == '000001'
    
    def test_calculate_buy_zone(self):
        """测试计算买入区间"""
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy = DragonHeadStrategy()
        
        result = strategy.calculate_buy_zone(high_price=110, current_price=100)
        
        assert result['high_price'] == 110
        assert result['current_price'] == 100
        assert 'buy_zone_low' in result
        assert 'buy_zone_high' in result
