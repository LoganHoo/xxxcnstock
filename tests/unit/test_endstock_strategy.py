#!/usr/bin/env python3
"""
尾盘选股策略测试
"""
import pytest
import pandas as pd


class TestEndstockPickStrategy:
    """尾盘选股策略测试"""
    
    def test_strategy_creation(self):
        """测试策略创建"""
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy = EndstockPickStrategy()
        assert strategy is not None
    
    def test_endstock_screening(self):
        """测试尾盘选股筛选"""
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy = EndstockPickStrategy()
        
        market_data = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'price_change': [4.0, 6.0, 2.0, 3.5],  # 涨幅
            'volume_ratio': [2.0, 6.0, 1.5, 3.0],  # 量比
            'market_cap': [100, 300, 30, 150],     # 市值(亿)
            'above_ma': [True, True, False, True]  # 是否在均线上方
        })
        
        result = strategy.screen(market_data)
        
        # 应该选出涨幅3-5%，量比1-5，市值50-200亿，且在均线上方的股票
        assert len(result) == 2
        assert '000001' in result['code'].values
        assert '000004' in result['code'].values
    
    def test_execute_before_time(self):
        """测试非尾盘时间不执行"""
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy = EndstockPickStrategy()
        
        market_data = pd.DataFrame({
            'code': ['000001'],
            'price_change': [4.0],
            'volume_ratio': [2.0],
            'market_cap': [100],
            'above_ma': [True]
        })
        
        # 14:00执行，不应返回结果
        result = strategy.execute(market_data, '14:00')
        assert len(result) == 0
    
    def test_execute_after_time(self):
        """测试尾盘时间执行"""
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy = EndstockPickStrategy()
        
        market_data = pd.DataFrame({
            'code': ['000001'],
            'price_change': [4.0],
            'volume_ratio': [2.0],
            'market_cap': [100],
            'above_ma': [True]
        })
        
        # 14:30后执行
        result = strategy.execute(market_data, '14:35')
        assert len(result) == 1
        assert result[0]['code'] == '000001'
        assert result[0]['signal_type'] == 'endstock_pick'
    
    def test_rank_stocks(self):
        """测试股票排序"""
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy = EndstockPickStrategy()
        
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'price_change': [3.0, 4.0, 5.0],
            'volume_ratio': [2.5, 2.0, 4.0],
            'market_cap': [100, 150, 80]
        })
        
        ranked = strategy.rank_stocks(stocks)
        
        assert 'total_score' in ranked.columns
        assert len(ranked) == 3
