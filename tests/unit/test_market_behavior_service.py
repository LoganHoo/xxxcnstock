#!/usr/bin/env python3
"""
市场行为数据服务单元测试

测试内容:
- 龙虎榜数据获取器
- 资金流向数据获取器
- 北向资金数据获取器
- 数据验证与处理
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch


class TestDragonTigerFetcher:
    """龙虎榜数据获取器测试"""
    
    @pytest.fixture
    def mock_akshare(self):
        """模拟AKShare响应"""
        mock = Mock()
        mock.return_value = pd.DataFrame({
            '序号': [1, 2],
            '代码': ['000001', '000002'],
            '名称': ['平安银行', '万科A'],
            '收盘价': [10.5, 15.2],
            '涨跌幅': [10.02, 9.98],
            '龙虎榜成交额': [50000000, 30000000],
            '买入额': [30000000, 18000000],
            '卖出额': [20000000, 12000000],
            '净额': [10000000, 6000000],
        })
        return mock
    
    def test_fetch_dragon_tiger_success(self, mock_akshare):
        """测试成功获取龙虎榜数据"""
        from services.data_service.fetchers.market_behavior.dragon_tiger_fetcher import DragonTigerFetcher
        
        fetcher = DragonTigerFetcher()
        
        with patch('akshare.stock_lhb_detail_daily_sina', mock_akshare):
            df = fetcher.fetch_dragon_tiger_list('20240419')
            
            assert not df.empty
            assert 'code' in df.columns
            assert 'close_price' in df.columns
    
    def test_fetch_dragon_tiger_empty_response(self):
        """测试空响应处理"""
        from services.data_service.fetchers.market_behavior.dragon_tiger_fetcher import DragonTigerFetcher
        
        fetcher = DragonTigerFetcher()
        
        with patch('akshare.stock_lhb_detail_daily_sina', return_value=pd.DataFrame()):
            df = fetcher.fetch_dragon_tiger_list('20240419')
            
            assert df.empty
    
    def test_fetch_institution_trading(self):
        """测试获取机构交易数据"""
        from services.data_service.fetchers.market_behavior.dragon_tiger_fetcher import DragonTigerFetcher
        
        fetcher = DragonTigerFetcher()
        
        mock_data = pd.DataFrame({
            '代码': ['000001'],
            '名称': ['平安银行'],
            '机构名称': ['机构专用'],
            '买入金额': [5000000],
            '卖出金额': [1000000],
        })
        
        with patch('akshare.stock_lhb_detail_institution_sina', return_value=mock_data):
            df = fetcher.fetch_institution_trading('20240419', '20240419')
            
            assert not df.empty
            assert 'institution_buy' in df.columns or '买入金额' in df.columns
    
    def test_calculate_institution_net(self):
        """测试计算机构净买入"""
        from services.data_service.fetchers.market_behavior.dragon_tiger_fetcher import DragonTigerFetcher
        
        fetcher = DragonTigerFetcher()
        
        data = pd.DataFrame({
            'code': ['000001', '000001', '000002'],
            'institution_buy': [5000000, 3000000, 2000000],
            'institution_sell': [1000000, 500000, 800000]
        })
        
        result = fetcher._calculate_institution_net(data)
        
        # 000001: (5000000 + 3000000) - (1000000 + 500000) = 6500000
        assert result[result['code'] == '000001']['institution_net'].iloc[0] == 6500000


class TestMoneyFlowFetcher:
    """资金流向数据获取器测试"""
    
    @pytest.fixture
    def mock_akshare(self):
        """模拟AKShare响应"""
        mock = Mock()
        mock.return_value = pd.DataFrame({
            '代码': ['000001', '000002'],
            '名称': ['平安银行', '万科A'],
            '最新价': [10.5, 15.2],
            '涨跌幅': [2.5, 1.8],
            '主力净流入': [5000000, -3000000],
            '小单净流入': [1000000, 500000],
            '中单净流入': [500000, -200000],
            '大单净流入': [3000000, -1500000],
            '超大单净流入': [2000000, -1500000],
        })
        return mock
    
    def test_fetch_money_flow_success(self, mock_akshare):
        """测试成功获取资金流向数据"""
        from services.data_service.fetchers.market_behavior.money_flow_fetcher import MoneyFlowFetcher
        
        fetcher = MoneyFlowFetcher()
        
        with patch('akshare.stock_money_flow_individual', mock_akshare):
            df = fetcher.fetch_money_flow('000001')
            
            assert not df.empty
            assert 'main_net_inflow' in df.columns or '主力净流入' in df.columns
    
    def test_fetch_sector_money_flow(self):
        """测试获取板块资金流向"""
        from services.data_service.fetchers.market_behavior.money_flow_fetcher import MoneyFlowFetcher
        
        fetcher = MoneyFlowFetcher()
        
        mock_data = pd.DataFrame({
            '板块': ['银行', '房地产'],
            '主力净流入': [100000000, -50000000],
            '主力净流入占比': [5.2, -3.1],
        })
        
        with patch('akshare.stock_sector_money_flow', return_value=mock_data):
            df = fetcher.fetch_sector_money_flow()
            
            assert not df.empty
    
    def test_calculate_net_inflow_ratio(self):
        """测试计算净流入占比"""
        from services.data_service.fetchers.market_behavior.money_flow_fetcher import MoneyFlowFetcher
        
        fetcher = MoneyFlowFetcher()
        
        data = pd.DataFrame({
            'main_net_inflow': [5000000],
            'turnover': [100000000]
        })
        
        ratio = fetcher._calculate_net_inflow_ratio(data)
        
        # 净流入占比 = 净流入 / 成交额 * 100
        expected_ratio = 5000000 / 100000000 * 100
        assert abs(ratio.iloc[0] - expected_ratio) < 0.01


class TestMarketBehaviorStorage:
    """市场行为数据存储测试"""
    
    def test_save_and_load_dragon_tiger(self, tmp_path):
        """测试龙虎榜数据存取"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        storage = FinancialStorageManager(base_path=tmp_path)
        
        df = pd.DataFrame({
            'code': ['000001', '000002'],
            'trade_date': ['2024-04-19', '2024-04-19'],
            'close_price': [10.5, 15.2],
            'change_pct': [10.02, 9.98],
            'institution_net': [10000000, 6000000]
        })
        
        # 保存
        storage.save_dragon_tiger('20240419', df)
        
        # 加载
        loaded = storage.load_dragon_tiger('20240419')
        
        assert not loaded.empty
        assert len(loaded) == 2
    
    def test_save_and_load_money_flow(self, tmp_path):
        """测试资金流向数据存取"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        storage = FinancialStorageManager(base_path=tmp_path)
        
        df = pd.DataFrame({
            'code': ['000001', '000002'],
            'trade_date': ['2024-04-19', '2024-04-19'],
            'main_net_inflow': [5000000, -3000000],
            'retail_net_inflow': [1000000, 500000]
        })
        
        # 保存
        storage.save_money_flow('20240419', df)
        
        # 加载
        loaded = storage.load_money_flow('20240419')
        
        assert not loaded.empty


class TestMarketBehaviorValidation:
    """市场行为数据验证测试"""
    
    def test_dragon_tiger_data_validation(self):
        """测试龙虎榜数据验证"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 正常数据
        valid_data = pd.DataFrame({
            'code': ['000001'],
            'close_price': [10.5],
            'change_pct': [10.02],
            'institution_net': [1000000]
        })
        
        result = validator.validate_dragon_tiger_data(valid_data)
        assert result['valid']
        
        # 异常数据 - 涨跌幅超过限制
        invalid_data = pd.DataFrame({
            'code': ['000001'],
            'close_price': [10.5],
            'change_pct': [25.0],  # 超过20%限制
            'institution_net': [1000000]
        })
        
        result = validator.validate_dragon_tiger_data(invalid_data)
        assert not result['valid']
    
    def test_money_flow_validation(self):
        """测试资金流向数据验证"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 正常数据
        valid_data = pd.DataFrame({
            'code': ['000001'],
            'main_net_inflow': [5000000],
            'retail_net_inflow': [1000000]
        })
        
        result = validator.validate_money_flow_data(valid_data)
        assert result['valid']


class TestMarketBehaviorTask:
    """市场行为更新任务测试"""
    
    def test_daily_update(self):
        """测试每日更新任务"""
        from services.data_service.tasks.market_behavior_task import MarketBehaviorUpdateTask
        
        task = MarketBehaviorUpdateTask()
        task.dragon_tiger_fetcher = Mock()
        task.dragon_tiger_fetcher.fetch_dragon_tiger_list.return_value = pd.DataFrame({
            'code': ['000001']
        })
        task.money_flow_fetcher = Mock()
        task.money_flow_fetcher.fetch_all_money_flow.return_value = pd.DataFrame({
            'code': ['000001']
        })
        
        result = task.run_daily_update()
        
        assert result is not None
        assert 'dragon_tiger' in result or 'success' in result
    
    def test_run_dragon_tiger_update(self):
        """测试龙虎榜更新"""
        from services.data_service.tasks.market_behavior_task import MarketBehaviorUpdateTask
        
        task = MarketBehaviorUpdateTask()
        task.dragon_tiger_fetcher = Mock()
        task.dragon_tiger_fetcher.fetch_dragon_tiger_list.return_value = pd.DataFrame({
            'code': ['000001'],
            'institution_net': [1000000]
        })
        task.storage = Mock()
        
        result = task._run_dragon_tiger_update('20240419')
        
        assert result is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
