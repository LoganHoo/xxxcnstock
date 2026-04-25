#!/usr/bin/env python3
"""
数据服务集成测试

测试内容:
- 完整数据流: 采集 -> 处理 -> 存储 -> 查询
- 多模块协作
- 错误恢复
- 性能基准
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import time
from unittest.mock import Mock, patch, MagicMock


class TestUnifiedDataServiceIntegration:
    """统一数据服务集成测试"""
    
    @pytest.fixture
    def temp_data_dir(self):
        """临时数据目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_complete_financial_data_flow(self, temp_data_dir):
        """测试完整财务数据流程"""
        from services.data_service.unified_data_service import UnifiedDataService
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        # 初始化服务
        service = UnifiedDataService()
        service.storage = FinancialStorageManager(base_path=temp_data_dir)
        
        # 模拟财务数据
        mock_balance_sheet = pd.DataFrame({
            'code': ['000001'],
            'report_date': ['2024-03-31'],
            'total_assets': [1000000],
            'total_liab': [400000],
            'total_hldr_eqy_exc_min_int': [600000]
        })
        
        mock_income = pd.DataFrame({
            'code': ['000001'],
            'report_date': ['2024-03-31'],
            'total_revenue': [1000000],
            'net_profit': [150000]
        })
        
        mock_cash_flow = pd.DataFrame({
            'code': ['000001'],
            'report_date': ['2024-03-31'],
            'n_cashflow_act': [200000]
        })
        
        # 保存数据
        service.storage.save_balance_sheet('000001', mock_balance_sheet)
        service.storage.save_income_statement('000001', mock_income)
        service.storage.save_cash_flow('000001', mock_cash_flow)
        
        # 查询财务指标
        indicators = service.get_financial_indicators('000001')
        
        assert indicators is not None
        assert 'roe' in indicators.columns
    
    def test_complete_market_behavior_flow(self, temp_data_dir):
        """测试完整市场行为数据流程"""
        from services.data_service.unified_data_service import UnifiedDataService
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        service = UnifiedDataService()
        service.storage = FinancialStorageManager(base_path=temp_data_dir)
        
        # 模拟龙虎榜数据
        mock_dragon_tiger = pd.DataFrame({
            'code': ['000001', '000002'],
            'trade_date': ['2024-04-19', '2024-04-19'],
            'close_price': [10.5, 15.2],
            'change_pct': [10.02, 9.98],
            'institution_net': [10000000, 5000000]
        })
        
        # 模拟资金流向数据
        mock_money_flow = pd.DataFrame({
            'code': ['000001', '000002'],
            'trade_date': ['2024-04-19', '2024-04-19'],
            'main_net_inflow': [5000000, -3000000],
            'retail_net_inflow': [1000000, 500000]
        })
        
        # 保存数据
        service.storage.save_dragon_tiger('20240419', mock_dragon_tiger)
        service.storage.save_money_flow('20240419', mock_money_flow)
        
        # 查询数据
        dragon_tiger = service.get_dragon_tiger_data('20240419')
        money_flow = service.get_money_flow('000001')
        
        assert dragon_tiger is not None
        assert money_flow is not None
    
    def test_complete_announcement_flow(self, temp_data_dir):
        """测试完整公告数据流程"""
        from services.data_service.unified_data_service import UnifiedDataService
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        service = UnifiedDataService()
        service.storage = FinancialStorageManager(base_path=temp_data_dir)
        
        # 模拟公告数据
        mock_announcements = pd.DataFrame({
            'code': ['000001', '000001'],
            'announce_date': ['2024-04-19', '2024-04-18'],
            'title': ['第一季度报告', '股东增持公告'],
            'category': ['regular_report', 'equity_change'],
            'importance': [80, 60]
        })
        
        # 保存数据
        service.storage.save_announcements('000001', mock_announcements)
        
        # 查询数据
        announcements = service.get_stock_announcements('000001')
        
        assert announcements is not None
        assert len(announcements) == 2
    
    def test_data_consistency_across_modules(self, temp_data_dir):
        """测试跨模块数据一致性"""
        from services.data_service.unified_data_service import UnifiedDataService
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        service = UnifiedDataService()
        service.storage = FinancialStorageManager(base_path=temp_data_dir)
        
        code = '000001'
        
        # 保存多类型数据
        balance_sheet = pd.DataFrame({
            'code': [code],
            'report_date': ['2024-03-31'],
            'total_assets': [1000000]
        })
        
        dragon_tiger = pd.DataFrame({
            'code': [code],
            'trade_date': ['2024-04-19'],
            'institution_net': [1000000]
        })
        
        announcements = pd.DataFrame({
            'code': [code],
            'announce_date': ['2024-04-19'],
            'title': ['测试公告']
        })
        
        service.storage.save_balance_sheet(code, balance_sheet)
        service.storage.save_dragon_tiger('20240419', dragon_tiger)
        service.storage.save_announcements(code, announcements)
        
        # 验证数据完整性
        assert service.storage.balance_sheet_exists(code)
        assert service.storage.dragon_tiger_exists('20240419')
        assert service.storage.announcements_exists(code)


class TestDataServicePerformance:
    """数据服务性能测试"""
    
    def test_financial_data_query_performance(self):
        """测试财务数据查询性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True
            )
            
            # 准备测试数据
            df = pd.DataFrame({
                'code': ['000001'] * 100,
                'report_date': pd.date_range('2021-01-01', periods=100, freq='Q').strftime('%Y-%m-%d'),
                'roe': np.random.uniform(5, 25, 100),
                'gross_margin': np.random.uniform(20, 50, 100)
            })
            
            storage.save_financial_indicators('000001', df)
            
            # 测试查询性能
            start = time.time()
            for _ in range(100):
                result = storage.load_financial_indicators('000001')
            elapsed = time.time() - start
            
            # 100次查询应在1秒内完成（使用缓存）
            assert elapsed < 1.0, f"查询性能不达标: {elapsed}s"
    
    def test_batch_data_loading_performance(self):
        """测试批量数据加载性能"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = OptimizedFinancialStorageManager(
                base_path=Path(tmpdir),
                enable_cache=True
            )
            
            # 准备多只股票的测试数据
            codes = [f'{i:06d}' for i in range(1, 11)]
            
            for code in codes:
                df = pd.DataFrame({
                    'code': [code] * 20,
                    'report_date': pd.date_range('2021-01-01', periods=20, freq='Q').strftime('%Y-%m-%d'),
                    'roe': np.random.uniform(5, 25, 20)
                })
                storage.save_financial_indicators(code, df)
            
            # 测试批量加载性能
            start = time.time()
            results = storage.batch_load_indicators(codes)
            elapsed = time.time() - start
            
            # 10只股票批量加载应在500ms内完成
            assert elapsed < 0.5, f"批量加载性能不达标: {elapsed}s"
            assert len(results) == 10


class TestDataServiceErrorRecovery:
    """数据服务错误恢复测试"""
    
    def test_fetch_retry_mechanism(self):
        """测试获取重试机制"""
        from services.data_service.fetchers.financial.balance_sheet_fetcher import BalanceSheetFetcher
        
        fetcher = BalanceSheetFetcher()
        fetcher.pro = Mock()
        
        # 前两次失败，第三次成功
        fetcher.pro.query.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            pd.DataFrame({'ts_code': ['000001.SZ']})
        ]
        
        # 应该成功（第三次）
        df = fetcher.fetch_stock_balance_sheet('000001')
        
        assert not df.empty
        assert fetcher.pro.query.call_count == 3
    
    def test_fallback_to_backup_source(self):
        """测试备用数据源切换"""
        from services.data_service.fetchers.financial.balance_sheet_fetcher import BalanceSheetFetcher
        
        fetcher = BalanceSheetFetcher()
        
        # 主数据源失败
        fetcher.pro = Mock()
        fetcher.pro.query.side_effect = Exception("API Error")
        
        # 备用数据源成功
        with patch('akshare.stock_balance_sheet_by_report_em', return_value=pd.DataFrame({
            '股票代码': ['000001'],
            '资产总计': [1000000]
        })):
            df = fetcher.fetch_stock_balance_sheet('000001')
            
            # 应该使用备用数据源获取数据
            assert not df.empty
    
    def test_data_validation_failure_handling(self):
        """测试数据验证失败处理"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 无效数据
        invalid_data = pd.DataFrame({
            'total_assets': [1000000],
            'total_liab': [400000],
            'total_hldr_eqy_exc_min_int': [500000]  # 应该是600000
        })
        
        result = validator.validate_accounting_identity(invalid_data)
        
        assert not result['valid']
        assert 'error' in result


class TestDailyUpdateTaskIntegration:
    """每日更新任务集成测试"""
    
    def test_daily_update_workflow(self):
        """测试每日更新工作流"""
        from services.data_service.tasks.daily_update_task import DailyUpdateTask
        
        task = DailyUpdateTask()
        
        # 模拟各子任务
        task.market_behavior_task = Mock()
        task.market_behavior_task.run_daily_update.return_value = {
            'success': True,
            'dragon_tiger_count': 50,
            'money_flow_count': 5000
        }
        
        task.announcement_task = Mock()
        task.announcement_task.run_daily_update.return_value = {
            'success': True,
            'announcement_count': 100
        }
        
        # 执行更新
        result = task.run_market_close_update()
        
        assert result is not None
        assert 'market_behavior' in result
        assert 'announcements' in result
    
    def test_partial_failure_handling(self):
        """测试部分失败处理"""
        from services.data_service.tasks.daily_update_task import DailyUpdateTask
        
        task = DailyUpdateTask()
        
        # 市场行为数据成功，公告数据失败
        task.market_behavior_task = Mock()
        task.market_behavior_task.run_daily_update.return_value = {'success': True}
        
        task.announcement_task = Mock()
        task.announcement_task.run_daily_update.side_effect = Exception("API Error")
        
        # 应该继续执行，记录错误
        result = task.run_market_close_update()
        
        assert result is not None
        assert result['market_behavior'] is not None
        assert result['announcements'] is not None
        assert not result['announcements'].get('success', True)


class TestDataPreheatingIntegration:
    """数据预热集成测试"""
    
    def test_preheating_workflow(self):
        """测试预热工作流"""
        from services.data_service.tasks.data_preheat_task import DataPreheatingTask
        
        task = DataPreheatingTask()
        
        # 模拟获取热点股票
        task._get_hot_stocks_from_dragon_tiger = Mock(return_value={'000001', '000002'})
        task._get_hot_stocks_from_money_flow = Mock(return_value={'000003', '000004'})
        task._get_blue_chip_stocks = Mock(return_value={'600000', '600036'})
        
        # 模拟存储
        task.storage = Mock()
        task.storage.load_financial_indicators.return_value = pd.DataFrame({
            'code': ['000001'],
            'roe': [15.0]
        })
        
        # 执行预热
        result = task.run_preheating()
        
        assert result is not None
        assert result['preheated_count'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
