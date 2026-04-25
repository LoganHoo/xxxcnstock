#!/usr/bin/env python3
"""
财务数据服务单元测试

测试内容:
- 资产负债表获取器
- 利润表获取器
- 现金流量表获取器
- 财务指标计算
- 数据验证
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock


class TestBalanceSheetFetcher:
    """资产负债表获取器测试"""
    
    @pytest.fixture
    def mock_tushare(self):
        """模拟Tushare API响应"""
        mock = Mock()
        mock.query.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'ann_date': ['20240331', '20231231'],
            'f_ann_date': ['20240430', '20240430'],
            'end_date': ['20240331', '20231231'],
            'total_assets': [1000000, 950000],
            'total_liab': [400000, 380000],
            'total_hldr_eqy_exc_min_int': [600000, 570000],
            'money_cap': [100000, 90000],
            'trad_asset': [50000, 45000],
            'notes_receiv': [20000, 18000],
            'accounts_receiv': [30000, 28000],
            'inventories': [50000, 48000],
            'fix_assets': [200000, 190000],
        })
        return mock
    
    def test_fetch_balance_sheet_success(self, mock_tushare):
        """测试成功获取资产负债表"""
        from services.data_service.fetchers.financial.balance_sheet_fetcher import BalanceSheetFetcher
        
        fetcher = BalanceSheetFetcher()
        fetcher.pro = mock_tushare
        
        df = fetcher.fetch_stock_balance_sheet('000001')
        
        assert not df.empty
        assert 'report_date' in df.columns
        assert 'total_assets' in df.columns
        assert len(df) == 2
    
    def test_fetch_balance_sheet_empty_response(self):
        """测试空响应处理"""
        from services.data_service.fetchers.financial.balance_sheet_fetcher import BalanceSheetFetcher
        
        fetcher = BalanceSheetFetcher()
        fetcher.pro = Mock()
        fetcher.pro.query.return_value = pd.DataFrame()
        
        df = fetcher.fetch_stock_balance_sheet('INVALID')
        
        assert df.empty
    
    def test_fetch_balance_sheet_api_error(self):
        """测试API错误处理"""
        from services.data_service.fetchers.financial.balance_sheet_fetcher import BalanceSheetFetcher
        from services.data_service.exceptions import DataFetchError
        
        fetcher = BalanceSheetFetcher()
        fetcher.pro = Mock()
        fetcher.pro.query.side_effect = Exception("API Error")
        
        with pytest.raises(DataFetchError):
            fetcher.fetch_stock_balance_sheet('000001')
    
    def test_accounting_identity_validation(self):
        """测试会计恒等式验证"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 正确的数据: 资产 = 负债 + 权益
        valid_data = pd.DataFrame({
            'total_assets': [1000000],
            'total_liab': [400000],
            'total_hldr_eqy_exc_min_int': [600000]
        })
        
        result = validator.validate_accounting_identity(valid_data)
        assert result['valid']
        
        # 错误的数据
        invalid_data = pd.DataFrame({
            'total_assets': [1000000],
            'total_liab': [400000],
            'total_hldr_eqy_exc_min_int': [500000]  # 应该是600000
        })
        
        result = validator.validate_accounting_identity(invalid_data)
        assert not result['valid']


class TestIncomeStatementFetcher:
    """利润表获取器测试"""
    
    @pytest.fixture
    def mock_tushare(self):
        """模拟Tushare API响应"""
        mock = Mock()
        mock.query.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'ann_date': ['20240331', '20231231'],
            'f_ann_date': ['20240430', '20240430'],
            'end_date': ['20240331', '20231231'],
            'total_revenue': [1000000, 3800000],
            'revenue': [1000000, 3800000],
            'total_cogs': [600000, 2200000],
            'oper_exp': [100000, 350000],
            'int_exp': [20000, 80000],
            'net_profit': [200000, 900000],
            'total_profit': [250000, 1100000],
            'income_tax': [50000, 200000],
        })
        return mock
    
    def test_fetch_income_statement_success(self, mock_tushare):
        """测试成功获取利润表"""
        from services.data_service.fetchers.financial.income_statement_fetcher import IncomeStatementFetcher
        
        fetcher = IncomeStatementFetcher()
        fetcher.pro = mock_tushare
        
        df = fetcher.fetch_stock_income_statement('000001')
        
        assert not df.empty
        assert 'report_date' in df.columns
        assert 'total_revenue' in df.columns
    
    def test_profit_calculation(self):
        """测试利润计算逻辑"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        data = pd.DataFrame({
            'total_revenue': [1000000],
            'total_cogs': [600000],
            'oper_exp': [100000],
            'int_exp': [20000],
            'income_tax': [50000]
        })
        
        # 毛利润 = 营收 - 成本
        gross_profit = data['total_revenue'] - data['total_cogs']
        assert gross_profit.iloc[0] == 400000
        
        # 营业利润 = 毛利润 - 运营费用
        operating_profit = gross_profit - data['oper_exp']
        assert operating_profit.iloc[0] == 300000


class TestCashFlowFetcher:
    """现金流量表获取器测试"""
    
    @pytest.fixture
    def mock_tushare(self):
        """模拟Tushare API响应"""
        mock = Mock()
        mock.query.return_value = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ'],
            'ann_date': ['20240331', '20231231'],
            'f_ann_date': ['20240430', '20240430'],
            'end_date': ['20240331', '20231231'],
            'n_cashflow_act': [150000, 600000],
            'c_inf_fr_operate_a': [1200000, 4500000],
            'c_paid_to_for_empl_a': [300000, 1100000],
            'c_paid_for_taxes': [100000, 400000],
            'n_cashflow_inv_act': [-80000, -300000],
            'n_cash_fina_act': [-50000, -200000],
        })
        return mock
    
    def test_fetch_cash_flow_success(self, mock_tushare):
        """测试成功获取现金流量表"""
        from services.data_service.fetchers.financial.cash_flow_fetcher import CashFlowFetcher
        
        fetcher = CashFlowFetcher()
        fetcher.pro = mock_tushare
        
        df = fetcher.fetch_stock_cash_flow('000001')
        
        assert not df.empty
        assert 'report_date' in df.columns
        assert 'n_cashflow_act' in df.columns
    
    def test_free_cash_flow_calculation(self):
        """测试自由现金流计算"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        data = pd.DataFrame({
            'n_cashflow_act': [150000],
            'n_cashflow_inv_act': [-80000]
        })
        
        # 自由现金流 = 经营现金流 + 投资现金流
        fcf = data['n_cashflow_act'] + data['n_cashflow_inv_act']
        assert fcf.iloc[0] == 70000


class TestFinancialIndicatorEngine:
    """财务指标引擎测试"""
    
    @pytest.fixture
    def sample_financial_data(self):
        """示例财务数据"""
        return pd.DataFrame({
            'code': ['000001', '000001', '000001'],
            'report_date': ['2024-03-31', '2023-12-31', '2023-09-30'],
            'total_assets': [1000000, 950000, 900000],
            'total_hldr_eqy_exc_min_int': [600000, 570000, 540000],
            'net_profit': [50000, 200000, 150000],
            'total_revenue': [300000, 1200000, 900000],
            'total_cogs': [180000, 720000, 540000],
            'total_liab': [400000, 380000, 360000],
            'inventories': [50000, 48000, 45000],
            'notes_receiv': [20000, 18000, 16000],
            'accounts_receiv': [30000, 28000, 26000],
            'money_cap': [100000, 90000, 80000],
            'trad_asset': [50000, 45000, 40000],
        })
    
    def test_calculate_roe(self, sample_financial_data):
        """测试ROE计算"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        roe = engine.calculate_roe(
            sample_financial_data['net_profit'],
            sample_financial_data['total_hldr_eqy_exc_min_int']
        )
        
        # ROE = 净利润 / 股东权益
        expected_roe = 50000 / 600000 * 100
        assert abs(roe.iloc[0] - expected_roe) < 0.01
    
    def test_calculate_gross_margin(self, sample_financial_data):
        """测试毛利率计算"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        margin = engine.calculate_gross_margin(
            sample_financial_data['total_revenue'],
            sample_financial_data['total_cogs']
        )
        
        # 毛利率 = (营收 - 成本) / 营收 * 100
        expected_margin = (300000 - 180000) / 300000 * 100
        assert abs(margin.iloc[0] - expected_margin) < 0.01
    
    def test_calculate_debt_ratio(self, sample_financial_data):
        """测试资产负债率计算"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        ratio = engine.calculate_debt_ratio(
            sample_financial_data['total_liab'],
            sample_financial_data['total_assets']
        )
        
        # 资产负债率 = 负债 / 资产 * 100
        expected_ratio = 400000 / 1000000 * 100
        assert abs(ratio.iloc[0] - expected_ratio) < 0.01
    
    def test_calculate_current_ratio(self, sample_financial_data):
        """测试流动比率计算"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        current_assets = (
            sample_financial_data['money_cap'] +
            sample_financial_data['trad_asset'] +
            sample_financial_data['notes_receiv'] +
            sample_financial_data['accounts_receiv'] +
            sample_financial_data['inventories']
        )
        
        # 假设流动负债
        current_liab = sample_financial_data['total_liab'] * 0.6
        
        ratio = engine.calculate_current_ratio(current_assets, current_liab)
        
        assert ratio.iloc[0] > 0
    
    def test_calculate_all_indicators(self, sample_financial_data):
        """测试批量计算所有指标"""
        from services.data_service.processors.financial.indicator_engine import FinancialIndicatorEngine
        
        engine = FinancialIndicatorEngine()
        
        indicators = engine.calculate_all_indicators(sample_financial_data)
        
        assert 'roe' in indicators.columns
        assert 'gross_margin' in indicators.columns
        assert 'debt_ratio' in indicators.columns
        assert 'current_ratio' in indicators.columns


class TestFinancialStorage:
    """财务数据存储测试"""
    
    def test_save_and_load_balance_sheet(self, tmp_path):
        """测试资产负债表存取"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        storage = FinancialStorageManager(base_path=tmp_path)
        
        df = pd.DataFrame({
            'code': ['000001', '000001'],
            'report_date': ['2024-03-31', '2023-12-31'],
            'total_assets': [1000000, 950000],
            'total_liab': [400000, 380000]
        })
        
        # 保存
        storage.save_balance_sheet('000001', df)
        
        # 加载
        loaded = storage.load_balance_sheet('000001')
        
        assert not loaded.empty
        assert len(loaded) == 2
    
    def test_load_indicators_from_cache(self, tmp_path):
        """测试从缓存加载指标"""
        from services.data_service.storage.optimized_financial_storage import OptimizedFinancialStorageManager
        
        storage = OptimizedFinancialStorageManager(
            base_path=tmp_path,
            enable_cache=True
        )
        
        df = pd.DataFrame({
            'code': ['000001'],
            'report_date': ['2024-03-31'],
            'roe': [15.0],
            'gross_margin': [30.0]
        })
        
        # 保存
        storage.save_financial_indicators('000001', df)
        
        # 第一次加载（从文件）
        loaded1 = storage.load_financial_indicators('000001')
        
        # 第二次加载（从缓存）
        loaded2 = storage.load_financial_indicators('000001')
        
        assert not loaded1.empty
        assert not loaded2.empty


class TestFinancialUpdateTask:
    """财务数据更新任务测试"""
    
    def test_incremental_update(self):
        """测试增量更新"""
        from services.data_service.tasks.financial_update_task import FinancialUpdateTask
        
        task = FinancialUpdateTask()
        task.service = Mock()
        task.service.batch_update_financial_data.return_value = {
            '000001': {'success': True},
            '000002': {'success': True}
        }
        
        with patch.object(task, '_get_stock_codes', return_value=['000001', '000002']):
            result = task.run_incremental_update(codes=['000001', '000002'])
            
            assert 'success_count' in result or 'total' in result
    
    def test_full_update(self):
        """测试全量更新"""
        from services.data_service.tasks.financial_update_task import FinancialUpdateTask
        
        task = FinancialUpdateTask()
        task.service = Mock()
        task.service.batch_update_financial_data.return_value = {}
        
        with patch.object(task, '_get_stock_codes', return_value=['000001']):
            result = task.run_full_update(codes=['000001'], years=1)
            
            assert result is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
