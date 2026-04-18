#!/usr/bin/env python3
"""
基本面分析模块测试
"""
import pytest
import pandas as pd


class TestFinancialScreener:
    """财务筛选器测试"""
    
    def test_fundamental_screening(self):
        """测试基本面筛选"""
        from services.analysis_service.fundamental.financial_screener import FinancialScreener
        
        screener = FinancialScreener()
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'roe': [15, 5, 25, 8],
            'gross_margin': [25, 15, 35, 18],
            'profit_growth': [30, 10, 50, 15]
        })
        
        result = screener.screen(stocks)
        
        # 应该筛选出ROE>=10, 毛利率>=20, 增长率>=20的股票
        assert '000001' in result['code'].values
        assert '000003' in result['code'].values
        assert '000002' not in result['code'].values
        assert '000004' not in result['code'].values
    
    def test_valuation_screening(self):
        """测试估值筛选"""
        from services.analysis_service.fundamental.financial_screener import FinancialScreener
        
        screener = FinancialScreener()
        stocks = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'pe': [20, 60, 15],
            'pb': [2, 15, 1.5],
            'roe': [15, 10, 18]
        })
        
        result = screener.screen_by_valuation(stocks)
        
        # PE<50, PB<10
        assert '000001' in result['code'].values
        assert '000003' in result['code'].values
        assert '000002' not in result['code'].values
    
    def test_growth_screening(self):
        """测试成长性筛选"""
        from services.analysis_service.fundamental.financial_screener import FinancialScreener
        
        screener = FinancialScreener()
        stocks = pd.DataFrame({
            'code': ['000001', '000002'],
            'revenue_growth': [25, 15],
            'profit_growth': [30, 18],
            'roe': [15, 12]
        })
        
        result = screener.screen_by_growth(stocks)
        
        assert '000001' in result['code'].values
        assert '000002' not in result['code'].values


class TestFinancialRiskDetector:
    """财务风险检测器测试"""
    
    def test_receivable_risk_detection(self):
        """测试应收账款风险检测"""
        from services.analysis_service.fundamental.risk_detector import FinancialRiskDetector
        
        detector = FinancialRiskDetector()
        risks = detector.detect({
            'receivable_growth': 100,  # 异常增长
            'inventory_turnover': 5
        })
        
        assert len(risks) > 0
        assert any('应收账款' in risk for risk in risks)
    
    def test_inventory_risk_detection(self):
        """测试存货风险检测"""
        from services.analysis_service.fundamental.risk_detector import FinancialRiskDetector
        
        detector = FinancialRiskDetector()
        risks = detector.detect({
            'receivable_growth': 20,
            'inventory_turnover': -20  # 周转率下降
        })
        
        assert len(risks) > 0
        assert any('存货' in risk for risk in risks)
    
    def test_debt_risk_detection(self):
        """测试负债风险检测"""
        from services.analysis_service.fundamental.risk_detector import FinancialRiskDetector
        
        detector = FinancialRiskDetector()
        risks = detector.detect({
            'debt_ratio': 75,  # 高负债
            'current_ratio': 0.8  # 流动比率低
        })
        
        assert len(risks) > 0
    
    def test_no_risk_detection(self):
        """测试无风险情况"""
        from services.analysis_service.fundamental.risk_detector import FinancialRiskDetector
        
        detector = FinancialRiskDetector()
        risks = detector.detect({
            'receivable_growth': 10,
            'inventory_turnover': 5,
            'debt_ratio': 40,
            'current_ratio': 2.0
        })
        
        assert len(risks) == 0


class TestValuationAnalyzer:
    """估值分析器测试"""
    
    def test_pe_analysis(self):
        """测试PE分析"""
        from services.analysis_service.fundamental.valuation_analyzer import ValuationAnalyzer
        
        analyzer = ValuationAnalyzer()
        result = analyzer.analyze_pe(20, industry_avg_pe=25)
        
        assert result['is_undervalued'] == True
        assert 'discount' in result
    
    def test_pb_analysis(self):
        """测试PB分析"""
        from services.analysis_service.fundamental.valuation_analyzer import ValuationAnalyzer
        
        analyzer = ValuationAnalyzer()
        result = analyzer.analyze_pb(2.5, roe=15)
        
        assert 'pb_roe_ratio' in result
        assert 'assessment' in result
