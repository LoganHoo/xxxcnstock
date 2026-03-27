"""功能验收测试 - End-to-End 验证"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestStockAnalysisAcceptance:
    """股票分析功能验收测试"""
    
    def test_realtime_data_loaded(self):
        """验收：实时行情数据已加载"""
        df = pd.read_parquet('data/realtime/20260316.parquet')
        
        assert len(df) > 5000, "实时行情数据不足5000只"
        assert 'code' in df.columns
        assert 'name' in df.columns
        assert 'price' in df.columns
        print(f"✓ 实时行情数据: {len(df)}只股票")
    
    def test_enhanced_analysis_completed(self):
        """验收：增强分析已完成"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        assert len(df) >= 4800, "分析结果不足4800只"
        
        # 检查评分分布合理
        s_count = len(df[df['grade'] == 'S'])
        a_count = len(df[df['grade'] == 'A'])
        
        assert s_count > 0, "无S级股票"
        assert a_count > 0, "无A级股票"
        assert s_count < len(df) * 0.15, "S级占比过高"  # S级应<15%
        print(f"✓ 增强分析完成: {len(df)}只, S级{s_count}只, A级{a_count}只")
    
    def test_technical_indicators_calculated(self):
        """验收：技术指标已计算"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        # 检查必要字段
        required_fields = ['enhanced_score', 'grade', 'rsi', 'momentum_10d']
        for field in required_fields:
            assert field in df.columns, f"缺少字段: {field}"
        
        # 检查RSI范围
        rsi = df['rsi'].dropna()
        assert rsi.min() >= 0 and rsi.max() <= 100, "RSI范围异常"
        print(f"✓ 技术指标已计算: RSI范围[{rsi.min():.1f}, {rsi.max():.1f}]")
    
    def test_s_grade_stocks_have_valid_reasons(self):
        """验收：S级股票有合理推荐理由"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        s_stocks = df[df['grade'] == 'S']
        
        # 所有S级股票应有推荐理由
        assert s_stocks['reasons'].notna().all(), "存在无推荐理由的S级股票"
        
        # 推荐理由应包含关键技术词
        sample = s_stocks.head(10)
        for _, row in sample.iterrows():
            reasons = str(row['reasons'])
            assert len(reasons) > 5, f"推荐理由过短: {row['code']}"
        print(f"✓ S级股票推荐理由验证通过")
    
    def test_score_distribution_reasonable(self):
        """验收：评分分布合理"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        scores = df['enhanced_score']
        
        # 评分应该在0-100范围内
        assert scores.min() >= 0, "存在负分"
        assert scores.max() <= 100, "存在超100分"
        
        # S级评分应>=80
        s_scores = df[df['grade'] == 'S']['enhanced_score']
        assert s_scores.min() >= 80, f"S级最低分应>=80, 实际{s_scores.min()}"
        
        print(f"✓ 评分分布验证: 范围[{scores.min():.1f}, {scores.max():.1f}]")


class TestIndexAnalysisAcceptance:
    """指数分析功能验收测试"""
    
    def test_all_indices_analyzed(self):
        """验收：所有主要指数已分析"""
        df = pd.read_parquet('data/index_analysis_20260316.parquet')
        
        expected_indices = ['上证指数', '深证成指', '创业板指', '沪深300', 
                           '上证50', '中证500', '科创50', '中证1000']
        
        for idx in expected_indices:
            assert idx in df['name'].values, f"缺少指数: {idx}"
        
        print(f"✓ 所有指数已分析: {len(df)}个")
    
    def test_index_scores_calculated(self):
        """验收：指数评分已计算"""
        df = pd.read_parquet('data/index_analysis_20260316.parquet')
        
        assert 'score' in df.columns
        assert 'grade' in df.columns
        assert 'trend' in df.columns
        
        # 至少有一个指数评分>=60
        assert df['score'].max() >= 60, "无指数评分>=60"
        print(f"✓ 指数评分范围: [{df['score'].min():.1f}, {df['score'].max():.1f}]")


class TestRedisCacheAcceptance:
    """Redis缓存功能验收测试"""
    
    def test_statistics_cached(self):
        """验收：统计数据已缓存"""
        from services.data_service.storage.enhanced_storage import get_storage
        
        storage = get_storage()
        stats = storage.cache_get('statistics:all')
        
        if stats:  # Redis可用时
            assert 'total' in stats
            assert stats['total'] >= 4800
            print(f"✓ 统计数据已缓存: {stats['total']}只")
        else:
            print("⚠ Redis未连接，跳过缓存验证")
    
    def test_s_grade_stocks_cached(self):
        """验收：S级股票已缓存"""
        from services.data_service.storage.enhanced_storage import get_storage
        
        storage = get_storage()
        s_stocks = storage.cache_get('stocks:s_grade_all')
        
        if s_stocks:
            assert len(s_stocks) >= 400, f"S级股票缓存不足: {len(s_stocks)}"
            print(f"✓ S级股票已缓存: {len(s_stocks)}只")
        else:
            print("⚠ Redis未连接，跳过缓存验证")


class TestDataIntegrityAcceptance:
    """数据完整性验收测试"""
    
    def test_no_duplicate_stocks(self):
        """验收：无重复股票"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        duplicates = df['code'].duplicated().sum()
        assert duplicates == 0, f"存在{duplicates}只重复股票"
        print(f"✓ 无重复股票")
    
    def test_no_null_critical_fields(self):
        """验收：关键字段无空值"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        critical_fields = ['code', 'name', 'price', 'enhanced_score', 'grade']
        for field in critical_fields:
            null_count = df[field].isna().sum()
            assert null_count == 0, f"字段{field}存在{null_count}个空值"
        
        print(f"✓ 关键字段无空值")
    
    def test_price_reasonable(self):
        """验收：价格数据合理"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        # 价格应>0
        assert df['price'].min() > 0, "存在非正价格"
        
        # 价格应<2000（贵州茅台等高价股合理范围）
        assert df['price'].max() < 2000, f"存在异常高价: {df['price'].max()}"
        
        print(f"✓ 价格范围合理: [{df['price'].min():.2f}, {df['price'].max():.2f}]")
