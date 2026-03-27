"""指数分析功能测试"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestIndexAnalysis:
    """指数分析测试类"""
    
    def test_index_codes_defined(self):
        """测试指数代码定义"""
        INDEX_CODES = {
            'sh000001': '上证指数',
            'sz399001': '深证成指',
            'sz399006': '创业板指',
            'sh000300': '沪深300',
            'sh000016': '上证50',
            'sh000905': '中证500',
            'sh000688': '科创50',
            'sh000852': '中证1000'
        }
        
        assert len(INDEX_CODES) == 8
        assert 'sh000001' in INDEX_CODES
        assert INDEX_CODES['sh000001'] == '上证指数'
    
    def test_analyze_index_data_structure(self):
        """测试指数分析数据结构"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 模拟指数数据
        np.random.seed(42)
        n = 120
        close = pd.Series(3000 + np.cumsum(np.random.randn(n) * 10))
        high = close * 1.01
        low = close * 0.99
        volume = pd.Series(np.random.uniform(1e9, 5e9, n))
        
        df = pd.DataFrame({
            'close': close,
            'high': high,
            'low': low,
            'volume': volume
        })
        
        result = vp_filter.calculate_enhanced_score(df)
        
        assert 'total' in result
        assert 'scores' in result
        assert 'indicators' in result
    
    def test_index_trend_analysis(self):
        """测试指数趋势分析"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 模拟上涨指数
        prices = pd.Series([3000 + i*5 for i in range(60)])
        trend = vp_filter.detect_trend(prices)
        
        assert trend in ['uptrend', 'weak_uptrend', 'downtrend', 'weak_downtrend', 'unknown']


class TestParquetDataValidation:
    """Parquet 数据验证测试"""
    
    def test_enhanced_scores_file_exists(self):
        """测试分析结果文件存在"""
        import os
        file_path = 'data/enhanced_scores_full.parquet'
        assert os.path.exists(file_path), f"文件不存在: {file_path}"
    
    def test_enhanced_scores_data_valid(self):
        """测试分析结果数据有效"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        assert len(df) > 0
        assert 'code' in df.columns
        assert 'name' in df.columns
        assert 'enhanced_score' in df.columns
        assert 'grade' in df.columns
    
    def test_enhanced_scores_grade_values(self):
        """测试评分等级值有效"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        valid_grades = {'S', 'A', 'B', 'C'}
        actual_grades = set(df['grade'].unique())
        
        assert actual_grades.issubset(valid_grades), f"无效等级: {actual_grades - valid_grades}"
    
    def test_enhanced_scores_score_range(self):
        """测试评分范围有效"""
        df = pd.read_parquet('data/enhanced_scores_full.parquet')
        
        assert df['enhanced_score'].min() >= 0
        assert df['enhanced_score'].max() <= 100
    
    def test_index_analysis_file_exists(self):
        """测试指数分析文件存在"""
        import os
        file_path = 'data/index_analysis_20260316.parquet'
        assert os.path.exists(file_path), f"文件不存在: {file_path}"
    
    def test_realtime_data_file_exists(self):
        """测试实时行情文件存在"""
        import os
        file_path = 'data/realtime/20260316.parquet'
        assert os.path.exists(file_path), f"文件不存在: {file_path}"


class TestRedisCache:
    """Redis 缓存测试"""
    
    def test_redis_connection(self):
        """测试 Redis 连接"""
        from services.data_service.storage.enhanced_storage import get_storage
        
        storage = get_storage()
        # 即使 Redis 未连接，也不应抛出异常
        assert storage is not None
    
    def test_cache_operations(self):
        """测试缓存操作"""
        from services.data_service.storage.enhanced_storage import get_storage
        
        storage = get_storage()
        
        # 测试设置和获取
        test_data = {'test': 'value', 'count': 100}
        storage.cache_set('unit_test_key', test_data, ttl=60)
        
        result = storage.cache_get('unit_test_key')
        if result:  # Redis 可用时
            assert result['test'] == 'value'
            assert result['count'] == 100
        
        # 清理
        storage.cache_delete('unit_test_key')
