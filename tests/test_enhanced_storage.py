"""EnhancedStorage 单元测试"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestEnhancedStorage:
    """EnhancedStorage 测试类"""
    
    def test_import_enhanced_storage(self):
        """测试导入 EnhancedStorage"""
        from services.data_service.storage.enhanced_storage import EnhancedStorage
        assert EnhancedStorage is not None
    
    def test_enhanced_storage_init(self):
        """测试 EnhancedStorage 初始化"""
        from services.data_service.storage.enhanced_storage import EnhancedStorage
        storage = EnhancedStorage()
        assert storage is not None
        assert storage.data_dir is not None
    
    def test_cache_set_get(self):
        """测试缓存设置和获取"""
        from services.data_service.storage.enhanced_storage import EnhancedStorage
        storage = EnhancedStorage()
        
        # 设置缓存
        test_data = {"key": "value", "number": 123}
        storage.cache_set("test_key", test_data, ttl=60)
        
        # 获取缓存
        result = storage.cache_get("test_key")
        assert result is not None
        assert result["key"] == "value"
        assert result["number"] == 123
    
    def test_cache_delete(self):
        """测试缓存删除"""
        from services.data_service.storage.enhanced_storage import EnhancedStorage
        storage = EnhancedStorage()
        
        storage.cache_set("test_delete", {"data": "test"}, ttl=60)
        result = storage.cache_get("test_delete")
        assert result is not None
        
        storage.cache_delete("test_delete")
        result = storage.cache_get("test_delete")
        assert result is None
    
    def test_get_storage_singleton(self):
        """测试单例模式"""
        from services.data_service.storage.enhanced_storage import get_storage
        
        storage1 = get_storage()
        storage2 = get_storage()
        assert storage1 is storage2


class TestVolumePriceFilterEnhanced:
    """VolumePriceFilter 增强功能测试"""
    
    def test_calculate_enhanced_score_valid(self):
        """测试增强评分计算"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建模拟K线数据
        np.random.seed(42)
        n = 120
        dates = pd.date_range('2024-01-01', periods=n, freq='D')
        
        # 模拟上涨趋势
        base_price = 10.0
        trend = np.linspace(0, 0.3, n)  # 30%涨幅
        noise = np.random.randn(n) * 0.02
        
        close = base_price * (1 + trend + noise)
        high = close * 1.02
        low = close * 0.98
        volume = np.random.uniform(1000000, 5000000, n)
        
        df = pd.DataFrame({
            'date': dates,
            'open': close * 0.99,
            'close': close,
            'high': high,
            'low': low,
            'volume': volume
        })
        
        result = vp_filter.calculate_enhanced_score(df)
        
        assert 'total' in result
        assert 'scores' in result
        assert 'reasons' in result
        assert result['total'] > 0
    
    def test_calculate_enhanced_score_insufficient_data(self):
        """测试数据不足时的增强评分"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建少量数据
        df = pd.DataFrame({
            'close': [10.0, 10.5, 11.0],
            'high': [10.2, 10.7, 11.2],
            'low': [9.8, 10.3, 10.8],
            'volume': [1000000, 1200000, 1100000]
        })
        
        result = vp_filter.calculate_enhanced_score(df)
        
        assert result['total'] == 0
        assert '数据不足' in result['reasons'][0]
    
    def test_detect_trend_uptrend(self):
        """测试上涨趋势检测"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建上涨趋势数据
        prices = pd.Series([10 + i*0.1 for i in range(30)])
        
        trend = vp_filter.detect_trend(prices)
        assert trend in ['uptrend', 'weak_uptrend']
    
    def test_detect_trend_downtrend(self):
        """测试下跌趋势检测"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建下跌趋势数据
        prices = pd.Series([20 - i*0.1 for i in range(30)])
        
        trend = vp_filter.detect_trend(prices)
        assert trend in ['downtrend', 'weak_downtrend']
    
    def test_calculate_kdj(self):
        """测试 KDJ 计算"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        np.random.seed(42)
        n = 20
        close = pd.Series(np.random.uniform(9, 11, n))
        high = close * 1.02
        low = close * 0.98
        
        k, d, j = vp_filter.calculate_kdj(high, low, close)
        
        assert 0 <= k <= 100
        assert 0 <= d <= 100
        # J值可能超出0-100范围
    
    def test_calculate_atr(self):
        """测试 ATR 计算"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        np.random.seed(42)
        n = 20
        close = pd.Series(np.random.uniform(9, 11, n))
        high = close * 1.03
        low = close * 0.97
        
        atr = vp_filter.calculate_atr(high, low, close)
        
        assert atr > 0
    
    def test_detect_volume_pattern(self):
        """测试量价关系检测"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建量价齐升数据
        prices = pd.Series([10, 10.5, 11, 11.5, 12])
        volume = pd.Series([100, 120, 150, 180, 200])
        
        result = vp_filter.detect_volume_pattern(volume, prices)
        
        assert 'pattern' in result
        assert 'score' in result
        assert result['score'] >= 50
    
    def test_detect_support_resistance(self):
        """测试支撑压力位检测"""
        from services.stock_service.filters.volume_price import VolumePriceFilter
        
        vp_filter = VolumePriceFilter()
        
        # 创建震荡数据
        np.random.seed(42)
        prices = pd.Series(np.random.uniform(9, 11, 60))
        
        result = vp_filter.detect_support_resistance(prices)
        
        assert 'support' in result
        assert 'resistance' in result
        assert 'position' in result
        assert result['support'] < result['resistance']
