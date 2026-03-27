"""
股票推荐系统单元测试
"""
import pytest
import polars as pl
from pathlib import Path
import sys
import os

sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from tomorrow_picks import (
    ConfigManager, DataLoader, SGradeFilter, AGradeFilter,
    BullishFilter, MACDVolumeFilter, FilterEngine, TextReporter,
    HTMLReporter, JSONReporter, StockRecommender
)


class TestConfigManager:
    """配置管理器测试"""
    
    def test_load_config(self):
        """测试配置加载"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        assert manager.config is not None
        assert 'recommendation' in manager.config
    
    def test_get_filter_config(self):
        """测试获取筛选器配置"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        s_grade_config = manager.get_filter_config('s_grade')
        assert 'min_score' in s_grade_config
        assert 'top_n' in s_grade_config
    
    def test_get_data_path(self):
        """测试获取数据路径"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        data_path = manager.get_data_path()
        assert data_path is not None
        assert 'parquet' in data_path


class TestDataLoader:
    """数据加载器测试"""
    
    def test_load_data(self):
        """测试数据加载"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        data_path = Path(__file__).parent.parent / manager.get_data_path()
        
        loader = DataLoader(str(data_path))
        df = loader.load_data()
        
        assert df is not None
        assert len(df) > 0
    
    def test_validate_data(self):
        """测试数据验证"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        data_path = Path(__file__).parent.parent / manager.get_data_path()
        
        loader = DataLoader(str(data_path))
        df = loader.load_data()
        
        # 验证必需字段存在
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score']
        for field in required_fields:
            assert field in df.columns


class TestFilters:
    """筛选器测试"""
    
    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        return pl.DataFrame({
            'code': ['000001', '000002', '000003', '000004'],
            'name': ['平安银行', '万科A', '国农科技', '国华网安'],
            'price': [10.0, 20.0, 30.0, 40.0],
            'grade': ['S', 'A', 'S', 'B'],
            'enhanced_score': [85, 78, 82, 70],
            'change_pct': [2.0, -1.0, 3.0, 0.5],
            'trend': [100, 50, 100, 0],
            'reasons': ['MACD金叉,量价齐升', '多头排列', 'MACD金叉', '量价齐升']
        })
    
    def test_s_grade_filter(self, sample_data):
        """测试S级筛选器"""
        filter_obj = SGradeFilter()
        config = {'min_score': 80, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 2
        assert all(result['grade'] == 'S')
        assert all(result['enhanced_score'] >= 80)
    
    def test_a_grade_filter(self, sample_data):
        """测试A级筛选器"""
        filter_obj = AGradeFilter()
        config = {'min_score': 75, 'max_score': 80, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 1
        assert all(result['grade'] == 'A')
        assert all(result['enhanced_score'] >= 75)
        assert all(result['enhanced_score'] < 80)
    
    def test_bullish_filter(self, sample_data):
        """测试多头排列筛选器"""
        filter_obj = BullishFilter()
        config = {'trend': 100, 'change_pct_min': 0, 'change_pct_max': 8, 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 2
        assert all(result['trend'] == 100)
        assert all(result['change_pct'] > 0)
    
    def test_macd_volume_filter(self, sample_data):
        """测试MACD+量价齐升筛选器"""
        filter_obj = MACDVolumeFilter()
        config = {'keywords': ['MACD', '量价齐升'], 'top_n': 10}
        
        result = filter_obj.apply(sample_data, config)
        
        assert len(result) == 1
        assert result['code'][0] == '000001'


class TestReporters:
    """报告生成器测试"""
    
    @pytest.fixture
    def sample_results(self):
        """创建测试结果"""
        return {
            's_grade': pl.DataFrame({
                'code': ['000001'],
                'name': ['平安银行'],
                'price': [10.0],
                'enhanced_score': [85],
                'change_pct': [2.0],
                'reasons': ['MACD金叉']
            })
        }
    
    @pytest.fixture
    def sample_stats(self):
        """创建测试统计"""
        return {
            'total_stocks': 100,
            's_grade_count': 10,
            'a_grade_count': 20,
            'bullish_count': 30,
            'rising_count': 40
        }
    
    def test_text_reporter(self, sample_results, sample_stats):
        """测试文本报告生成器"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        reporter = TextReporter()
        report = reporter.generate(sample_results, sample_stats, manager)
        
        assert report is not None
        assert '明日股票推荐' in report
        assert '平安银行' in report
    
    def test_html_reporter(self, sample_results, sample_stats):
        """测试HTML报告生成器"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        reporter = HTMLReporter()
        report = reporter.generate(sample_results, sample_stats, manager)
        
        assert report is not None
        assert '<!DOCTYPE html>' in report
        assert '平安银行' in report
    
    def test_json_reporter(self, sample_results, sample_stats):
        """测试JSON报告生成器"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        manager = ConfigManager(str(config_path))
        
        reporter = JSONReporter()
        report = reporter.generate(sample_results, sample_stats, manager)
        
        assert report is not None
        import json
        data = json.loads(report)
        assert 'timestamp' in data
        assert 'filters' in data


class TestIntegration:
    """集成测试"""
    
    def test_full_workflow(self):
        """测试完整工作流"""
        config_path = Path(__file__).parent.parent / 'config' / 'xcn_comm.yaml'
        
        recommender = StockRecommender(str(config_path))
        
        # 测试数据加载
        df = recommender.data_loader.load_data()
        assert df is not None
        assert len(df) > 0
        
        # 测试筛选
        filter_results = recommender.filter_engine.apply_all_filters(df)
        assert filter_results is not None
        assert len(filter_results) > 0
        
        # 测试统计
        stats = recommender.calculate_stats(df)
        assert stats is not None
        assert stats['total_stocks'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
