#!/usr/bin/env python3
"""
公告数据服务单元测试

测试内容:
- 公告数据获取器
- 公告分类器
- 重要性评估
- 数据存储
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch


class TestAnnouncementFetcher:
    """公告数据获取器测试"""
    
    @pytest.fixture
    def mock_akshare(self):
        """模拟AKShare响应"""
        mock = Mock()
        mock.return_value = pd.DataFrame({
            '代码': ['000001', '000002'],
            '名称': ['平安银行', '万科A'],
            '公告标题': ['2024年第一季度报告', '关于股东增持股份的公告'],
            '公告时间': ['2024-04-19 15:30:00', '2024-04-19 16:00:00'],
            '公告类型': ['定期报告', '股权变动'],
            '公告内容': ['公司一季度净利润同比增长20%', '股东增持100万股'],
        })
        return mock
    
    def test_fetch_announcements_success(self, mock_akshare):
        """测试成功获取公告数据"""
        from services.data_service.fetchers.announcement.announcement_fetcher import AnnouncementFetcher
        
        fetcher = AnnouncementFetcher()
        
        with patch('akshare.stock_notice_report', mock_akshare):
            df = fetcher.fetch_announcements('000001', '20240419', '20240419')
            
            assert not df.empty
            assert 'code' in df.columns or '代码' in df.columns
    
    def test_fetch_announcements_empty_response(self):
        """测试空响应处理"""
        from services.data_service.fetchers.announcement.announcement_fetcher import AnnouncementFetcher
        
        fetcher = AnnouncementFetcher()
        
        with patch('akshare.stock_notice_report', return_value=pd.DataFrame()):
            df = fetcher.fetch_announcements('000001', '20240419', '20240419')
            
            assert df.empty
    
    def test_fetch_earnings_forecast(self):
        """测试获取业绩预告"""
        from services.data_service.fetchers.announcement.announcement_fetcher import AnnouncementFetcher
        
        fetcher = AnnouncementFetcher()
        
        mock_data = pd.DataFrame({
            '代码': ['000001'],
            '名称': ['平安银行'],
            '预告类型': ['预增'],
            '预告内容': ['净利润同比增长50%-80%'],
            '净利润下限': [1000000000],
            '净利润上限': [1200000000],
        })
        
        with patch('akshare.stock_yjyg_em', return_value=mock_data):
            df = fetcher.fetch_earnings_forecast('20240101', '20240419')
            
            assert not df.empty
    
    def test_fetch_major_events(self):
        """测试获取重大事项"""
        from services.data_service.fetchers.announcement.announcement_fetcher import AnnouncementFetcher
        
        fetcher = AnnouncementFetcher()
        
        mock_data = pd.DataFrame({
            '代码': ['000001'],
            '名称': ['平安银行'],
            '事件类型': ['并购重组'],
            '事件描述': ['拟收购某金融科技公司'],
        })
        
        with patch('akshare.stock_notice_report', return_value=mock_data):
            df = fetcher.fetch_major_events('20240101', '20240419')
            
            assert not df.empty


class TestAnnouncementClassifier:
    """公告分类器测试"""
    
    def test_classify_by_keywords(self):
        """测试基于关键词的分类"""
        from services.data_service.processors.announcement.classifier import AnnouncementClassifier
        
        classifier = AnnouncementClassifier()
        
        test_cases = [
            ('2024年第一季度报告', 'regular_report'),
            ('关于股东增持股份的公告', 'equity_change'),
            ('关于重大资产重组的公告', 'major_event'),
            ('2023年年度报告', 'regular_report'),
            ('关于董事辞职的公告', 'management_change'),
        ]
        
        for title, expected_type in test_cases:
            result = classifier.classify_by_keywords(title)
            assert result == expected_type, f"'{title}' 应分类为 {expected_type}"
    
    def test_classify_earnings_forecast(self):
        """测试业绩预告分类"""
        from services.data_service.processors.announcement.classifier import AnnouncementClassifier
        
        classifier = AnnouncementClassifier()
        
        forecast_types = [
            ('预增', 'positive'),
            ('预减', 'negative'),
            ('扭亏', 'positive'),
            ('预亏', 'negative'),
            ('续盈', 'neutral'),
        ]
        
        for forecast_type, expected in forecast_types:
            result = classifier.classify_earnings_forecast(forecast_type)
            assert result == expected
    
    def test_batch_classify(self):
        """测试批量分类"""
        from services.data_service.processors.announcement.classifier import AnnouncementClassifier
        
        classifier = AnnouncementClassifier()
        
        df = pd.DataFrame({
            'title': [
                '2024年第一季度报告',
                '关于股东增持股份的公告',
                '关于重大资产重组的公告'
            ]
        })
        
        result = classifier.batch_classify(df)
        
        assert 'category' in result.columns
        assert len(result) == 3


class TestImportanceEvaluator:
    """重要性评估器测试"""
    
    def test_evaluate_regular_report(self):
        """测试定期报告重要性评估"""
        from services.data_service.processors.announcement.importance_evaluator import ImportanceEvaluator
        
        evaluator = ImportanceEvaluator()
        
        announcement = {
            'title': '2024年第一季度报告',
            'category': 'regular_report',
            'content': '净利润同比增长50%'
        }
        
        score = evaluator.evaluate(announcement)
        
        assert score > 0
        assert score <= 100
    
    def test_evaluate_earnings_forecast(self):
        """测试业绩预告重要性评估"""
        from services.data_service.processors.announcement.importance_evaluator import ImportanceEvaluator
        
        evaluator = ImportanceEvaluator()
        
        # 预增50%以上 - 高重要性
        announcement = {
            'title': '2024年第一季度业绩预告',
            'category': 'earnings_forecast',
            'forecast_type': '预增',
            'profit_change_min': 50,
            'profit_change_max': 80
        }
        
        score = evaluator.evaluate(announcement)
        
        assert score >= 70  # 预增50%以上应该是高重要性
    
    def test_evaluate_major_event(self):
        """测试重大事项重要性评估"""
        from services.data_service.processors.announcement.importance_evaluator import ImportanceEvaluator
        
        evaluator = ImportanceEvaluator()
        
        # 并购重组 - 最高重要性
        announcement = {
            'title': '关于重大资产重组的公告',
            'category': 'major_event',
            'event_type': '并购重组'
        }
        
        score = evaluator.evaluate(announcement)
        
        assert score >= 80  # 并购重组应该是最高重要性
    
    def test_evaluate_equity_change(self):
        """测试股权变动重要性评估"""
        from services.data_service.processors.announcement.importance_evaluator import ImportanceEvaluator
        
        evaluator = ImportanceEvaluator()
        
        # 大股东增持
        announcement = {
            'title': '关于大股东增持股份的公告',
            'category': 'equity_change',
            'change_type': '增持',
            'change_amount': 1000000,
            'change_percent': 0.5
        }
        
        score = evaluator.evaluate(announcement)
        
        assert score > 0


class TestAnnouncementStorage:
    """公告数据存储测试"""
    
    def test_save_and_load_announcements(self, tmp_path):
        """测试公告数据存取"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        storage = FinancialStorageManager(base_path=tmp_path)
        
        df = pd.DataFrame({
            'code': ['000001', '000001'],
            'announce_date': ['2024-04-19', '2024-04-18'],
            'title': ['第一季度报告', '股东增持公告'],
            'category': ['regular_report', 'equity_change'],
            'importance': [80, 60]
        })
        
        # 保存
        storage.save_announcements('000001', df)
        
        # 加载
        loaded = storage.load_announcements('000001')
        
        assert not loaded.empty
        assert len(loaded) == 2
    
    def test_query_by_date_range(self, tmp_path):
        """测试按日期范围查询"""
        from services.data_service.storage.financial_storage import FinancialStorageManager
        
        storage = FinancialStorageManager(base_path=tmp_path)
        
        df = pd.DataFrame({
            'code': ['000001', '000001', '000001'],
            'announce_date': ['2024-04-19', '2024-04-18', '2024-04-17'],
            'title': ['公告1', '公告2', '公告3'],
            'category': ['regular_report', 'equity_change', 'major_event'],
            'importance': [80, 60, 90]
        })
        
        storage.save_announcements('000001', df)
        
        # 查询最近2天的公告
        loaded = storage.load_announcements('000001', days=2)
        
        assert len(loaded) <= 2


class TestAnnouncementTask:
    """公告更新任务测试"""
    
    def test_daily_update(self):
        """测试每日更新任务"""
        from services.data_service.tasks.announcement_task import AnnouncementUpdateTask
        
        task = AnnouncementUpdateTask()
        task.fetcher = Mock()
        task.fetcher.fetch_announcements.return_value = pd.DataFrame({
            'code': ['000001'],
            'title': ['测试公告']
        })
        task.classifier = Mock()
        task.classifier.batch_classify.return_value = pd.DataFrame({
            'code': ['000001'],
            'title': ['测试公告'],
            'category': ['regular_report']
        })
        task.evaluator = Mock()
        task.evaluator.evaluate.return_value = 80
        
        result = task.run_daily_update(watch_list=['000001'])
        
        assert result is not None
    
    def test_classify_announcements(self):
        """测试公告分类"""
        from services.data_service.tasks.announcement_task import AnnouncementUpdateTask
        
        task = AnnouncementUpdateTask()
        task.classifier = Mock()
        task.classifier.batch_classify.return_value = pd.DataFrame({
            'title': ['第一季度报告'],
            'category': ['regular_report']
        })
        
        df = pd.DataFrame({
            'title': ['第一季度报告']
        })
        
        result = task._classify_announcements(df)
        
        assert 'category' in result.columns


class TestAnnouncementValidation:
    """公告数据验证测试"""
    
    def test_announcement_data_validation(self):
        """测试公告数据验证"""
        from services.data_service.quality.financial.financial_validator import FinancialValidator
        
        validator = FinancialValidator()
        
        # 正常数据
        valid_data = pd.DataFrame({
            'code': ['000001'],
            'announce_date': ['2024-04-19'],
            'title': ['测试公告'],
            'category': ['regular_report']
        })
        
        result = validator.validate_announcement_data(valid_data)
        assert result['valid']
        
        # 缺少必需字段
        invalid_data = pd.DataFrame({
            'code': ['000001'],
            'announce_date': ['2024-04-19']
            # 缺少title字段
        })
        
        result = validator.validate_announcement_data(invalid_data)
        assert not result['valid']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
