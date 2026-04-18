#!/usr/bin/env python3
"""
情绪分析模块测试
"""
import pytest


class TestDeepSeekClient:
    """DeepSeek客户端测试"""
    
    def test_deepseek_client_creation(self):
        """测试客户端创建"""
        from services.analysis_service.sentiment.deepseek_client import DeepSeekClient
        client = DeepSeekClient(api_key='test_key')
        assert client is not None
    
    def test_report_analysis(self):
        """测试研报分析"""
        from services.analysis_service.sentiment.deepseek_client import DeepSeekClient
        client = DeepSeekClient()
        result = client.analyze_report('该公司业绩大幅增长，前景看好')
        
        assert 'bullish_probability' in result
        assert 'sentiment' in result
    
    def test_text_analysis(self):
        """测试文本分析"""
        from services.analysis_service.sentiment.deepseek_client import DeepSeekClient
        client = DeepSeekClient()
        result = client.analyze_text('该公司业绩大幅增长，前景看好')
        
        assert result['score'] > 0
        assert result['sentiment'] == 'positive'


class TestNewsAnalyzer:
    """新闻分析器测试"""
    
    def test_positive_sentiment(self):
        """测试正面情绪"""
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        result = analyzer.analyze('该公司业绩大幅增长，订单饱满，前景看好')
        
        assert result.sentiment == 'positive'
        assert result.score > 0
    
    def test_negative_sentiment(self):
        """测试负面情绪"""
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        result = analyzer.analyze('该公司业绩下滑，面临亏损风险，前景堪忧')
        
        assert result.sentiment == 'negative'
        assert result.score < 0
    
    def test_neutral_sentiment(self):
        """测试中性情绪"""
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        result = analyzer.analyze('该公司今日发布公告')
        
        assert result.sentiment == 'neutral'
    
    def test_batch_analysis(self):
        """测试批量分析"""
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        texts = ['业绩增长', '业绩下滑', '正常经营']
        results = analyzer.analyze_batch(texts)
        
        assert len(results) == 3
    
    def test_market_sentiment(self):
        """测试市场情绪"""
        from services.analysis_service.sentiment.news_analyzer import NewsAnalyzer
        analyzer = NewsAnalyzer()
        
        news_items = [
            {'title': '业绩大幅增长', 'content': '订单饱满'},
            {'title': '新产品发布', 'content': '技术领先'}
        ]
        
        result = analyzer.get_market_sentiment(news_items)
        
        assert 'overall' in result
        assert 'score' in result
        assert result['total_count'] == 2
