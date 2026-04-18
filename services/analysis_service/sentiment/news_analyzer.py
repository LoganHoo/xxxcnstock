#!/usr/bin/env python3
"""
新闻情绪分析器
分析新闻、公告等文本的情绪
"""
import logging
from typing import Dict, Any, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SentimentResult:
    """情绪分析结果"""
    score: float  # -1到1，负值表示负面，正值表示正面
    sentiment: str  # 'positive', 'negative', 'neutral'
    confidence: float
    keywords: List[str]


class NewsAnalyzer:
    """
    新闻情绪分析器
    
    基于关键词和规则的情绪分析
    """
    
    # 正面关键词
    POSITIVE_WORDS = [
        '增长', '上涨', '利好', '超预期', '看好', '突破', '创新', '领先',
        '扩张', '提升', '改善', '强劲', '优异', '成功', '合作', '签约',
        '订单', '中标', '获批', '通过', '完成', '达成'
    ]
    
    # 负面关键词
    NEGATIVE_WORDS = [
        '下跌', '亏损', '利空', '不及预期', '看空', '跌破', '下滑',
        '衰退', '恶化', '风险', '问题', '违规', '处罚', '调查',
        '诉讼', '仲裁', '违约', '逾期', '停产', '裁员', '减持'
    ]
    
    # 程度副词
    INTENSIFIERS = ['大幅', '显著', '明显', '严重', '重大', '急剧']
    
    def analyze(self, text: str) -> SentimentResult:
        """
        分析文本情绪
        
        Args:
            text: 待分析文本
        
        Returns:
            情绪分析结果
        """
        if not text:
            return SentimentResult(0.0, 'neutral', 0.0, [])
        
        # 计算关键词匹配
        positive_matches = [w for w in self.POSITIVE_WORDS if w in text]
        negative_matches = [w for w in self.NEGATIVE_WORDS if w in text]
        
        # 计算强度
        intensity = 1.0
        for intensifier in self.INTENSIFIERS:
            if intensifier in text:
                intensity += 0.5
        
        # 计算得分
        positive_score = len(positive_matches) * 0.2 * intensity
        negative_score = len(negative_matches) * 0.2 * intensity
        
        score = positive_score - negative_score
        score = max(-1.0, min(1.0, score))
        
        # 判断情绪
        if score > 0.2:
            sentiment = 'positive'
        elif score < -0.2:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        # 计算置信度
        total_matches = len(positive_matches) + len(negative_matches)
        confidence = min(1.0, total_matches * 0.2 + 0.3)
        
        keywords = positive_matches + negative_matches
        
        return SentimentResult(score, sentiment, confidence, keywords)
    
    def analyze_batch(self, texts: List[str]) -> List[SentimentResult]:
        """
        批量分析文本
        
        Args:
            texts: 文本列表
        
        Returns:
            情绪分析结果列表
        """
        return [self.analyze(text) for text in texts]
    
    def get_market_sentiment(self, news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        获取整体市场情绪
        
        Args:
            news_items: 新闻列表，每项包含 'title' 和 'content'
        
        Returns:
            市场情绪报告
        """
        if not news_items:
            return {'overall': 'neutral', 'score': 0.0, 'confidence': 0.0}
        
        results = []
        for item in news_items:
            text = item.get('title', '') + ' ' + item.get('content', '')
            result = self.analyze(text)
            results.append(result)
        
        # 计算平均分
        avg_score = sum(r.score for r in results) / len(results)
        avg_confidence = sum(r.confidence for r in results) / len(results)
        
        # 统计
        positive_count = sum(1 for r in results if r.sentiment == 'positive')
        negative_count = sum(1 for r in results if r.sentiment == 'negative')
        neutral_count = len(results) - positive_count - negative_count
        
        if avg_score > 0.2:
            overall = 'positive'
        elif avg_score < -0.2:
            overall = 'negative'
        else:
            overall = 'neutral'
        
        return {
            'overall': overall,
            'score': avg_score,
            'confidence': avg_confidence,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'neutral_count': neutral_count,
            'total_count': len(results)
        }
