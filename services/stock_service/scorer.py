from typing import Dict
from core.models import StockScore


class StockScorer:
    """股票综合评分器
    
    基于第一性原理：
    股票价值 = 内在价值(35%) + 技术面(25%) + 资金面(25%) + 情绪面(15%)
    """
    
    # 维度权重
    WEIGHTS = {
        "fundamental": 0.35,
        "volume_price": 0.25,
        "fund_flow": 0.25,
        "sentiment": 0.15
    }
    
    def calculate_total_score(
        self,
        fundamental_score: float,
        volume_price_score: float,
        fund_flow_score: float,
        sentiment_score: float
    ) -> float:
        """计算综合评分"""
        total = (
            fundamental_score * self.WEIGHTS["fundamental"] +
            volume_price_score * self.WEIGHTS["volume_price"] +
            fund_flow_score * self.WEIGHTS["fund_flow"] +
            sentiment_score * self.WEIGHTS["sentiment"]
        )
        return round(total, 2)
    
    def create_score(
        self,
        code: str,
        name: str,
        fundamental_score: float = 0,
        volume_price_score: float = 0,
        fund_flow_score: float = 0,
        sentiment_score: float = 0,
        reasons: list = None
    ) -> StockScore:
        """创建股票评分对象"""
        total = self.calculate_total_score(
            fundamental_score,
            volume_price_score,
            fund_flow_score,
            sentiment_score
        )
        
        return StockScore(
            code=code,
            name=name,
            total_score=total,
            fundamental_score=fundamental_score,
            volume_price_score=volume_price_score,
            fund_flow_score=fund_flow_score,
            sentiment_score=sentiment_score,
            reasons=reasons or []
        )
