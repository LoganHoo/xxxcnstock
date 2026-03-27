import asyncio
from typing import List, Dict
import logging

from core.models import StockScore, StockSelectionSignal, SignalLevel
from core.logger import setup_logger
from services.stock_service.filters.fundamental import FundamentalFilter
from services.stock_service.filters.volume_price import VolumePriceFilter
from services.stock_service.scorer import StockScorer

logger = setup_logger("stock_engine", log_file="signals/stock_engine.log")


class StockSelectionEngine:
    """选股引擎"""
    
    def __init__(self):
        self.fundamental_filter = FundamentalFilter()
        self.volume_price_filter = VolumePriceFilter()
        self.scorer = StockScorer()
    
    async def analyze_stock(
        self,
        code: str,
        name: str,
        fundamental_data: Dict,
        kline_data,
        fund_flow_data: Dict,
        sentiment_data: Dict
    ) -> StockSelectionSignal:
        """
        分析单只股票
        
        Returns:
            选股信号
        """
        # 基本面筛选
        fund_result = self.fundamental_filter.filter(fundamental_data)
        fundamental_score = fund_result["score"]
        reasons = fund_result["reasons"]
        
        # 量价筛选
        vp_result = self.volume_price_filter.filter(kline_data)
        volume_price_score = vp_result["score"]
        reasons.extend(vp_result["reasons"])
        
        # 资金流向评分（简化）
        fund_flow_score = self._calculate_fund_flow_score(fund_flow_data)
        
        # 情绪评分（简化）
        sentiment_score = self._calculate_sentiment_score(sentiment_data)
        
        # 综合评分
        score = self.scorer.create_score(
            code=code,
            name=name,
            fundamental_score=fundamental_score,
            volume_price_score=volume_price_score,
            fund_flow_score=fund_flow_score,
            sentiment_score=sentiment_score,
            reasons=reasons
        )
        
        # 确定信号等级
        signal_level = self._determine_signal_level(score.total_score)
        
        return StockSelectionSignal(
            code=code,
            name=name,
            score=score,
            current_price=fundamental_data.get("price", 0),
            change_pct=fundamental_data.get("change_pct", 0),
            signal_level=signal_level,
            reasons=reasons
        )
    
    def _calculate_fund_flow_score(self, data: Dict) -> float:
        """计算资金流向评分"""
        if not data:
            return 50.0
        
        score = 50.0
        
        # 主力净流入
        if data.get("main_net_inflow", 0) > 0:
            score += 20
        
        # 北向资金
        if data.get("north_bound_days", 0) >= 3:
            score += 15
        
        # 大单净比
        if data.get("big_order_ratio", 0) > 0:
            score += 15
        
        return min(score, 100)
    
    def _calculate_sentiment_score(self, data: Dict) -> float:
        """计算情绪评分"""
        if not data:
            return 50.0
        
        score = 50.0
        
        # 板块热度
        if data.get("sector_rank", 999) <= 5:
            score += 20
        
        # 换手率
        turnover = data.get("turnover_rate", 0)
        if 3 <= turnover <= 15:
            score += 15
        
        # 市场情绪
        if data.get("market_up", False):
            score += 15
        
        return min(score, 100)
    
    def _determine_signal_level(self, total_score: float) -> SignalLevel:
        """确定信号等级"""
        if total_score >= 80:
            return SignalLevel.S
        elif total_score >= 70:
            return SignalLevel.A
        elif total_score >= 60:
            return SignalLevel.B
        else:
            return SignalLevel.C
    
    async def screen_stocks(
        self,
        stock_list: List[Dict],
        min_score: float = 60.0
    ) -> List[StockSelectionSignal]:
        """
        批量筛选股票
        
        Args:
            stock_list: 股票列表，每个包含 code, name, fundamental_data 等
            min_score: 最低评分阈值
        Returns:
            符合条件的选股信号列表
        """
        results = []
        
        for stock in stock_list:
            try:
                signal = await self.analyze_stock(
                    code=stock["code"],
                    name=stock["name"],
                    fundamental_data=stock.get("fundamental", {}),
                    kline_data=stock.get("kline"),
                    fund_flow_data=stock.get("fund_flow", {}),
                    sentiment_data=stock.get("sentiment", {})
                )
                
                if signal.score.total_score >= min_score:
                    results.append(signal)
                    
            except Exception as e:
                logger.error(f"分析股票失败: {stock.get('code')}, {e}")
        
        # 按评分排序
        results.sort(key=lambda x: x.score.total_score, reverse=True)
        
        logger.info(f"筛选完成: 输入{len(stock_list)}只, 输出{len(results)}只")
        return results
