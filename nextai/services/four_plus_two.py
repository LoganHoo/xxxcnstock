"""4+2 全维度分析体系服务"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime

import polars as pl

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


@dataclass
class DimensionScore:
    dimension: str
    score: float
    weight: float
    signals: Dict[str, bool] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AnalysisResult:
    code: str
    name: str
    total_score: float
    dimensions: List[DimensionScore]
    recommendation: str
    confidence: float
    timestamp: str = ""

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "name": self.name,
            "total_score": round(self.total_score, 2),
            "dimensions": [d.to_dict() for d in self.dimensions],
            "recommendation": self.recommendation,
            "confidence": round(self.confidence, 2),
            "timestamp": self.timestamp,
        }


DIMENSION_WEIGHTS = {
    "macro": 0.10,
    "fundamental": 0.20,
    "technical": 0.25,
    "sentiment": 0.10,
    "fund_behavior": 0.20,
    "ai_catalyst": 0.15,
}


class MacroAnalyzer:
    def analyze(self, kline: pl.DataFrame, market_data: Optional[dict] = None) -> DimensionScore:
        score = 50.0
        signals = {}

        if market_data:
            shibor = market_data.get("shibor_overnight")
            if shibor and shibor < 2.0:
                score += 15
                signals["shibor_loose"] = True
            elif shibor and shibor > 3.0:
                score -= 15
                signals["shibor_tight"] = True

        if kline.height >= 20:
            recent = kline.tail(20)
            avg_volume = recent.select(pl.col("volume").mean()).item()
            latest_volume = recent.select(pl.col("volume").last()).item()
            if latest_volume and avg_volume and latest_volume > avg_volume * 1.2:
                score += 10
                signals["volume_expansion"] = True

        return DimensionScore(
            dimension="macro",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["macro"],
            signals=signals,
        )


class FundamentalAnalyzer:
    def analyze(self, kline: pl.DataFrame, financial_data: Optional[dict] = None) -> DimensionScore:
        score = 50.0
        signals = {}

        if financial_data:
            roe = financial_data.get("roe")
            if roe and roe > 15:
                score += 20
                signals["high_roe"] = True
            elif roe and roe < 5:
                score -= 15
                signals["low_roe"] = True

            gross_margin = financial_data.get("gross_margin")
            if gross_margin and gross_margin > 30:
                score += 10
                signals["high_margin"] = True

            profit_growth = financial_data.get("profit_growth")
            if profit_growth and profit_growth > 20:
                score += 15
                signals["profit_growing"] = True
            elif profit_growth and profit_growth < -10:
                score -= 20
                signals["profit_declining"] = True

        return DimensionScore(
            dimension="fundamental",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["fundamental"],
            signals=signals,
        )


class TechnicalAnalyzer:
    def analyze(self, kline: pl.DataFrame) -> DimensionScore:
        score = 50.0
        signals = {}

        if kline.height < 20:
            return DimensionScore(dimension="technical", score=score, weight=DIMENSION_WEIGHTS["technical"])

        close = kline.select(pl.col("close")).to_series()

        ema5 = close.rolling_mean(window_size=5).tail(1).item()
        ema10 = close.rolling_mean(window_size=10).tail(1).item()
        ema20 = close.rolling_mean(window_size=20).tail(1).item()
        current = close.tail(1).item()

        if ema5 and ema10 and ema20 and current:
            if current > ema5 > ema10 > ema20:
                score += 25
                signals["bullish_alignment"] = True
            elif current < ema5 < ema10 < ema20:
                score -= 20
                signals["bearish_alignment"] = True

            if ema5 > ema10:
                score += 10
                signals["ema5_above_ema10"] = True

        if kline.height >= 12:
            recent_close = close.tail(12)
            ema_fast = recent_close.rolling_mean(window_size=5).tail(1).item()
            ema_slow = recent_close.rolling_mean(window_size=10).tail(1).item()
            prev_fast = close.tail(6).head(1).rolling_mean(window_size=5).tail(1).item() if close.len() >= 6 else None
            if ema_fast and ema_slow and prev_fast:
                if ema_fast > ema_slow and prev_fast <= ema_slow:
                    score += 15
                    signals["macd_golden_cross"] = True

        return DimensionScore(
            dimension="technical",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["technical"],
            signals=signals,
        )


class SentimentAnalyzer:
    def analyze(self, kline: pl.DataFrame, sentiment_data: Optional[dict] = None) -> DimensionScore:
        score = 50.0
        signals = {}

        if sentiment_data:
            bullish_ratio = sentiment_data.get("bullish_ratio", 0.5)
            if bullish_ratio > 0.7:
                score += 20
                signals["bullish_sentiment"] = True
            elif bullish_ratio < 0.3:
                score -= 15
                signals["bearish_sentiment"] = True

            news_score = sentiment_data.get("news_score", 0)
            if news_score > 0.5:
                score += 10
                signals["positive_news"] = True

        return DimensionScore(
            dimension="sentiment",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["sentiment"],
            signals=signals,
        )


class FundBehaviorAnalyzer:
    def analyze(self, kline: pl.DataFrame, fund_data: Optional[dict] = None) -> DimensionScore:
        score = 50.0
        signals = {}

        if fund_data:
            main_net_inflow = fund_data.get("main_net_inflow", 0)
            if main_net_inflow > 50_000_000:
                score += 25
                signals["strong_main_inflow"] = True
            elif main_net_inflow < -50_000_000:
                score -= 20
                signals["strong_main_outflow"] = True

            super_large_ratio = fund_data.get("super_large_ratio", 0)
            if super_large_ratio > 0.3:
                score += 15
                signals["super_large_dominant"] = True

        if kline.height >= 5:
            recent = kline.tail(5)
            volumes = recent.select(pl.col("volume")).to_series()
            avg_vol = volumes.mean()
            latest_vol = volumes.tail(1).item()
            if latest_vol and avg_vol and latest_vol > avg_vol * 2:
                score += 10
                signals["volume_surge"] = True

        return DimensionScore(
            dimension="fund_behavior",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["fund_behavior"],
            signals=signals,
        )


class AICatalystAnalyzer:
    def analyze(self, catalyst_data: Optional[dict] = None) -> DimensionScore:
        score = 50.0
        signals = {}

        if catalyst_data:
            policy_score = catalyst_data.get("policy_score", 0)
            if policy_score > 0.7:
                score += 25
                signals["strong_policy_support"] = True

            sector_heat = catalyst_data.get("sector_heat", 0)
            if sector_heat > 0.6:
                score += 15
                signals["hot_sector"] = True

            news_catalyst = catalyst_data.get("news_catalyst", False)
            if news_catalyst:
                score += 10
                signals["news_catalyst"] = True

        return DimensionScore(
            dimension="ai_catalyst",
            score=min(max(score, 0), 100),
            weight=DIMENSION_WEIGHTS["ai_catalyst"],
            signals=signals,
        )


class FourPlusTwoAnalyzer:
    def __init__(self):
        self.macro = MacroAnalyzer()
        self.fundamental = FundamentalAnalyzer()
        self.technical = TechnicalAnalyzer()
        self.sentiment = SentimentAnalyzer()
        self.fund_behavior = FundBehaviorAnalyzer()
        self.ai_catalyst = AICatalystAnalyzer()

    def analyze(
        self,
        code: str,
        name: str,
        kline: pl.DataFrame,
        market_data: Optional[dict] = None,
        financial_data: Optional[dict] = None,
        sentiment_data: Optional[dict] = None,
        fund_data: Optional[dict] = None,
        catalyst_data: Optional[dict] = None,
    ) -> AnalysisResult:
        dimensions = [
            self.macro.analyze(kline, market_data),
            self.fundamental.analyze(kline, financial_data),
            self.technical.analyze(kline),
            self.sentiment.analyze(kline, sentiment_data),
            self.fund_behavior.analyze(kline, fund_data),
            self.ai_catalyst.analyze(catalyst_data),
        ]

        total_score = sum(d.score * d.weight for d in dimensions)
        confidence = total_score / 100.0

        if total_score >= 80:
            recommendation = "强烈推荐"
        elif total_score >= 70:
            recommendation = "推荐"
        elif total_score >= 60:
            recommendation = "观望"
        elif total_score >= 50:
            recommendation = "谨慎"
        else:
            recommendation = "回避"

        return AnalysisResult(
            code=code,
            name=name,
            total_score=total_score,
            dimensions=dimensions,
            recommendation=recommendation,
            confidence=confidence,
            timestamp=datetime.now().isoformat(),
        )
