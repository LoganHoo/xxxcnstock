"""
K线形态识别引擎
整合所有形态识别功能
"""
import polars as pl
from typing import Dict, List, Optional, Any
import logging

from patterns.base_pattern import (
    BasePattern, PatternResult, PatternRegistry,
    SignalType, PatternStrength
)

from patterns.candlestick import (
    DojiPattern, HammerPattern, InvertedHammerPattern,
    HangingManPattern, ShootingStarPattern, MarubozuPattern,
    SpinningTopPattern
)
from patterns.reversal import (
    BullishEngulfingPattern, BearishEngulfingPattern,
    BullishHaramiPattern, BearishHaramiPattern,
    MorningStarPattern, EveningStarPattern,
    ThreeWhiteSoldiersPattern, ThreeBlackCrowsPattern,
    TweezerTopPattern, TweezerBottomPattern
)
from patterns.continuation import (
    RisingWindowPattern, FallingWindowPattern,
    RisingThreeMethodsPattern, FallingThreeMethodsPattern,
    SeparatingLinesBullishPattern, SeparatingLinesBearishPattern,
    UpsideGapTwoCrowsPattern, MeetingLinesBullishPattern,
    MeetingLinesBearishPattern
)
from patterns.special import (
    LimitUpPattern, LimitDownPattern, TShapePattern,
    InvertedTPattern, OneWordBoardPattern, LongLeggedDojiPattern,
    DragonflyDojiPattern, GravestoneDojiPattern, NearLimitUpPattern
)

logger = logging.getLogger(__name__)


class PatternEngine:
    """K线形态识别引擎"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.patterns: Dict[str, BasePattern] = {}
        self._load_patterns()
    
    def _load_patterns(self):
        """加载所有已注册的形态"""
        enabled_patterns = self.config.get('enabled_patterns', None)
        
        for class_name, pattern_class in PatternRegistry.get_all_patterns().items():
            if enabled_patterns is None or class_name in enabled_patterns:
                pattern_config = self.config.get('patterns', {}).get(class_name, {})
                instance = pattern_class(pattern_config)
                self.patterns[instance.name] = instance
    
    def detect_single_pattern(
        self, 
        df: pl.DataFrame, 
        pattern_name: str
    ) -> Optional[PatternResult]:
        """检测单个形态"""
        pattern = self.patterns.get(pattern_name)
        if pattern:
            return pattern.detect(df)
        return None
    
    def detect_all_patterns(
        self, 
        df: pl.DataFrame,
        min_confidence: float = 0.5
    ) -> List[PatternResult]:
        """检测所有形态"""
        results = []
        
        for name, pattern in self.patterns.items():
            try:
                result = pattern.detect(df)
                if result and result.confidence >= min_confidence:
                    results.append(result)
            except Exception as e:
                logger.debug(f"形态 {name} 检测失败: {e}")
        
        return results
    
    def get_strongest_signal(
        self, 
        df: pl.DataFrame
    ) -> Optional[PatternResult]:
        """获取最强信号"""
        results = self.detect_all_patterns(df)
        
        if not results:
            return None
        
        scored_results = []
        for r in results:
            score = (
                r.confidence * 
                r.strength.value * 
                (1.5 if r.signal != SignalType.NEUTRAL else 1.0)
            )
            scored_results.append((score, r))
        
        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        return scored_results[0][1]
    
    def get_bullish_patterns(
        self, 
        df: pl.DataFrame,
        min_confidence: float = 0.6
    ) -> List[PatternResult]:
        """获取看涨形态"""
        results = self.detect_all_patterns(df, min_confidence)
        return [r for r in results if r.signal == SignalType.BULLISH]
    
    def get_bearish_patterns(
        self, 
        df: pl.DataFrame,
        min_confidence: float = 0.6
    ) -> List[PatternResult]:
        """获取看跌形态"""
        results = self.detect_all_patterns(df, min_confidence)
        return [r for r in results if r.signal == SignalType.BEARISH]
    
    def analyze(
        self, 
        df: pl.DataFrame,
        min_confidence: float = 0.5
    ) -> Dict[str, Any]:
        """
        综合分析K线形态
        
        Args:
            df: K线数据
            min_confidence: 最小置信度阈值
        
        Returns:
            分析结果字典
        """
        results = self.detect_all_patterns(df, min_confidence)
        
        bullish = [r for r in results if r.signal == SignalType.BULLISH]
        bearish = [r for r in results if r.signal == SignalType.BEARISH]
        neutral = [r for r in results if r.signal == SignalType.NEUTRAL]
        
        bullish_score = sum(r.confidence * r.strength.value for r in bullish)
        bearish_score = sum(r.confidence * r.strength.value for r in bearish)
        
        total_score = bullish_score + bearish_score
        
        if total_score > 0 and bullish_score > bearish_score * 1.5:
            overall_signal = SignalType.BULLISH
            strength_value = min(4, int(bullish_score / total_score * 4) + 1)
            overall_strength = PatternStrength(strength_value)
        elif total_score > 0 and bearish_score > bullish_score * 1.5:
            overall_signal = SignalType.BEARISH
            strength_value = min(4, int(bearish_score / total_score * 4) + 1)
            overall_strength = PatternStrength(strength_value)
        else:
            overall_signal = SignalType.NEUTRAL
            overall_strength = PatternStrength.WEAK
        
        return {
            'overall_signal': overall_signal.value,
            'overall_strength': overall_strength.value,
            'bullish_score': bullish_score,
            'bearish_score': bearish_score,
            'bullish_patterns': [
                {
                    'name': r.pattern_name,
                    'strength': r.strength.value,
                    'confidence': r.confidence,
                    'description': r.description
                }
                for r in sorted(bullish, key=lambda x: x.confidence * x.strength.value, reverse=True)
            ],
            'bearish_patterns': [
                {
                    'name': r.pattern_name,
                    'strength': r.strength.value,
                    'confidence': r.confidence,
                    'description': r.description
                }
                for r in sorted(bearish, key=lambda x: x.confidence * x.strength.value, reverse=True)
            ],
            'neutral_patterns': [
                {
                    'name': r.pattern_name,
                    'strength': r.strength.value,
                    'confidence': r.confidence,
                    'description': r.description
                }
                for r in neutral
            ],
            'total_patterns_found': len(results)
        }
    
    def get_pattern_info(self, pattern_name: str) -> Optional[Dict[str, Any]]:
        """获取形态信息"""
        pattern = self.patterns.get(pattern_name)
        if pattern:
            return {
                'name': pattern.name,
                'description': pattern.description,
                'min_periods': pattern.min_periods,
                'params': pattern.params
            }
        return None
    
    def list_available_patterns(self) -> List[Dict[str, Any]]:
        """列出所有可用形态"""
        return [
            {
                'name': p.name,
                'description': p.description,
                'min_periods': p.min_periods
            }
            for p in self.patterns.values()
        ]
    
    def batch_analyze(
        self,
        stock_data: Dict[str, pl.DataFrame],
        min_confidence: float = 0.5
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量分析多只股票
        
        Args:
            stock_data: 股票代码到K线数据的映射
            min_confidence: 最小置信度阈值
        
        Returns:
            股票代码到分析结果的映射
        """
        results = {}
        
        for code, df in stock_data.items():
            try:
                results[code] = self.analyze(df, min_confidence)
            except Exception as e:
                logger.warning(f"股票 {code} 分析失败: {e}")
                results[code] = {'error': str(e)}
        
        return results
