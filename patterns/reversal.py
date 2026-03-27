"""
反转形态识别
"""
import polars as pl
from typing import Optional, Dict, Any, List
from patterns.base_pattern import (
    BasePattern, PatternResult, PatternRegistry,
    SignalType, PatternStrength
)


@PatternRegistry.register
class BullishEngulfingPattern(BasePattern):
    """看涨吞没形态"""
    
    name = "bullish_engulfing"
    description = "看涨吞没 - 阳线完全吞没前一根阴线"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_ratio = params.get('body_ratio', 1.0)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        is_bullish_engulfing = (
            prev_props['is_bearish'] and
            curr_props['is_bullish'] and
            curr_props['open'] <= prev_props['close'] and
            curr_props['close'] >= prev_props['open'] and
            curr_props['body'] >= prev_props['body'] * self.body_ratio
        )
        
        if is_bullish_engulfing:
            prev_rows = df.head(len(df) - 2).tail(5)
            is_downtrend = True
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_downtrend = prev_close[-1] < prev_close[0] if len(prev_close) > 1 else True
            
            strength = PatternStrength.STRONG if is_downtrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=strength,
                confidence=0.8 if is_downtrend else 0.65,
                description="看涨吞没形态，阳线完全吞没前一根阴线",
                details={
                    'prev_body': prev_props['body'],
                    'curr_body': curr_props['body'],
                    'in_downtrend': is_downtrend
                }
            )
        
        return None


@PatternRegistry.register
class BearishEngulfingPattern(BasePattern):
    """看跌吞没形态"""
    
    name = "bearish_engulfing"
    description = "看跌吞没 - 阴线完全吞没前一根阳线"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_ratio = params.get('body_ratio', 1.0)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        is_bearish_engulfing = (
            prev_props['is_bullish'] and
            curr_props['is_bearish'] and
            curr_props['open'] >= prev_props['close'] and
            curr_props['close'] <= prev_props['open'] and
            curr_props['body'] >= prev_props['body'] * self.body_ratio
        )
        
        if is_bearish_engulfing:
            prev_rows = df.head(len(df) - 2).tail(5)
            is_uptrend = True
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_uptrend = prev_close[-1] > prev_close[0] if len(prev_close) > 1 else True
            
            strength = PatternStrength.STRONG if is_uptrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=strength,
                confidence=0.8 if is_uptrend else 0.65,
                description="看跌吞没形态，阴线完全吞没前一根阳线",
                details={
                    'prev_body': prev_props['body'],
                    'curr_body': curr_props['body'],
                    'in_uptrend': is_uptrend
                }
            )
        
        return None


@PatternRegistry.register
class BullishHaramiPattern(BasePattern):
    """看涨孕线形态"""
    
    name = "bullish_harami"
    description = "看涨孕线 - 小阳线在前一根大阴线实体内"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_ratio = params.get('body_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        is_bullish_harami = (
            prev_props['is_bearish'] and
            curr_props['is_bullish'] and
            curr_props['open'] >= prev_props['close'] and
            curr_props['close'] <= prev_props['open'] and
            curr_props['body'] <= prev_props['body'] * self.body_ratio
        )
        
        if is_bullish_harami:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.MODERATE,
                confidence=0.65,
                description="看涨孕线形态，小阳线在前一根大阴线实体内",
                details={
                    'prev_body': prev_props['body'],
                    'curr_body': curr_props['body']
                }
            )
        
        return None


@PatternRegistry.register
class BearishHaramiPattern(BasePattern):
    """看跌孕线形态"""
    
    name = "bearish_harami"
    description = "看跌孕线 - 小阴线在前一根大阳线实体内"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_ratio = params.get('body_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        is_bearish_harami = (
            prev_props['is_bullish'] and
            curr_props['is_bearish'] and
            curr_props['open'] <= prev_props['close'] and
            curr_props['close'] >= prev_props['open'] and
            curr_props['body'] <= prev_props['body'] * self.body_ratio
        )
        
        if is_bearish_harami:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.MODERATE,
                confidence=0.65,
                description="看跌孕线形态，小阴线在前一根大阳线实体内",
                details={
                    'prev_body': prev_props['body'],
                    'curr_body': curr_props['body']
                }
            )
        
        return None


@PatternRegistry.register
class MorningStarPattern(BasePattern):
    """早晨之星形态"""
    
    name = "morning_star"
    description = "早晨之星 - 三根K线组成的底部反转形态"
    min_periods = 3
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.gap_threshold = params.get('gap_threshold', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 3:
            return None
        
        rows = df.tail(3).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        first = props[0]
        second = props[1]
        third = props[2]
        
        is_morning_star = (
            first['is_bearish'] and
            first['body_ratio'] > 0.3 and
            second['body_ratio'] < 0.3 and
            second['low'] < first['close'] and
            third['is_bullish'] and
            third['close'] > (first['open'] + first['close']) / 2
        )
        
        if is_morning_star:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.VERY_STRONG,
                confidence=0.85,
                description="早晨之星形态，底部反转信号",
                details={
                    'first_body_ratio': first['body_ratio'],
                    'second_body_ratio': second['body_ratio'],
                    'third_body_ratio': third['body_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class EveningStarPattern(BasePattern):
    """黄昏之星形态"""
    
    name = "evening_star"
    description = "黄昏之星 - 三根K线组成的顶部反转形态"
    min_periods = 3
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.gap_threshold = params.get('gap_threshold', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 3:
            return None
        
        rows = df.tail(3).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        first = props[0]
        second = props[1]
        third = props[2]
        
        is_evening_star = (
            first['is_bullish'] and
            first['body_ratio'] > 0.3 and
            second['body_ratio'] < 0.3 and
            second['high'] > first['close'] and
            third['is_bearish'] and
            third['close'] < (first['open'] + first['close']) / 2
        )
        
        if is_evening_star:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.VERY_STRONG,
                confidence=0.85,
                description="黄昏之星形态，顶部反转信号",
                details={
                    'first_body_ratio': first['body_ratio'],
                    'second_body_ratio': second['body_ratio'],
                    'third_body_ratio': third['body_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class ThreeWhiteSoldiersPattern(BasePattern):
    """三白兵形态"""
    
    name = "three_white_soldiers"
    description = "三白兵 - 连续三根上涨阳线"
    min_periods = 3
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_body_ratio = params.get('min_body_ratio', 0.5)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 3:
            return None
        
        rows = df.tail(3).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        is_three_white_soldiers = all([
            p['is_bullish'] and
            p['body_ratio'] > self.min_body_ratio and
            p['close'] > props[i]['close']
            for i, p in enumerate(props)
        ])
        
        if is_three_white_soldiers:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.VERY_STRONG,
                confidence=0.85,
                description="三白兵形态，强势上涨信号",
                details={
                    'closes': [p['close'] for p in props],
                    'body_ratios': [p['body_ratio'] for p in props]
                }
            )
        
        return None


@PatternRegistry.register
class ThreeBlackCrowsPattern(BasePattern):
    """三只乌鸦形态"""
    
    name = "three_black_crows"
    description = "三只乌鸦 - 连续三根下跌阴线"
    min_periods = 3
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_body_ratio = params.get('min_body_ratio', 0.5)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 3:
            return None
        
        rows = df.tail(3).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        is_three_black_crows = all([
            p['is_bearish'] and
            p['body_ratio'] > self.min_body_ratio and
            p['close'] < props[i]['close']
            for i, p in enumerate(props)
        ])
        
        if is_three_black_crows:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.VERY_STRONG,
                confidence=0.85,
                description="三只乌鸦形态，强势下跌信号",
                details={
                    'closes': [p['close'] for p in props],
                    'body_ratios': [p['body_ratio'] for p in props]
                }
            )
        
        return None


@PatternRegistry.register
class TweezerTopPattern(BasePattern):
    """镊子顶形态"""
    
    name = "tweezer_top"
    description = "镊子顶 - 两根K线的高点相同或相近"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.high_threshold = params.get('high_threshold', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        high_diff = abs(prev_props['high'] - curr_props['high']) / prev_props['high']
        
        is_tweezer_top = (
            high_diff <= self.high_threshold and
            curr_props['is_bearish']
        )
        
        if is_tweezer_top:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.75,
                description=f"镊子顶形态，高点差异{high_diff:.2%}",
                details={
                    'prev_high': prev_props['high'],
                    'curr_high': curr_props['high'],
                    'high_diff': high_diff
                }
            )
        
        return None


@PatternRegistry.register
class TweezerBottomPattern(BasePattern):
    """镊子底形态"""
    
    name = "tweezer_bottom"
    description = "镊子底 - 两根K线的低点相同或相近"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.low_threshold = params.get('low_threshold', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        low_diff = abs(prev_props['low'] - curr_props['low']) / prev_props['low']
        
        is_tweezer_bottom = (
            low_diff <= self.low_threshold and
            curr_props['is_bullish']
        )
        
        if is_tweezer_bottom:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.STRONG,
                confidence=0.75,
                description=f"镊子底形态，低点差异{low_diff:.2%}",
                details={
                    'prev_low': prev_props['low'],
                    'curr_low': curr_props['low'],
                    'low_diff': low_diff
                }
            )
        
        return None
