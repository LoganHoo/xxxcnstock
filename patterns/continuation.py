"""
持续形态识别
"""
import polars as pl
from typing import Optional, Dict, Any, List
from patterns.base_pattern import (
    BasePattern, PatternResult, PatternRegistry,
    SignalType, PatternStrength
)


@PatternRegistry.register
class RisingWindowPattern(BasePattern):
    """上升窗口（向上跳空缺口）"""
    
    name = "rising_window"
    description = "上升窗口 - 当前K线最低价高于前一根K线最高价"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_gap_ratio = params.get('min_gap_ratio', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        gap = curr_props['low'] - prev_props['high']
        gap_ratio = gap / prev_props['high']
        
        is_rising_window = (
            gap > 0 and
            gap_ratio >= self.min_gap_ratio
        )
        
        if is_rising_window:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description=f"上升窗口，跳空{gap_ratio:.2%}",
                details={
                    'gap': gap,
                    'gap_ratio': gap_ratio,
                    'prev_high': prev_props['high'],
                    'curr_low': curr_props['low']
                }
            )
        
        return None


@PatternRegistry.register
class FallingWindowPattern(BasePattern):
    """下降窗口（向下跳空缺口）"""
    
    name = "falling_window"
    description = "下降窗口 - 当前K线最高价低于前一根K线最低价"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_gap_ratio = params.get('min_gap_ratio', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        gap = prev_props['low'] - curr_props['high']
        gap_ratio = gap / prev_props['low']
        
        is_falling_window = (
            gap > 0 and
            gap_ratio >= self.min_gap_ratio
        )
        
        if is_falling_window:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description=f"下降窗口，跳空{gap_ratio:.2%}",
                details={
                    'gap': gap,
                    'gap_ratio': gap_ratio,
                    'prev_low': prev_props['low'],
                    'curr_high': curr_props['high']
                }
            )
        
        return None


@PatternRegistry.register
class RisingThreeMethodsPattern(BasePattern):
    """上升三法形态"""
    
    name = "rising_three_methods"
    description = "上升三法 - 长阳线后跟三根小阴线，再跟一根长阳线"
    min_periods = 5
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_body_ratio = params.get('min_body_ratio', 0.5)
        self.max_small_body_ratio = params.get('max_small_body_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 5:
            return None
        
        rows = df.tail(5).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        first = props[0]
        middle = props[1:4]
        last = props[4]
        
        is_rising_three = (
            first['is_bullish'] and
            first['body_ratio'] > self.min_body_ratio and
            all(m['is_bearish'] and m['body_ratio'] < self.max_small_body_ratio for m in middle) and
            all(m['close'] > first['close'] for m in middle) and
            last['is_bullish'] and
            last['close'] > first['close']
        )
        
        if is_rising_three:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description="上升三法形态，看涨持续信号",
                details={
                    'first_close': first['close'],
                    'last_close': last['close'],
                    'middle_closes': [m['close'] for m in middle]
                }
            )
        
        return None


@PatternRegistry.register
class FallingThreeMethodsPattern(BasePattern):
    """下降三法形态"""
    
    name = "falling_three_methods"
    description = "下降三法 - 长阴线后跟三根小阳线，再跟一根长阴线"
    min_periods = 5
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_body_ratio = params.get('min_body_ratio', 0.5)
        self.max_small_body_ratio = params.get('max_small_body_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 5:
            return None
        
        rows = df.tail(5).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        first = props[0]
        middle = props[1:4]
        last = props[4]
        
        is_falling_three = (
            first['is_bearish'] and
            first['body_ratio'] > self.min_body_ratio and
            all(m['is_bullish'] and m['body_ratio'] < self.max_small_body_ratio for m in middle) and
            all(m['close'] < first['close'] for m in middle) and
            last['is_bearish'] and
            last['close'] < first['close']
        )
        
        if is_falling_three:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description="下降三法形态，看跌持续信号",
                details={
                    'first_close': first['close'],
                    'last_close': last['close'],
                    'middle_closes': [m['close'] for m in middle]
                }
            )
        
        return None


@PatternRegistry.register
class SeparatingLinesBullishPattern(BasePattern):
    """看涨分离线"""
    
    name = "separating_lines_bullish"
    description = "看涨分离线 - 阴线后跟开盘价相同的阳线"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.open_threshold = params.get('open_threshold', 0.005)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        open_diff = abs(prev_props['open'] - curr_props['open']) / prev_props['open']
        
        is_separating = (
            prev_props['is_bearish'] and
            curr_props['is_bullish'] and
            open_diff <= self.open_threshold
        )
        
        if is_separating:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.MODERATE,
                confidence=0.7,
                description="看涨分离线形态",
                details={
                    'prev_open': prev_props['open'],
                    'curr_open': curr_props['open'],
                    'open_diff': open_diff
                }
            )
        
        return None


@PatternRegistry.register
class SeparatingLinesBearishPattern(BasePattern):
    """看跌分离线"""
    
    name = "separating_lines_bearish"
    description = "看跌分离线 - 阳线后跟开盘价相同的阴线"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.open_threshold = params.get('open_threshold', 0.005)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        open_diff = abs(prev_props['open'] - curr_props['open']) / prev_props['open']
        
        is_separating = (
            prev_props['is_bullish'] and
            curr_props['is_bearish'] and
            open_diff <= self.open_threshold
        )
        
        if is_separating:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.MODERATE,
                confidence=0.7,
                description="看跌分离线形态",
                details={
                    'prev_open': prev_props['open'],
                    'curr_open': curr_props['open'],
                    'open_diff': open_diff
                }
            )
        
        return None


@PatternRegistry.register
class UpsideGapTwoCrowsPattern(BasePattern):
    """向上跳空两只乌鸦"""
    
    name = "upside_gap_two_crows"
    description = "向上跳空两只乌鸦 - 跳空后两根阴线"
    min_periods = 3
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_gap_ratio = params.get('min_gap_ratio', 0.01)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 3:
            return None
        
        rows = df.tail(3).to_dicts()
        props = [self._get_candle_properties(row) for row in rows]
        
        first = props[0]
        second = props[1]
        third = props[2]
        
        gap = second['low'] - first['high']
        gap_ratio = gap / first['high']
        
        is_pattern = (
            first['is_bullish'] and
            gap > 0 and
            gap_ratio >= self.min_gap_ratio and
            second['is_bearish'] and
            third['is_bearish'] and
            third['open'] > second['open'] and
            third['close'] < second['close']
        )
        
        if is_pattern:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.75,
                description="向上跳空两只乌鸦形态",
                details={
                    'gap_ratio': gap_ratio,
                    'second_close': second['close'],
                    'third_close': third['close']
                }
            )
        
        return None


@PatternRegistry.register
class MeetingLinesBullishPattern(BasePattern):
    """看涨会合线"""
    
    name = "meeting_lines_bullish"
    description = "看涨会合线 - 阴线和阳线收盘价相同"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.close_threshold = params.get('close_threshold', 0.005)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        close_diff = abs(prev_props['close'] - curr_props['close']) / prev_props['close']
        
        is_meeting = (
            prev_props['is_bearish'] and
            curr_props['is_bullish'] and
            close_diff <= self.close_threshold
        )
        
        if is_meeting:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.MODERATE,
                confidence=0.65,
                description="看涨会合线形态",
                details={
                    'prev_close': prev_props['close'],
                    'curr_close': curr_props['close'],
                    'close_diff': close_diff
                }
            )
        
        return None


@PatternRegistry.register
class MeetingLinesBearishPattern(BasePattern):
    """看跌会合线"""
    
    name = "meeting_lines_bearish"
    description = "看跌会合线 - 阳线和阴线收盘价相同"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.close_threshold = params.get('close_threshold', 0.005)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_props = self._get_candle_properties(rows[0])
        curr_props = self._get_candle_properties(rows[1])
        
        close_diff = abs(prev_props['close'] - curr_props['close']) / prev_props['close']
        
        is_meeting = (
            prev_props['is_bullish'] and
            curr_props['is_bearish'] and
            close_diff <= self.close_threshold
        )
        
        if is_meeting:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.MODERATE,
                confidence=0.65,
                description="看跌会合线形态",
                details={
                    'prev_close': prev_props['close'],
                    'curr_close': curr_props['close'],
                    'close_diff': close_diff
                }
            )
        
        return None
