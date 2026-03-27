"""
特殊K线形态识别（A股特有）
"""
import polars as pl
from typing import Optional, Dict, Any
from patterns.base_pattern import (
    BasePattern, PatternResult, PatternRegistry,
    SignalType, PatternStrength
)


@PatternRegistry.register
class LimitUpPattern(BasePattern):
    """涨停板形态"""
    
    name = "limit_up"
    description = "涨停板 - 收盘价等于涨停价（涨幅约10%/20%/30%）"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.limit_threshold = params.get('limit_threshold', 0.095)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_close = float(rows[0]['close'])
        curr_close = float(rows[1]['close'])
        curr_high = float(rows[1]['high'])
        curr_low = float(rows[1]['low'])
        curr_open = float(rows[1]['open'])
        
        change_ratio = (curr_close - prev_close) / prev_close
        
        is_limit_up = (
            change_ratio >= self.limit_threshold and
            abs(curr_close - curr_high) < 0.01
        )
        
        if is_limit_up:
            is_one_word = abs(curr_open - curr_close) < 0.01 and abs(curr_high - curr_low) < 0.01
            
            if is_one_word:
                strength = PatternStrength.VERY_STRONG
                description = "一字涨停板，最强形态"
            elif curr_open < curr_close:
                strength = PatternStrength.STRONG
                description = f"涨停板，涨幅{change_ratio:.2%}"
            else:
                strength = PatternStrength.MODERATE
                description = f"涨停板（开盘较低），涨幅{change_ratio:.2%}"
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=strength,
                confidence=0.9,
                description=description,
                details={
                    'change_ratio': change_ratio,
                    'is_one_word': is_one_word,
                    'open': curr_open,
                    'close': curr_close,
                    'high': curr_high,
                    'low': curr_low
                }
            )
        
        return None


@PatternRegistry.register
class LimitDownPattern(BasePattern):
    """跌停板形态"""
    
    name = "limit_down"
    description = "跌停板 - 收盘价等于跌停价（跌幅约10%/20%/30%）"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.limit_threshold = params.get('limit_threshold', -0.095)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_close = float(rows[0]['close'])
        curr_close = float(rows[1]['close'])
        curr_high = float(rows[1]['high'])
        curr_low = float(rows[1]['low'])
        curr_open = float(rows[1]['open'])
        
        change_ratio = (curr_close - prev_close) / prev_close
        
        is_limit_down = (
            change_ratio <= self.limit_threshold and
            abs(curr_close - curr_low) < 0.01
        )
        
        if is_limit_down:
            is_one_word = abs(curr_open - curr_close) < 0.01 and abs(curr_high - curr_low) < 0.01
            
            if is_one_word:
                strength = PatternStrength.VERY_STRONG
                description = "一字跌停板，最弱形态"
            elif curr_open > curr_close:
                strength = PatternStrength.STRONG
                description = f"跌停板，跌幅{change_ratio:.2%}"
            else:
                strength = PatternStrength.MODERATE
                description = f"跌停板（开盘较高），跌幅{change_ratio:.2%}"
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=strength,
                confidence=0.9,
                description=description,
                details={
                    'change_ratio': change_ratio,
                    'is_one_word': is_one_word,
                    'open': curr_open,
                    'close': curr_close,
                    'high': curr_high,
                    'low': curr_low
                }
            )
        
        return None


@PatternRegistry.register
class TShapePattern(BasePattern):
    """T字板形态"""
    
    name = "t_shape"
    description = "T字板 - 开盘价、收盘价、最高价相同，有下影线"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.price_threshold = params.get('price_threshold', 0.01)
        self.min_lower_shadow_ratio = params.get('min_lower_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        open_high_diff = abs(props['open'] - props['high']) / props['high']
        close_high_diff = abs(props['close'] - props['high']) / props['high']
        
        is_t_shape = (
            open_high_diff < self.price_threshold and
            close_high_diff < self.price_threshold and
            props['lower_shadow'] > 0 and
            props['lower_shadow_ratio'] >= self.min_lower_shadow_ratio
        )
        
        if is_t_shape:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.STRONG,
                confidence=0.85,
                description=f"T字板形态，下影线占比{props['lower_shadow_ratio']:.2%}",
                details={
                    'open': props['open'],
                    'high': props['high'],
                    'close': props['close'],
                    'low': props['low'],
                    'lower_shadow_ratio': props['lower_shadow_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class InvertedTPattern(BasePattern):
    """倒T字板形态"""
    
    name = "inverted_t"
    description = "倒T字板 - 开盘价、收盘价、最低价相同，有上影线"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.price_threshold = params.get('price_threshold', 0.01)
        self.min_upper_shadow_ratio = params.get('min_upper_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        open_low_diff = abs(props['open'] - props['low']) / props['low']
        close_low_diff = abs(props['close'] - props['low']) / props['low']
        
        is_inverted_t = (
            open_low_diff < self.price_threshold and
            close_low_diff < self.price_threshold and
            props['upper_shadow'] > 0 and
            props['upper_shadow_ratio'] >= self.min_upper_shadow_ratio
        )
        
        if is_inverted_t:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.85,
                description=f"倒T字板形态，上影线占比{props['upper_shadow_ratio']:.2%}",
                details={
                    'open': props['open'],
                    'high': props['high'],
                    'close': props['close'],
                    'low': props['low'],
                    'upper_shadow_ratio': props['upper_shadow_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class OneWordBoardPattern(BasePattern):
    """一字板形态"""
    
    name = "one_word_board"
    description = "一字板 - 开盘价、收盘价、最高价、最低价全部相同"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.price_threshold = params.get('price_threshold', 0.005)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        price_range = props['high'] - props['low']
        price_range_ratio = price_range / props['close'] if props['close'] > 0 else 0
        
        is_one_word = price_range_ratio < self.price_threshold
        
        if is_one_word:
            if len(df) >= 2:
                prev_close = df.tail(2).to_dicts()[0]['close']
                change_ratio = (props['close'] - prev_close) / prev_close
                
                if change_ratio > 0.05:
                    signal = SignalType.BULLISH
                    description = f"一字涨停板，涨幅{change_ratio:.2%}"
                elif change_ratio < -0.05:
                    signal = SignalType.BEARISH
                    description = f"一字跌停板，跌幅{change_ratio:.2%}"
                else:
                    signal = SignalType.NEUTRAL
                    description = f"一字板，涨跌幅{change_ratio:.2%}"
            else:
                signal = SignalType.NEUTRAL
                description = "一字板形态"
            
            return PatternResult(
                pattern_name=self.name,
                signal=signal,
                strength=PatternStrength.VERY_STRONG,
                confidence=0.95,
                description=description,
                details={
                    'open': props['open'],
                    'high': props['high'],
                    'close': props['close'],
                    'low': props['low'],
                    'price_range_ratio': price_range_ratio
                }
            )
        
        return None


@PatternRegistry.register
class LongLeggedDojiPattern(BasePattern):
    """长腿十字星形态"""
    
    name = "long_legged_doji"
    description = "长腿十字星 - 实体很小，上下影线都很长"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_threshold = params.get('body_threshold', 0.1)
        self.min_shadow_ratio = params.get('min_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['total_range'] == 0:
            return None
        
        is_long_legged_doji = (
            props['body_ratio'] < self.body_threshold and
            props['upper_shadow_ratio'] >= self.min_shadow_ratio and
            props['lower_shadow_ratio'] >= self.min_shadow_ratio
        )
        
        if is_long_legged_doji:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.NEUTRAL,
                strength=PatternStrength.MODERATE,
                confidence=0.7,
                description=f"长腿十字星，上下影线均较长",
                details={
                    'body_ratio': props['body_ratio'],
                    'upper_shadow_ratio': props['upper_shadow_ratio'],
                    'lower_shadow_ratio': props['lower_shadow_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class DragonflyDojiPattern(BasePattern):
    """蜻蜓十字星形态"""
    
    name = "dragonfly_doji"
    description = "蜻蜓十字星 - 开盘价=收盘价=最高价，只有下影线"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.price_threshold = params.get('price_threshold', 0.01)
        self.min_lower_shadow_ratio = params.get('min_lower_shadow_ratio', 0.5)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        open_close_diff = abs(props['open'] - props['close']) / props['close'] if props['close'] > 0 else 0
        close_high_diff = abs(props['close'] - props['high']) / props['high'] if props['high'] > 0 else 0
        
        is_dragonfly = (
            open_close_diff < self.price_threshold and
            close_high_diff < self.price_threshold and
            props['upper_shadow'] < 0.01 and
            props['lower_shadow_ratio'] >= self.min_lower_shadow_ratio
        )
        
        if is_dragonfly:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description="蜻蜓十字星，看涨信号",
                details={
                    'open': props['open'],
                    'high': props['high'],
                    'close': props['close'],
                    'low': props['low'],
                    'lower_shadow_ratio': props['lower_shadow_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class GravestoneDojiPattern(BasePattern):
    """墓碑十字星形态"""
    
    name = "gravestone_doji"
    description = "墓碑十字星 - 开盘价=收盘价=最低价，只有上影线"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.price_threshold = params.get('price_threshold', 0.01)
        self.min_upper_shadow_ratio = params.get('min_upper_shadow_ratio', 0.5)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        open_close_diff = abs(props['open'] - props['close']) / props['close'] if props['close'] > 0 else 0
        close_low_diff = abs(props['close'] - props['low']) / props['low'] if props['low'] > 0 else 0
        
        is_gravestone = (
            open_close_diff < self.price_threshold and
            close_low_diff < self.price_threshold and
            props['lower_shadow'] < 0.01 and
            props['upper_shadow_ratio'] >= self.min_upper_shadow_ratio
        )
        
        if is_gravestone:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=PatternStrength.STRONG,
                confidence=0.8,
                description="墓碑十字星，看跌信号",
                details={
                    'open': props['open'],
                    'high': props['high'],
                    'close': props['close'],
                    'low': props['low'],
                    'upper_shadow_ratio': props['upper_shadow_ratio']
                }
            )
        
        return None


@PatternRegistry.register
class NearLimitUpPattern(BasePattern):
    """接近涨停形态"""
    
    name = "near_limit_up"
    description = "接近涨停 - 涨幅接近涨停但未封板"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.min_change_ratio = params.get('min_change_ratio', 0.07)
        self.max_change_ratio = params.get('max_change_ratio', 0.095)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        rows = df.tail(2).to_dicts()
        prev_close = float(rows[0]['close'])
        curr_close = float(rows[1]['close'])
        curr_high = float(rows[1]['high'])
        
        change_ratio = (curr_close - prev_close) / prev_close
        
        is_near_limit = (
            self.min_change_ratio <= change_ratio < self.max_change_ratio and
            curr_close < curr_high
        )
        
        if is_near_limit:
            high_diff = (curr_high - curr_close) / curr_close
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=PatternStrength.MODERATE,
                confidence=0.7,
                description=f"接近涨停，涨幅{change_ratio:.2%}，距涨停{high_diff:.2%}",
                details={
                    'change_ratio': change_ratio,
                    'high_diff': high_diff,
                    'close': curr_close,
                    'high': curr_high
                }
            )
        
        return None
