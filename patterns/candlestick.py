"""
单根K线形态识别
"""
import polars as pl
from typing import Optional, Dict, Any
from patterns.base_pattern import (
    BasePattern, PatternResult, PatternRegistry,
    SignalType, PatternStrength
)


@PatternRegistry.register
class DojiPattern(BasePattern):
    """十字星形态"""
    
    name = "doji"
    description = "十字星 - 开盘价等于收盘价，表示市场犹豫"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_threshold = params.get('body_threshold', 0.1)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['body_ratio'] < self.body_threshold:
            avg_body = self._calculate_avg_body(df.tail(10))
            
            if props['total_range'] > avg_body * 0.5:
                strength = PatternStrength.MODERATE
            else:
                strength = PatternStrength.WEAK
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.NEUTRAL,
                strength=strength,
                confidence=0.7,
                description=f"十字星形态，实体占比{props['body_ratio']:.2%}",
                details={
                    'body_ratio': props['body_ratio'],
                    'total_range': props['total_range']
                }
            )
        
        return None


@PatternRegistry.register
class HammerPattern(BasePattern):
    """锤子线形态"""
    
    name = "hammer"
    description = "锤子线 - 下影线长，实体小，出现在下跌趋势中"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.lower_shadow_ratio = params.get('lower_shadow_ratio', 2.0)
        self.upper_shadow_ratio = params.get('upper_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['body'] == 0:
            return None
        
        lower_to_body = props['lower_shadow'] / props['body'] if props['body'] > 0 else 0
        upper_to_body = props['upper_shadow'] / props['body'] if props['body'] > 0 else 0
        
        is_hammer = (
            lower_to_body >= self.lower_shadow_ratio and
            upper_to_body <= self.upper_shadow_ratio and
            props['body_ratio'] < 0.4
        )
        
        if is_hammer:
            prev_rows = df.head(len(df) - 1).tail(5)
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_downtrend = prev_close[-1] < prev_close[0] if len(prev_close) > 1 else True
            else:
                is_downtrend = True
            
            strength = PatternStrength.STRONG if is_downtrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=strength,
                confidence=0.75 if is_downtrend else 0.6,
                description=f"锤子线形态，下影线/实体={lower_to_body:.2f}",
                details={
                    'lower_shadow_ratio': lower_to_body,
                    'upper_shadow_ratio': upper_to_body,
                    'in_downtrend': is_downtrend
                }
            )
        
        return None


@PatternRegistry.register
class InvertedHammerPattern(BasePattern):
    """倒锤子线形态"""
    
    name = "inverted_hammer"
    description = "倒锤子线 - 上影线长，实体小，出现在下跌趋势中"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.upper_shadow_ratio = params.get('upper_shadow_ratio', 2.0)
        self.lower_shadow_ratio = params.get('lower_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['body'] == 0:
            return None
        
        upper_to_body = props['upper_shadow'] / props['body'] if props['body'] > 0 else 0
        lower_to_body = props['lower_shadow'] / props['body'] if props['body'] > 0 else 0
        
        is_inverted_hammer = (
            upper_to_body >= self.upper_shadow_ratio and
            lower_to_body <= self.lower_shadow_ratio and
            props['body_ratio'] < 0.4
        )
        
        if is_inverted_hammer:
            prev_rows = df.head(len(df) - 1).tail(5)
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_downtrend = prev_close[-1] < prev_close[0] if len(prev_close) > 1 else True
            else:
                is_downtrend = True
            
            strength = PatternStrength.STRONG if is_downtrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BULLISH,
                strength=strength,
                confidence=0.7 if is_downtrend else 0.55,
                description=f"倒锤子线形态，上影线/实体={upper_to_body:.2f}",
                details={
                    'upper_shadow_ratio': upper_to_body,
                    'lower_shadow_ratio': lower_to_body,
                    'in_downtrend': is_downtrend
                }
            )
        
        return None


@PatternRegistry.register
class HangingManPattern(BasePattern):
    """上吊线形态"""
    
    name = "hanging_man"
    description = "上吊线 - 与锤子线形态相同，但出现在上涨趋势中"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.lower_shadow_ratio = params.get('lower_shadow_ratio', 2.0)
        self.upper_shadow_ratio = params.get('upper_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['body'] == 0:
            return None
        
        lower_to_body = props['lower_shadow'] / props['body'] if props['body'] > 0 else 0
        upper_to_body = props['upper_shadow'] / props['body'] if props['body'] > 0 else 0
        
        is_hanging_man = (
            lower_to_body >= self.lower_shadow_ratio and
            upper_to_body <= self.upper_shadow_ratio and
            props['body_ratio'] < 0.4
        )
        
        if is_hanging_man:
            prev_rows = df.head(len(df) - 1).tail(5)
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_uptrend = prev_close[-1] > prev_close[0] if len(prev_close) > 1 else True
            else:
                is_uptrend = True
            
            strength = PatternStrength.STRONG if is_uptrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=strength,
                confidence=0.75 if is_uptrend else 0.6,
                description=f"上吊线形态，下影线/实体={lower_to_body:.2f}",
                details={
                    'lower_shadow_ratio': lower_to_body,
                    'upper_shadow_ratio': upper_to_body,
                    'in_uptrend': is_uptrend
                }
            )
        
        return None


@PatternRegistry.register
class ShootingStarPattern(BasePattern):
    """流星线形态"""
    
    name = "shooting_star"
    description = "流星线 - 上影线长，实体小，出现在上涨趋势中"
    min_periods = 2
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.upper_shadow_ratio = params.get('upper_shadow_ratio', 2.0)
        self.lower_shadow_ratio = params.get('lower_shadow_ratio', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 2:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['body'] == 0:
            return None
        
        upper_to_body = props['upper_shadow'] / props['body'] if props['body'] > 0 else 0
        lower_to_body = props['lower_shadow'] / props['body'] if props['body'] > 0 else 0
        
        is_shooting_star = (
            upper_to_body >= self.upper_shadow_ratio and
            lower_to_body <= self.lower_shadow_ratio and
            props['body_ratio'] < 0.4
        )
        
        if is_shooting_star:
            prev_rows = df.head(len(df) - 1).tail(5)
            if len(prev_rows) > 0:
                prev_close = prev_rows['close'].to_list()
                is_uptrend = prev_close[-1] > prev_close[0] if len(prev_close) > 1 else True
            else:
                is_uptrend = True
            
            strength = PatternStrength.STRONG if is_uptrend else PatternStrength.MODERATE
            
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.BEARISH,
                strength=strength,
                confidence=0.75 if is_uptrend else 0.6,
                description=f"流星线形态，上影线/实体={upper_to_body:.2f}",
                details={
                    'upper_shadow_ratio': upper_to_body,
                    'lower_shadow_ratio': lower_to_body,
                    'in_uptrend': is_uptrend
                }
            )
        
        return None


@PatternRegistry.register
class MarubozuPattern(BasePattern):
    """光头光脚形态"""
    
    name = "marubozu"
    description = "光头光脚 - 无上下影线或影线极短，表示强势"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.shadow_threshold = params.get('shadow_threshold', 0.05)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['total_range'] == 0:
            return None
        
        upper_ratio = props['upper_shadow'] / props['total_range']
        lower_ratio = props['lower_shadow'] / props['total_range']
        
        is_marubozu = (
            upper_ratio <= self.shadow_threshold and
            lower_ratio <= self.shadow_threshold and
            props['body_ratio'] > 0.8
        )
        
        if is_marubozu:
            signal = SignalType.BULLISH if props['is_bullish'] else SignalType.BEARISH
            strength = PatternStrength.VERY_STRONG
            
            return PatternResult(
                pattern_name=self.name,
                signal=signal,
                strength=strength,
                confidence=0.85,
                description=f"光头光脚{'阳线' if props['is_bullish'] else '阴线'}，实体占比{props['body_ratio']:.2%}",
                details={
                    'body_ratio': props['body_ratio'],
                    'upper_shadow_ratio': upper_ratio,
                    'lower_shadow_ratio': lower_ratio,
                    'is_bullish': props['is_bullish']
                }
            )
        
        return None


@PatternRegistry.register
class SpinningTopPattern(BasePattern):
    """纺锤线形态"""
    
    name = "spinning_top"
    description = "纺锤线 - 实体小，上下影线较长，表示市场犹豫"
    min_periods = 1
    
    def __init__(self, params: Optional[Dict] = None):
        super().__init__(params)
        self.body_threshold = params.get('body_threshold', 0.3)
        self.shadow_threshold = params.get('shadow_threshold', 0.3)
    
    def detect(self, df: pl.DataFrame) -> Optional[PatternResult]:
        if len(df) < 1:
            return None
        
        last_row = df.tail(1).to_dicts()[0]
        props = self._get_candle_properties(last_row)
        
        if props['total_range'] == 0:
            return None
        
        upper_ratio = props['upper_shadow'] / props['total_range']
        lower_ratio = props['lower_shadow'] / props['total_range']
        
        is_spinning_top = (
            props['body_ratio'] < self.body_threshold and
            upper_ratio >= self.shadow_threshold and
            lower_ratio >= self.shadow_threshold
        )
        
        if is_spinning_top:
            return PatternResult(
                pattern_name=self.name,
                signal=SignalType.NEUTRAL,
                strength=PatternStrength.WEAK,
                confidence=0.6,
                description=f"纺锤线形态，实体占比{props['body_ratio']:.2%}",
                details={
                    'body_ratio': props['body_ratio'],
                    'upper_shadow_ratio': upper_ratio,
                    'lower_shadow_ratio': lower_ratio
                }
            )
        
        return None
