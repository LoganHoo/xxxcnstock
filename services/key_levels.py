"""关键位计算工具类 - Polars版"""
import numpy as np
import polars as pl
from typing import Dict, List, Any


class KeyLevels:
    """关键位计算器
    
    计算支撑位、压力位、启动位、Pivot点、斐波那契位等
    """
    
    def _rolling_mean(self, arr: np.ndarray, window: int) -> float:
        """计算滚动平均值"""
        if len(arr) < window:
            return float(np.mean(arr))
        return float(np.mean(arr[-window:]))
    
    def _rolling_std(self, arr: np.ndarray, window: int) -> float:
        """计算滚动标准差"""
        if len(arr) < window:
            return float(np.std(arr))
        return float(np.std(arr[-window:]))
    
    def _rolling_min(self, arr: np.ndarray, window: int) -> float:
        """计算滚动最小值"""
        if len(arr) < window:
            return float(np.min(arr))
        return float(np.min(arr[-window:]))
    
    def _rolling_max(self, arr: np.ndarray, window: int) -> float:
        """计算滚动最大值"""
        if len(arr) < window:
            return float(np.max(arr))
        return float(np.max(arr[-window:]))
    
    def calculate_key_levels(self, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
        """计算关键位(支撑位/压力位) - 增强版
        
        Args:
            closes: 收盘价列表（最新在后）
            highs: 最高价列表（最新在后）
            lows: 最低价列表（最新在后）
            
        Returns:
            包含所有关键位的字典
        """
        result = {}
        
        if not closes or len(closes) < 5:
            result['error'] = '数据不足'
            return result
        
        try:
            close_arr = np.array(closes)
            high_arr = np.array(highs)
            low_arr = np.array(lows)
            
            # 均线
            ma5 = self._rolling_mean(close_arr, 5)
            ma10 = self._rolling_mean(close_arr, 10) if len(closes) >= 10 else ma5
            ma20 = self._rolling_mean(close_arr, 20) if len(closes) >= 20 else ma10
            ma60 = self._rolling_mean(close_arr, 60) if len(closes) >= 60 else ma20
            
            result['ma5'] = float(ma5)
            result['ma10'] = float(ma10)
            result['ma20'] = float(ma20)
            result['ma60'] = float(ma60)
            
            # 支撑位
            result['support_ma20'] = float(ma20)
            result['support_ma60'] = float(ma60)
            
            low_20 = self._rolling_min(low_arr, 20)
            result['support_low'] = float(low_20)
            
            # 压力位
            high_20 = self._rolling_max(high_arr, 20)
            result['resistance_high'] = float(high_20)
            
            # 近期支撑和压力
            recent_lows = []
            for i in range(5, len(low_arr)):
                if all(low_arr[i] <= low_arr[i-j] for j in range(1, min(5, i))):
                    recent_lows.append(low_arr[i])
            result['support_recent'] = float(min(recent_lows)) if recent_lows else float(low_20)
            
            recent_highs = []
            for i in range(5, len(high_arr)):
                if all(high_arr[i] >= high_arr[i-j] for j in range(1, min(5, i))):
                    recent_highs.append(high_arr[i])
            result['resistance_recent'] = float(max(recent_highs)) if recent_highs else float(high_20)
            
            current_price = close_arr[-1]
            
            # Pivot点
            pivot = (high_arr[-1] + low_arr[-1] + current_price) / 3
            result['pivot'] = float(pivot)
            result['pivot_r1'] = float(2 * pivot - low_arr[-1])
            result['pivot_r2'] = float(pivot + (high_arr[-1] - low_arr[-1]))
            result['pivot_r3'] = float(pivot + 2 * (high_arr[-1] - low_arr[-1]))
            result['pivot_s1'] = float(2 * pivot - high_arr[-1])
            result['pivot_s2'] = float(pivot - (high_arr[-1] - low_arr[-1]))
            result['pivot_s3'] = float(pivot - 2 * (high_arr[-1] - low_arr[-1]))
            
            # 斐波那契回调位
            if len(closes) >= 60:
                period_high = float(np.max(close_arr[-60:]))
                period_low = float(np.min(close_arr[-60:]))
                fib_range = period_high - period_low
                
                result['fibonacci'] = {}
                fib_levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
                for fib in fib_levels:
                    level = period_low + fib_range * fib
                    result['fibonacci'][f'fib_{int(fib*1000)}'] = round(float(level), 2)
                
                result['fib_support'] = [
                    round(float(period_low + fib_range * 0.382), 2),
                    round(float(period_low + fib_range * 0.5), 2),
                    round(float(period_low + fib_range * 0.618), 2)
                ]
                result['fib_resistance'] = [
                    round(float(period_high - fib_range * 0.382), 2),
                    round(float(period_high - fib_range * 0.5), 2),
                    round(float(period_high - fib_range * 0.618), 2)
                ]
            else:
                result['fibonacci'] = {}
                result['fib_support'] = []
                result['fib_resistance'] = []
            
            # 布林带
            if len(closes) >= 20:
                bb_mean = ma20
                bb_std = self._rolling_std(close_arr, 20)
                result['bb_upper'] = round(float(bb_mean + 2 * bb_std), 2)
                result['bb_mid'] = round(float(bb_mean), 2)
                result['bb_lower'] = round(float(bb_mean - 2 * bb_std), 2)
            else:
                result['bb_upper'] = round(float(current_price), 2)
                result['bb_mid'] = round(float(current_price), 2)
                result['bb_lower'] = round(float(current_price), 2)
            
            # 强支撑和强压力位
            strong_support = float(np.percentile(low_arr[-60:], 10)) if len(lows) >= 60 else low_20
            strong_resistance = float(np.percentile(high_arr[-60:], 90)) if len(highs) >= 60 else high_20
            result['strong_support_zone'] = round(float(strong_support), 2)
            result['strong_resistance_zone'] = round(float(strong_resistance), 2)
            
            # 价格位置判断
            if current_price > strong_resistance:
                result['price_position'] = 'above_resistance'
            elif current_price < strong_support:
                result['price_position'] = 'below_support'
            elif current_price > result.get('pivot_r1', pivot):
                result['price_position'] = 'strong_uptrend'
            elif current_price < result.get('pivot_s1', pivot):
                result['price_position'] = 'strong_downtrend'
            else:
                result['price_position'] = 'neutral'
            
            # 核心区间
            result['core_zone_low'] = round(float(pivot - (strong_resistance - strong_support) * 0.1), 2)
            result['core_zone_high'] = round(float(pivot + (strong_resistance - strong_support) * 0.1), 2)
            result['in_core_zone'] = result['core_zone_low'] <= current_price <= result['core_zone_high']
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def calculate_current_levels(self, current_data: Dict, history_df: pl.DataFrame) -> Dict[str, Any]:
        """计算当前股票的关键位（包含今日数据）
        
        Args:
            current_data: 当前日数据（dict格式）
            history_df: 历史数据Polars DataFrame（包含最近60天）
            
        Returns:
            包含所有关键位的字典
        """
        result = {}
        
        try:
            closes = history_df['close'].to_list()
            highs = history_df['high'].to_list()
            lows = history_df['low'].to_list()
            
            if not closes or len(closes) < 5:
                result['error'] = '数据不足'
                return result
            
            close_arr = np.array(closes)
            high_arr = np.array(highs)
            low_arr = np.array(lows)
            
            # 均线
            ma5 = self._rolling_mean(close_arr, 5)
            ma10 = self._rolling_mean(close_arr, 10) if len(closes) >= 10 else ma5
            ma20 = self._rolling_mean(close_arr, 20) if len(closes) >= 20 else ma10
            ma60 = self._rolling_mean(close_arr, 60) if len(closes) >= 60 else ma20
            
            result['ma5'] = float(ma5)
            result['ma10'] = float(ma10)
            result['ma20'] = float(ma20)
            result['ma60'] = float(ma60)
            
            # 支撑位
            result['support_ma20'] = float(ma20)
            result['support_ma60'] = float(ma60)
            
            low_20 = self._rolling_min(low_arr, 20)
            result['support_low'] = float(low_20)
            
            # 压力位
            high_20 = self._rolling_max(high_arr, 20)
            result['resistance_high'] = float(high_20)
            
            # 近期支撑和压力
            recent_lows = []
            for i in range(5, len(low_arr)):
                if all(low_arr[i] <= low_arr[i-j] for j in range(1, min(5, i))):
                    recent_lows.append(low_arr[i])
            result['support_recent'] = float(min(recent_lows)) if recent_lows else float(low_20)
            
            recent_highs = []
            for i in range(5, len(high_arr)):
                if all(high_arr[i] >= high_arr[i-j] for j in range(1, min(5, i))):
                    recent_highs.append(high_arr[i])
            result['resistance_recent'] = float(max(recent_highs)) if recent_highs else float(high_20)
            
            current_price = close_arr[-1]
            
            # Pivot点
            pivot = (high_arr[-1] + low_arr[-1] + current_price) / 3
            result['pivot'] = float(pivot)
            result['pivot_r1'] = float(2 * pivot - low_arr[-1])
            result['pivot_r2'] = float(pivot + (high_arr[-1] - low_arr[-1]))
            result['pivot_r3'] = float(pivot + 2 * (high_arr[-1] - low_arr[-1]))
            result['pivot_s1'] = float(2 * pivot - high_arr[-1])
            result['pivot_s2'] = float(pivot - (high_arr[-1] - low_arr[-1]))
            result['pivot_s3'] = float(pivot - 2 * (high_arr[-1] - low_arr[-1]))
            
            # 斐波那契回调位
            if len(closes) >= 60:
                period_high = float(np.max(close_arr[-60:]))
                period_low = float(np.min(close_arr[-60:]))
                fib_range = period_high - period_low
                
                result['fibonacci'] = {}
                fib_levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
                for fib in fib_levels:
                    level = period_low + fib_range * fib
                    result['fibonacci'][f'fib_{int(fib*1000)}'] = round(float(level), 2)
                
                result['fib_support'] = [
                    round(float(period_low + fib_range * 0.382), 2),
                    round(float(period_low + fib_range * 0.5), 2),
                    round(float(period_low + fib_range * 0.618), 2)
                ]
                result['fib_resistance'] = [
                    round(float(period_high - fib_range * 0.382), 2),
                    round(float(period_high - fib_range * 0.5), 2),
                    round(float(period_high - fib_range * 0.618), 2)
                ]
            else:
                result['fibonacci'] = {}
                result['fib_support'] = []
                result['fib_resistance'] = []
            
            # 布林带
            if len(closes) >= 20:
                bb_mean = ma20
                bb_std = self._rolling_std(close_arr, 20)
                result['bb_upper'] = round(float(bb_mean + 2 * bb_std), 2)
                result['bb_mid'] = round(float(bb_mean), 2)
                result['bb_lower'] = round(float(bb_mean - 2 * bb_std), 2)
            else:
                result['bb_upper'] = round(float(current_price), 2)
                result['bb_mid'] = round(float(current_price), 2)
                result['bb_lower'] = round(float(current_price), 2)
            
            # 强支撑和强压力位
            strong_support = float(np.percentile(low_arr[-60:], 10)) if len(lows) >= 60 else low_20
            strong_resistance = float(np.percentile(high_arr[-60:], 90)) if len(highs) >= 60 else high_20
            result['strong_support_zone'] = round(float(strong_support), 2)
            result['strong_resistance_zone'] = round(float(strong_resistance), 2)
            
            # 价格位置判断
            if current_price > strong_resistance:
                result['price_position'] = 'above_resistance'
            elif current_price < strong_support:
                result['price_position'] = 'below_support'
            elif current_price > result.get('pivot_r1', pivot):
                result['price_position'] = 'strong_uptrend'
            elif current_price < result.get('pivot_s1', pivot):
                result['price_position'] = 'strong_downtrend'
            else:
                result['price_position'] = 'neutral'
            
            # 核心区间
            result['core_zone_low'] = round(float(pivot - (strong_resistance - strong_support) * 0.1), 2)
            result['core_zone_high'] = round(float(pivot + (strong_resistance - strong_support) * 0.1), 2)
            result['in_core_zone'] = result['core_zone_low'] <= current_price <= result['core_zone_high']
            
            # 今日关键位
            current_high = float(current_data['high'])
            current_low = float(current_data['low'])
            current_open = float(current_data['open'])
            current_close = float(current_data['close'])
            
            # 前一日关键位
            current_date = current_data['date']
            if isinstance(current_date, str):
                current_date = pl.datetime(int(current_date[:4]), int(current_date[5:7]), int(current_date[8:10]))
            
            prev_day = history_df.filter(pl.col('date') == current_date - pl.duration(days=1))
            if len(prev_day) > 0:
                result['prev_high'] = float(prev_day['high'].item())
                result['prev_low'] = float(prev_day['low'].item())
                result['prev_close'] = float(prev_day['close'].item())
            else:
                result['prev_high'] = current_high
                result['prev_low'] = current_low
                result['prev_close'] = current_low
            
            # 今日压力位
            result['pressure_today'] = current_high
            result['pressure_5day'] = float(np.max(high_arr[-5:]))
            result['pressure_prev_high'] = result['prev_high']
            result['strong_pressure'] = max(current_high, result['prev_high'])
            
            # 今日支撑位
            result['support_today'] = current_low
            result['support_open'] = current_open
            result['support_5day'] = float(np.min(low_arr[-5:]))
            result['strong_support'] = min(current_low, current_open)
            
            # 启动位
            result['trigger_high'] = current_high
            result['trigger_prev_high'] = result['prev_high']
            result['trigger_open'] = current_open
            result['trigger_ma20'] = ma20
            result['trigger_ma60'] = ma60
            
            # 斐波那契位（今日）
            price_range = current_high - current_low
            result['fib_382'] = current_low + price_range * 0.382
            result['fib_618'] = current_low + price_range * 0.618
            result['fib_500'] = current_low + price_range * 0.5
            result['fib_100'] = current_low + price_range * 1.0
            
            # 涨停价
            result['limit_up_price'] = current_close * 1.1
            
            # 价格与关键位的距离
            result['dist_to_strong_pressure'] = (current_close - result['strong_pressure']) / result['strong_pressure'] * 100
            result['dist_to_strong_support'] = (current_close - result['strong_support']) / result['strong_support'] * 100
            result['dist_to_ma20'] = (current_close - ma20) / ma20 * 100
            result['dist_to_ma60'] = (current_close - ma60) / ma60 * 100
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def calculate_advanced_indicators(self, closes: List[float], highs: List[float], 
                                      lows: List[float], volumes: List[float]) -> Dict[str, Any]:
        """计算高级技术指标
        
        Args:
            closes: 收盘价列表
            highs: 最高价列表
            lows: 最低价列表
            volumes: 成交量列表
            
        Returns:
            包含MACD、KDJ、RSI、布林带等指标
        """
        result = {}
        
        if not closes or len(closes) < 5:
            result['error'] = '数据不足'
            return result
        
        try:
            close_arr = np.array(closes)
            high_arr = np.array(highs)
            low_arr = np.array(lows)
            volume_arr = np.array(volumes)
            
            # 均线
            ma5 = self._rolling_mean(close_arr, 5)
            ma10 = self._rolling_mean(close_arr, 10) if len(closes) >= 10 else ma5
            ma20 = self._rolling_mean(close_arr, 20) if len(closes) >= 20 else ma10
            ma60 = self._rolling_mean(close_arr, 60) if len(closes) >= 60 else ma20
            
            result['ma5'] = float(ma5)
            result['ma10'] = float(ma10)
            result['ma20'] = float(ma20)
            result['ma60'] = float(ma60)
            
            # MACD
            if len(closes) >= 26:
                ema12 = self._ewm_mean(close_arr, span=12)
                ema26 = self._ewm_mean(close_arr, span=26)
                macd_line = ema12 - ema26
                signal_line = self._ewm_mean(np.array([macd_line]), span=9)[0]
                histogram = macd_line - signal_line
                
                result['macd'] = float(macd_line)
                result['macd_signal'] = float(signal_line)
                result['macd_histogram'] = float(histogram)
                
                if len(closes) >= 2:
                    prev_macd = self._ewm_mean(close_arr[:-1], span=12)[-1] - self._ewm_mean(close_arr[:-1], span=26)[-1]
                    prev_signal = self._ewm_mean(np.array([prev_macd]), span=9)[0]
                    prev_histogram = prev_macd - prev_signal
                    
                    result['macd_crossover'] = 'golden' if histogram > 0 and prev_histogram <= 0 else 'death' if histogram < 0 and prev_histogram >= 0 else 'none'
                else:
                    result['macd_crossover'] = 'none'
            else:
                result['macd'] = 0.0
                result['macd_signal'] = 0.0
                result['macd_histogram'] = 0.0
                result['macd_crossover'] = 'none'
            
            # 布林带
            if len(closes) >= 20:
                boll_mean = ma20
                std = self._rolling_std(close_arr, 20)
                result['boll_upper'] = float(boll_mean + 2 * std)
                result['boll_middle'] = float(boll_mean)
                result['boll_lower'] = float(boll_mean - 2 * std)
                result['boll_position'] = (close_arr[-1] - result['boll_lower']) / (result['boll_upper'] - result['boll_lower'] + 0.001)
            else:
                result['boll_upper'] = float(close_arr[-1])
                result['boll_middle'] = float(close_arr[-1])
                result['boll_lower'] = float(close_arr[-1])
                result['boll_position'] = 0.5
            
            # KDJ
            if len(closes) >= 9:
                low_min = self._rolling_min(low_arr, 9)
                high_max = self._rolling_max(high_arr, 9)
                rsv = (close_arr[-1] - low_min) / (high_max - low_min + 0.001) * 100
                result['kdj_k'] = float(rsv)
                result['kdj_d'] = float(rsv * 0.8 + 50 * 0.2)
                result['kdj_j'] = float(3 * rsv - 2 * result['kdj_d'])
            else:
                result['kdj_k'] = 50.0
                result['kdj_d'] = 50.0
                result['kdj_j'] = 50.0
            
            # RSI
            if len(closes) >= 14:
                deltas = np.diff(close_arr)
                gains = np.where(deltas > 0, deltas, 0)[-14:]
                losses = np.where(deltas < 0, -deltas, 0)[-14:]
                avg_gain = np.mean(gains) if len(gains) > 0 else 0
                avg_loss = np.mean(losses) if len(losses) > 0 else 0
                rs = avg_gain / (avg_loss + 0.0001)
                result['rsi'] = float(100 - (100 / (1 + rs)))
            else:
                result['rsi'] = 50.0
            
            # WR
            if len(closes) >= 14:
                high_max = float(np.max(high_arr[-14:]))
                result['wr'] = float((high_max - close_arr[-1]) / (high_max - np.min(low_arr[-14:]) + 0.001) * 100)
            else:
                result['wr'] = 50.0
            
            # BIAS
            if len(closes) >= 20:
                result['bias5'] = float((close_arr[-1] - ma5) / ma5 * 100)
                result['bias10'] = float((close_arr[-1] - ma10) / ma10 * 100)
                result['bias20'] = float((close_arr[-1] - ma20) / ma20 * 100)
            else:
                result['bias5'] = 0.0
                result['bias10'] = 0.0
                result['bias20'] = 0.0
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _ewm_mean(self, arr: np.ndarray, span: int) -> np.ndarray:
        """计算指数移动平均"""
        alpha = 2 / (span + 1)
        result = np.zeros_like(arr)
        result[0] = arr[0]
        for i in range(1, len(arr)):
            result[i] = alpha * arr[i] + (1 - alpha) * result[i-1]
        return result
