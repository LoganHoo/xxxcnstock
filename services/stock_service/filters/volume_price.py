import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

from core.logger import setup_logger

logger = setup_logger("volume_price_filter")


class VolumePriceFilter:
    """量价筛选器 - 增强版
    
    基于第一性原理：价格趋势 + 成交量确认 = 有效突破
    支持多时间维度分析（3个月历史数据）
    """
    
    def calculate_ma(self, prices: pd.Series, period: int) -> pd.Series:
        """计算移动平均线"""
        return prices.rolling(window=period).mean()
    
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """计算指数移动平均线"""
        return prices.ewm(span=period, adjust=False).mean()
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """计算RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1] if not rsi.empty else 50
    
    def calculate_macd(self, prices: pd.Series) -> Tuple[float, float, float]:
        """计算MACD"""
        ema12 = prices.ewm(span=12, adjust=False).mean()
        ema26 = prices.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        
        return macd.iloc[-1], signal.iloc[-1], histogram.iloc[-1]
    
    def calculate_bollinger(self, prices: pd.Series, period: int = 20) -> Tuple[float, float, float]:
        """计算布林带"""
        mid = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper = mid + 2 * std
        lower = mid - 2 * std
        
        return upper.iloc[-1], mid.iloc[-1], lower.iloc[-1]
    
    def calculate_kdj(self, high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9) -> Tuple[float, float, float]:
        """计算KDJ指标"""
        low_n = low.rolling(window=n).min()
        high_n = high.rolling(window=n).max()
        
        rsv = (close - low_n) / (high_n - low_n) * 100
        rsv = rsv.fillna(50)
        
        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return k.iloc[-1], d.iloc[-1], j.iloc[-1]
    
    def calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        """计算ATR（真实波幅）"""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr.iloc[-1] if not atr.empty else 0
    
    def detect_trend(self, prices: pd.Series, period: int = 20) -> str:
        """检测趋势方向"""
        if len(prices) < period:
            return "unknown"
        
        ma = self.calculate_ma(prices, period)
        ma5 = self.calculate_ma(prices, 5)
        ma10 = self.calculate_ma(prices, 10)
        ma20 = self.calculate_ma(prices, 20)
        
        # 多头排列：MA5 > MA10 > MA20
        if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
            return "uptrend"
        # 空头排列
        elif ma5.iloc[-1] < ma10.iloc[-1] < ma20.iloc[-1]:
            return "downtrend"
        # 交叉状态
        elif ma5.iloc[-1] > ma20.iloc[-1]:
            return "weak_uptrend"
        else:
            return "weak_downtrend"
    
    def calculate_momentum(self, prices: pd.Series, period: int = 10) -> float:
        """计算动量（N日涨幅）"""
        if len(prices) < period:
            return 0
        return (prices.iloc[-1] - prices.iloc[-period]) / prices.iloc[-period] * 100
    
    def detect_volume_pattern(self, volume: pd.Series, prices: pd.Series) -> Dict:
        """检测量价关系"""
        if len(volume) < 5:
            return {"pattern": "unknown", "score": 50}
        
        # 最近5日量价关系
        vol_change = volume.pct_change().iloc[-5:]
        price_change = prices.pct_change().iloc[-5:]
        
        # 量价同向天数
        same_direction = sum((vol_change > 0) == (price_change > 0))
        
        # 放量上涨天数
        vol_up_price_up = sum((vol_change > 0.2) & (price_change > 0))
        
        # 缩量下跌天数
        vol_down_price_down = sum((vol_change < -0.1) & (price_change < 0))
        
        if same_direction >= 4:
            return {"pattern": "量价齐升", "score": 90}
        elif vol_up_price_up >= 2:
            return {"pattern": "放量上涨", "score": 80}
        elif vol_down_price_down >= 2:
            return {"pattern": "缩量回调", "score": 70}
        elif same_direction <= 1:
            return {"pattern": "量价背离", "score": 30}
        else:
            return {"pattern": "量价正常", "score": 60}
    
    def detect_support_resistance(self, prices: pd.Series, period: int = 60) -> Dict:
        """检测支撑压力位"""
        if len(prices) < period:
            return {"support": None, "resistance": None, "position": "unknown"}
        
        recent = prices.iloc[-period:]
        current = prices.iloc[-1]
        
        # 找近期高点和低点
        resistance = recent.max()
        support = recent.min()
        
        # 计算当前位置
        if resistance > support:
            position = (current - support) / (resistance - support)
        else:
            position = 0.5
        
        return {
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "position": round(position, 2),  # 0=支撑位, 1=压力位
            "distance_to_support": round((current - support) / current * 100, 2),
            "distance_to_resistance": round((resistance - current) / current * 100, 2)
        }
    
    def calculate_score(self, kline_df: pd.DataFrame) -> float:
        """
        计算量价评分 (基础版)
        
        Args:
            kline_df: K线数据，包含 close, volume 列
        Returns:
            评分 0-100
        """
        if kline_df is None or len(kline_df) < 30:
            return 0.0
        
        try:
            close = kline_df['close']
            volume = kline_df['volume']
            
            scores = {}
            
            # 1. 成交量突破 (30%)
            ma5_vol = self.calculate_ma(volume, 5)
            vol_ratio = volume.iloc[-1] / ma5_vol.iloc[-1] if ma5_vol.iloc[-1] > 0 else 0
            if vol_ratio > 2:
                scores['volume'] = 100
            elif vol_ratio > 1.5:
                scores['volume'] = 80
            elif vol_ratio > 1:
                scores['volume'] = 60
            else:
                scores['volume'] = 30
            
            # 2. 价格突破 (25%)
            ma20 = self.calculate_ma(close, 20)
            if close.iloc[-1] > ma20.iloc[-1] * 1.05:
                scores['price'] = 100
            elif close.iloc[-1] > ma20.iloc[-1]:
                scores['price'] = 70
            else:
                scores['price'] = 30
            
            # 3. RSI (20%)
            rsi = self.calculate_rsi(close)
            if 40 <= rsi <= 70:
                scores['rsi'] = 100
            elif 30 <= rsi <= 80:
                scores['rsi'] = 70
            else:
                scores['rsi'] = 40
            
            # 4. MACD (15%)
            macd, signal, hist = self.calculate_macd(close)
            if hist > 0 and macd > signal:
                scores['macd'] = 100
            elif macd > signal:
                scores['macd'] = 70
            else:
                scores['macd'] = 30
            
            # 5. 布林带 (10%)
            upper, mid, lower = self.calculate_bollinger(close)
            if close.iloc[-1] > mid:
                scores['bollinger'] = 100
            elif close.iloc[-1] > lower:
                scores['bollinger'] = 60
            else:
                scores['bollinger'] = 30
            
            # 加权平均
            weights = {
                'volume': 0.30,
                'price': 0.25,
                'rsi': 0.20,
                'macd': 0.15,
                'bollinger': 0.10
            }
            
            total_score = sum(scores[k] * weights[k] for k in scores)
            return round(total_score, 2)
            
        except Exception as e:
            logger.error(f"计算量价评分失败: {e}")
            return 0.0
    
    def calculate_enhanced_score(self, kline_df: pd.DataFrame) -> Dict:
        """
        计算增强版评分 - 需要3个月以上历史数据
        
        基于第一性原理的多维度评分：
        1. 趋势判断 (20%) - 多空排列、均线趋势
        2. 动量指标 (15%) - 3日/5日/10日/20日涨跌幅
        3. 成交量分析 (15%) - 量价关系、放量缩量
        4. 技术指标 (20%) - MACD/RSI/KDJ/布林带
        5. 位置分析 (15%) - 支撑压力位、上涨空间
        6. 波动分析 (15%) - ATR、振幅、稳定性
        
        Args:
            kline_df: K线数据，需包含 open, high, low, close, volume, amount 列
        Returns:
            包含总分、各维度得分、分析详情的字典
        """
        if kline_df is None or len(kline_df) < 60:
            return {"total": 0, "reasons": ["数据不足(需60天以上)"]}
        
        try:
            close = kline_df['收盘'] if '收盘' in kline_df.columns else kline_df['close']
            high = kline_df['最高'] if '最高' in kline_df.columns else kline_df['high']
            low = kline_df['最低'] if '最低' in kline_df.columns else kline_df['low']
            volume = kline_df['成交量'] if '成交量' in kline_df.columns else kline_df['volume']
            
            scores = {}
            reasons = []
            
            # 1. 趋势判断 (20%)
            trend = self.detect_trend(close, 20)
            if trend == "uptrend":
                scores['trend'] = 100
                reasons.append("多头排列")
            elif trend == "weak_uptrend":
                scores['trend'] = 75
                reasons.append("偏多趋势")
            elif trend == "weak_downtrend":
                scores['trend'] = 40
            else:
                scores['trend'] = 20
                reasons.append("空头排列")
            
            # 2. 动量指标 (15%)
            momentum_3d = self.calculate_momentum(close, 3)
            momentum_5d = self.calculate_momentum(close, 5)
            momentum_10d = self.calculate_momentum(close, 10)
            momentum_20d = self.calculate_momentum(close, 20)
            
            # 短期动量好，中长期稳定
            if momentum_3d > 0 and momentum_5d > 0 and momentum_10d > 0:
                scores['momentum'] = 100
                reasons.append(f"强势上涨(3日+{momentum_3d:.1f}%)")
            elif momentum_5d > 0 and momentum_10d > 0:
                scores['momentum'] = 80
                reasons.append("趋势向上")
            elif momentum_3d > 0 and momentum_10d < 0:
                scores['momentum'] = 60
                reasons.append("短期反弹")
            elif momentum_20d < -10:
                scores['momentum'] = 20
            else:
                scores['momentum'] = 50
            
            # 3. 成交量分析 (15%)
            vol_pattern = self.detect_volume_pattern(volume, close)
            scores['volume'] = vol_pattern['score']
            if vol_pattern['score'] >= 70:
                reasons.append(vol_pattern['pattern'])
            
            # 4. 技术指标 (20%)
            tech_scores = []
            
            # MACD
            macd, signal, hist = self.calculate_macd(close)
            if hist > 0 and macd > signal:
                tech_scores.append(100)
                reasons.append("MACD金叉")
            elif macd > signal:
                tech_scores.append(70)
            else:
                tech_scores.append(30)
            
            # RSI
            rsi = self.calculate_rsi(close)
            if 40 <= rsi <= 70:
                tech_scores.append(100)
            elif 30 <= rsi <= 80:
                tech_scores.append(70)
            elif rsi < 30:
                tech_scores.append(50)  # 超卖可能是机会
                reasons.append(f"RSI超卖({rsi:.0f})")
            else:
                tech_scores.append(30)
            
            # KDJ
            if len(high) >= 9 and len(low) >= 9:
                k, d, j = self.calculate_kdj(high, low, close)
                if k > d and j > k:
                    tech_scores.append(100)
                    reasons.append("KDJ金叉")
                elif k > d:
                    tech_scores.append(70)
                else:
                    tech_scores.append(40)
            else:
                tech_scores.append(50)
            
            # 布林带
            upper, mid, lower = self.calculate_bollinger(close)
            boll_position = (close.iloc[-1] - lower) / (upper - lower) if upper > lower else 0.5
            if 0.3 <= boll_position <= 0.6:
                tech_scores.append(100)
            elif boll_position > 0.8:
                tech_scores.append(40)  # 接近上轨
            else:
                tech_scores.append(70)
            
            scores['tech'] = sum(tech_scores) / len(tech_scores)
            
            # 5. 位置分析 (15%)
            sr = self.detect_support_resistance(close, 60)
            position = sr['position']
            
            if position < 0.3:  # 接近支撑位
                scores['position'] = 100
                reasons.append(f"接近支撑位(+{sr['distance_to_resistance']:.1f}%空间)")
            elif position > 0.8:  # 接近压力位
                scores['position'] = 30
                reasons.append(f"接近压力位(-{sr['distance_to_support']:.1f}%风险)")
            elif 0.4 <= position <= 0.6:  # 中位
                scores['position'] = 80
            else:
                scores['position'] = 60
            
            # 6. 波动分析 (15%)
            atr = self.calculate_atr(high, low, close, 14)
            atr_pct = atr / close.iloc[-1] * 100 if close.iloc[-1] > 0 else 0
            
            # 计算近期波动率
            returns = close.pct_change().iloc[-20:]
            volatility = returns.std() * 100
            
            if 1.5 <= atr_pct <= 3:  # 适中波动
                scores['volatility'] = 100
                reasons.append("波动适中")
            elif atr_pct > 5:  # 高波动
                scores['volatility'] = 40
                reasons.append("高波动风险")
            elif atr_pct < 1:  # 低波动
                scores['volatility'] = 60
                reasons.append("波动较低")
            else:
                scores['volatility'] = 75
            
            # 计算总分
            weights = {
                'trend': 0.20,
                'momentum': 0.15,
                'volume': 0.15,
                'tech': 0.20,
                'position': 0.15,
                'volatility': 0.15
            }
            
            total = sum(scores[k] * weights[k] for k in scores)
            
            return {
                "total": round(total, 1),
                "scores": {k: round(v, 1) for k, v in scores.items()},
                "reasons": reasons,
                "indicators": {
                    "rsi": round(rsi, 1),
                    "momentum_3d": round(momentum_3d, 2),
                    "momentum_10d": round(momentum_10d, 2),
                    "momentum_20d": round(momentum_20d, 2),
                    "position": round(position, 2),
                    "atr_pct": round(atr_pct, 2),
                    "volatility": round(volatility, 2)
                }
            }
            
        except Exception as e:
            logger.error(f"计算增强评分失败: {e}")
            return {"total": 0, "reasons": [f"计算错误: {str(e)}"]}
    
    def filter(self, kline_df: pd.DataFrame) -> Dict:
        """执行量价筛选"""
        score = self.calculate_score(kline_df)
        reasons = []
        
        if kline_df is None or len(kline_df) < 30:
            return {"passed": False, "score": 0, "reasons": ["数据不足"]}
        
        close = kline_df['close']
        volume = kline_df['volume']
        
        # 检查突破
        ma20 = self.calculate_ma(close, 20)
        if close.iloc[-1] > ma20.iloc[-1]:
            reasons.append("突破20日均线")
        
        # 检查量能
        ma5_vol = self.calculate_ma(volume, 5)
        vol_ratio = volume.iloc[-1] / ma5_vol.iloc[-1] if ma5_vol.iloc[-1] > 0 else 0
        if vol_ratio > 1.5:
            reasons.append(f"放量{vol_ratio:.1f}倍")
        
        # 检查MACD
        macd, signal, _ = self.calculate_macd(close)
        if macd > signal:
            reasons.append("MACD金叉")
        
        passed = score >= 60
        
        return {
            "passed": passed,
            "score": score,
            "reasons": reasons
        }
    
    def filter_enhanced(self, kline_df: pd.DataFrame) -> Dict:
        """执行增强版筛选"""
        result = self.calculate_enhanced_score(kline_df)
        
        passed = result['total'] >= 65
        
        return {
            "passed": passed,
            "score": result['total'],
            "scores": result.get('scores', {}),
            "reasons": result.get('reasons', []),
            "indicators": result.get('indicators', {})
        }
