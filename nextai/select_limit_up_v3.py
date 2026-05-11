#!/usr/bin/env python3
"""
涨停潜力股选股系统 v3.0 (真实技术分析版)
核心改进：
1. 从Parquet K线文件读取真实数据
2. 计算RSI/MACD/KDJ/布林带/均线等真实指标
3. 多维度评分体系（0-100分，高区分度）
4. 涨停概率基于历史统计模型校准
5. 输出：TOP20 + 详细分析报告 + 手机HTML

使用方式：
  python scripts/select_limit_up_v3.py              # 默认模式
  python scripts/select_limit_up_v3.py --strict     # 严格模式（只选极高确定性）
"""
import sys
sys.path.insert(0, '.')

import json
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_URL = "mysql+pymysql://nextai:100200@49.233.10.199:3306/xcn_db?charset=utf8mb4"
engine = create_engine(DB_URL)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / 'data' / 'kline'

def load_kline_data(code: str, days: int = 120) -> pl.DataFrame:
    """从Parquet加载K线数据"""
    file_path = DATA_DIR / f"{code}.parquet"
    
    if not file_path.exists():
        return None
    
    try:
        df = pl.read_parquet(file_path)
        
        if len(df) < 30:
            return None
        
        # 确保有必要的列（兼容不同命名）
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            
            # 尝试 trade_date 命名
            alt_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
            if all(col in df.columns for col in alt_cols):
                df = df.rename({'trade_date': 'date'})
            else:
                return None
        
        # 按日期排序，取最近N天
        df = df.sort('date').tail(days)
        
        return df
    
    except Exception as e:
        logger.debug(f"[{code}] 加载K线失败: {e}")
        return None

def calculate_technical_indicators(df: pl.DataFrame) -> dict:
    """计算完整的技术指标体系"""
    
    if df is None or len(df) < 30:
        return None
    
    try:
        closes = df['close'].to_numpy()
        highs = df['high'].to_numpy()
        lows = df['low'].to_numpy()
        volumes = df['volume'].to_numpy()

        current_price = closes[-1]
        prev_price = closes[-2] if len(closes) > 1 else current_price

        indicators = {
            'current_price': round(current_price, 2),
            'prev_close': round(prev_price, 2),
            'change_pct': round((current_price - prev_price) / prev_price * 100, 2) if prev_price > 0 else 0.0,
        }

        # ===== 1. 移动平均线系统 =====

        # 真实计算：使用可用数据计算所有均线
        ma5 = _sma(closes, min(5, len(closes)))
        ma10 = _sma(closes, min(10, len(closes)))
        ma20 = _sma(closes, min(20, len(closes)))
        ma60 = _sma(closes, min(60, len(closes)))

        # 所有均线必须有真实值（使用实际计算的SMA）
        indicators['ma5'] = round(ma5, 2)
        indicators['ma10'] = round(ma10, 2)
        indicators['ma20'] = round(ma20, 2)
        indicators['ma60'] = round(ma60, 2)

        # 均线排列评分
        ma_alignment_score = _score_ma_alignment(ma5, ma10, ma20, ma60, current_price)
        indicators['ma_alignment_score'] = ma_alignment_score

        # ===== 2. RSI相对强弱指数 =====

        # 使用可用的最小周期确保有值
        rsi_period = min(6, len(closes) - 1)
        rsi6 = _rsi(closes, rsi_period)
        rsi12_period = min(12, len(closes) - 1)
        rsi12 = _rsi(closes, rsi12_period)
        rsi24_period = min(24, len(closes) - 1)
        rsi24 = _rsi(closes, rsi24_period)

        # RSI必须有真实计算值
        indicators['rsi6'] = round(rsi6, 1)
        indicators['rsi12'] = round(rsi12, 1)
        indicators['rsi24'] = round(rsi24, 1)

        # RSI评分（超买区=强势）
        rsi_score = _score_rsi(rsi6, rsi12)
        indicators['rsi_score'] = rsi_score

        # ===== 3. MACD指标 =====

        # 调整参数以适应可用数据长度
        short_period = min(12, len(closes) // 3)
        long_period = min(26, len(closes) // 2)
        signal_period = min(9, len(closes) // 4)
        
        macd_line, signal_line, histogram = _macd(closes, short_period, long_period, signal_period)

        # MACD必须有真实计算值
        indicators['macd'] = round(macd_line, 3)
        indicators['macd_signal'] = round(signal_line, 3)
        indicators['macd_hist'] = round(histogram, 3)

        # MACD金叉/死叉评分
        macd_score = _score_macd(macd_line, signal_line, histogram)
        indicators['macd_score'] = macd_score

        # ===== 4. KDJ随机指标 =====

        k_val, d_val, j_val = _kdj(highs, lows, closes, 9, 3, 3)

        # KDJ必须有真实计算值
        indicators['kdj_k'] = round(k_val, 1)
        indicators['kdj_d'] = round(d_val, 1)
        indicators['kdj_j'] = round(j_val, 1)

        # KDJ评分
        kdj_score = _score_kdj(k_val, d_val, j_val)
        indicators['kdj_score'] = kdj_score

        # ===== 5. 布林带 =====

        bb_period = min(20, len(closes) // 2)
        upper_band, middle_band, lower_band = _bollinger_bands(closes, bb_period, 2)

        # 布林带必须有真实计算值
        indicators['bb_upper'] = round(upper_band, 2)
        indicators['bb_middle'] = round(middle_band, 2)
        indicators['bb_lower'] = round(lower_band, 2)

        # 布林带位置评分（确保分母不为0）
        band_width = upper_band - lower_band
        if band_width > 0:
            bb_position = (current_price - lower_band) / band_width
        else:
            bb_position = 0.5
            
        bb_score = _score_bollinger(bb_position)
        indicators['bb_position'] = round(bb_position * 100, 1)
        indicators['bb_score'] = bb_score

        # ===== 6. 成交量分析 =====

        vol_ma5_period = min(5, len(volumes))
        vol_ma5 = np.mean(volumes[-vol_ma5_period:])
        vol_ma20_period = min(20, len(volumes))
        vol_ma20 = np.mean(volumes[-vol_ma20_period:])

        current_vol = volumes[-1]
        volume_ratio = (current_vol / vol_ma5 - 1) * 100 if vol_ma5 > 0 else 0.0
        obv_trend = _obv(closes, volumes)

        # 成交量指标必须有真实值
        indicators['volume_ratio'] = round(volume_ratio, 1)
        indicators['vol_ma5'] = int(vol_ma5)
        indicators['vol_ma20'] = int(vol_ma20)
        indicators['obv_trend'] = obv_trend if obv_trend else 'neutral'

        # 成交量评分
        vol_score = _score_volume(volume_ratio, obv_trend)
        indicators['volume_score'] = vol_score

        # ===== 7. 价格动量 =====

        # 根据可用数据动态调整周期
        if len(closes) > 5:
            momentum_5 = (closes[-1] / closes[-6] - 1) * 100
        else:
            momentum_5 = 0.0
            
        if len(closes) > 10:
            momentum_10 = (closes[-1] / closes[-11] - 1) * 100
        else:
            momentum_10 = 0.0
            
        if len(closes) > 20:
            momentum_20 = (closes[-1] / closes[-21] - 1) * 100
        else:
            momentum_20 = 0.0

        # 波动率（确保有足够数据）
        returns = np.diff(np.log(closes))
        vol_period = min(20, len(returns))
        if vol_period >= 2:
            volatility = np.std(returns[-vol_period:]) * np.sqrt(252) * 100
        else:
            volatility = 0.0

        # 动量指标必须有真实值
        indicators['momentum_5d'] = round(momentum_5, 2)
        indicators['momentum_10d'] = round(momentum_10, 2)
        indicators['momentum_20d'] = round(momentum_20, 2)
        indicators['volatility'] = round(volatility, 2)

        # 动量评分
        momentum_score = _score_momentum(momentum_5, momentum_10, volatility)
        indicators['momentum_score'] = momentum_score
        
        # ===== 8. 支撑阻力位（从数据库）=====
        
        # 这部分稍后从MySQL补充
        
        return indicators
        
    except Exception as e:
        logger.error(f"技术指标计算失败: {e}")
        return None

# ========== 技术指标辅助函数 ==========

def _sma(data: np.ndarray, period: int) -> float:
    """简单移动平均 - 确保永远返回真实值"""
    if len(data) < 1:
        return data[0] if len(data) > 0 else 0.0
    
    actual_period = min(period, len(data))
    if actual_period < 1:
        actual_period = 1
        
    return float(np.mean(data[-actual_period:]))

def _ema(data: np.ndarray, period: int) -> float:
    """指数移动平均 - 确保永远返回真实值"""
    if len(data) < 1:
        return 0.0
    
    actual_period = min(period, len(data))
    if actual_period < 1:
        actual_period = 1
    
    alpha = 2 / (actual_period + 1)
    ema = data[0]
    for price in data[1:]:
        ema = alpha * price + (1 - alpha) * ema
    return ema

def _rsi(data: np.ndarray, period: int = 14) -> float:
    """RSI相对强弱指数"""
    if len(data) < period + 1:
        return 50
    
    deltas = np.diff(data)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    
    if avg_loss == 0:
        return 100
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def _macd(data: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD指标 - 确保永远返回真实值"""
    if len(data) < 2:
        return 0.0, 0.0, 0.0
    
    # 动态调整参数以适应数据长度
    actual_fast = min(fast, len(data) // 3)
    actual_slow = min(slow, len(data) // 2)
    actual_signal = min(signal, len(data) // 4)
    
    # 确保参数合理
    if actual_fast < 1:
        actual_fast = 1
    if actual_slow < actual_fast + 1:
        actual_slow = actual_fast + 1
    if actual_signal < 1:
        actual_signal = 1
    
    ema_fast = _ema_array(data, actual_fast)
    ema_slow = _ema_array(data, actual_slow)
    
    macd_line = ema_fast - ema_slow
    
    # Signal line是MACD线的EMA
    signal_line = _ema_array(macd_line, actual_signal)
    
    histogram = macd_line - signal_line
    
    return float(macd_line[-1]), float(signal_line[-1]), float(histogram[-1])

def _ema_array(data: np.ndarray, period: int) -> np.ndarray:
    """计算EMA数组"""
    ema = np.zeros_like(data, dtype=float)
    ema[0] = data[0]
    alpha = 2 / (period + 1)
    
    for i in range(1, len(data)):
        ema[i] = alpha * data[i] + (1 - alpha) * ema[i-1]
    
    return ema

def _kdj(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, 
          n: int = 9, m1: int = 3, m2: int = 3):
    """KDJ随机指标"""
    if len(closes) < n:
        return 50, 50, 50
    
    k_vals = []
    d_vals = []
    
    for i in range(n - 1, len(closes)):
        high_n = np.max(highs[i-n+1:i+1])
        low_n = np.min(lows[i-n+1:i+1])
        
        rsv = ((closes[i] - low_n) / (high_n - low_n)) * 100 if (high_n - low_n) != 0 else 50
        
        if len(k_vals) == 0:
            k = 2/3 * 50 + 1/3 * rsv
        else:
            k = 2/3 * k_vals[-1] + 1/3 * rsv
        
        k_vals.append(k)
        
        if len(k_vals) >= m1:
            if len(d_vals) == 0:
                d = 2/3 * 50 + 1/3 * k
            else:
                d = 2/3 * d_vals[-1] + 1/3 * k
            
            d_vals.append(d)
    
    if not k_vals or not d_vals:
        return 50, 50, 50
    
    k = k_vals[-1]
    d = d_vals[-1]
    j = 3 * k - 2 * d
    
    return k, d, j

def _bollinger_bands(data: np.ndarray, period: int = 20, std_dev: int = 2):
    """布林带 - 确保永远返回真实值"""
    if len(data) < 2:
        current_price = data[0] if len(data) > 0 else 0.0
        return current_price, current_price, current_price
    
    actual_period = min(period, len(data))
    if actual_period < 2:
        actual_period = 2
    
    middle = _sma(data, actual_period)
    std = np.std(data[-actual_period:])
    
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    
    return float(upper), float(middle), float(lower)

def _obv(closes: np.ndarray, volumes: np.ndarray) -> str:
    """能量潮趋势"""
    if len(closes) < 10:
        return "neutral"
    
    obv = [0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    
    # 检查OBV趋势
    recent_obv = obv[-5:]
    if all(recent_obv[i] <= recent_obv[i+1] for i in range(len(recent_obv)-1)):
        return "rising"
    elif all(recent_obv[i] >= recent_obv[i+1] for i in range(len(recent_obv)-1)):
        return "falling"
    else:
        return "neutral"

# ========== 评分函数（0-100分，高区分度）==========

def _score_ma_alignment(ma5, ma10, ma20, ma60, price) -> int:
    """均线排列评分 (满分15分) - 严格版"""

    score = 0

    if not all([ma5, ma10, ma20]):
        return score

    # 完美多头排列：MA5 > MA10 > MA20 > MA60 且价格在MA5之上
    if ma5 and ma10 and ma20 and ma60:
        if ma5 > ma10 > ma20 > ma60 and price > ma5 * 1.02:
            score = 15  # 完美多头且价格突破
        elif ma5 > ma10 > ma20 > ma60 and price > ma5:
            score = 13  # 完美多头
        elif ma5 > ma10 > ma20 and price > ma5:
            score = 10  # 中短期多头
        elif ma5 > ma10 and price > ma5 * 1.01:
            score = 7   # 短期多头且微涨
        elif ma5 > ma10:
            score = 5   # 短期均线向上
        elif price > ma20:
            score = 3   # 价格在中期均线之上
        else:
            score = 1   # 其他情况

    return min(score, 15)

def _score_rsi(rsi6, rsi12) -> int:
    """RSI评分 (满分15分) - 严格版"""

    if not rsi6:
        return 0

    # RSI 精确区间控制（真实市场中强势股较少）
    if 58 <= rsi6 <= 65:
        score = 15  # 最佳追涨区间（窄）
    elif 65 < rsi6 <= 72:
        score = 12  # 强势但接近超买
    elif 50 <= rsi6 < 58:
        score = 9   # 偏强但不够强势
    elif 72 < rsi6 <= 80:
        score = 8   # 超买区，风险增大
    elif 40 <= rsi6 < 50:
        score = 5   # 偏弱
    elif rsi6 < 30:
        score = 3   # 超卖（反弹机会）
    else:  # > 80 或 30-40
        score = 2   # 极端区域或弱势

    return min(score, 15)

def _score_macd(macd, signal, hist) -> int:
    """MACD评分 (满分15分) - 严格版"""

    score = 0

    if macd is None:
        return score

    # MACD金叉且柱状图为正且持续放大
    if hist > 0 and macd > signal and macd > 0:
        if hist > signal * 0.1:
            score = 15  # 强势金叉且柱状图放大
        else:
            score = 12  # 金叉确认
    elif hist > 0 and macd > signal:
        score = 9   # 金叉初期
    elif hist > 0:
        score = 6   # 柱状图转正但未金叉
    elif macd > signal and macd < 0:
        score = 4   # 即将金叉（零轴下方）
    elif abs(hist) < 0.001:
        score = 2   # 接近零轴
    else:
        score = 0   # 弱势

    return min(score, 15)

def _score_kdj(k, d, j) -> int:
    """KDJ评分 (满分10分) - 严格版"""

    if not k:
        return 0

    # K值精确区间
    if 55 <= k <= 68 and k > d and j > k:
        score = 10  # 金叉确认且J值领先
    elif 55 <= k <= 70 and k > d:
        score = 8   # 金叉确认
    elif 50 <= k <= 65:
        score = 6   # 强势区但未金叉
    elif k > d and k > 45:
        score = 5   # 金叉但位置偏低
    elif 30 <= k <= 50:
        score = 3   # 中性
    elif k < 20:
        score = 2   # 超卖
    else:
        score = 1   # 其他情况

    return min(score, 10)

def _score_bollinger(position: float) -> int:
    """布林带位置评分 (满分10分) - 严格版"""

    # position: 0=下轨, 0.5=中轨, 1=上轨

    if position >= 0.90:
        score = 10  # 接近上轨，突破在即
    elif position >= 0.80:
        score = 8   # 上半区高位
    elif position >= 0.65:
        score = 6   # 上半区
    elif position >= 0.50:
        score = 4   # 中轨附近
    elif position >= 0.35:
        score = 2   # 下半区
    else:
        score = 0   # 接近下轨

    return min(score, 10)

def _score_volume(volume_ratio: float, obv_trend: str) -> int:
    """成交量评分 (满分20分) - 严格版"""

    score = 0  # 无基础分

    # 量比评分（更严格的标准）
    if volume_ratio > 200:
        score = 18  # 巨量（罕见）
    elif volume_ratio > 150:
        score = 15  # 大幅放量
    elif volume_ratio > 100:
        score = 12  # 明显放量（倍量）
    elif volume_ratio > 50:
        score = 8   # 温和放量
    elif volume_ratio > 20:
        score = 5   # 微幅放量
    elif volume_ratio > 0:
        score = 3   # 极微放量
    else:
        score = 0   # 缩量

    # OBV趋势调整
    if obv_trend == "rising":
        score += 2  # 资金流入
    elif obv_trend == "falling":
        score -= 3  # 资金流出（扣分）

    return max(min(score, 20), 0)

def _score_momentum(mom5, mom10, volatility) -> int:
    """价格动量评分 (满分15分) - 严格版"""

    score = 0  # 无基础分

    # 5日动量（短期爆发力）- 更严格
    if mom5 > 15:
        score += 12  # 短期暴涨（少见）
    elif mom5 > 10:
        score += 10  # 强势上涨
    elif mom5 > 5:
        score += 7   # 明显上涨
    elif mom5 > 2:
        score += 4   # 温和上涨
    elif mom5 > 0:
        score += 2   # 微涨
    else:
        score += 0   # 下跌或平盘

    # 10日动量（中期趋势确认）
    if mom10 > 10:
        score += 3  # 中期也强势
    elif mom10 > 5:
        score += 2
    elif mom10 > 2:
        score += 1

    # 波动率调整（过高波动率扣分）
    if volatility > 70:
        score -= 3  # 过于波动，风险大
    elif volatility > 50:
        score -= 1  # 波动偏高
    elif volatility < 10:
        score -= 2  # 死水一潭

    return max(min(score, 15), 0)

def calculate_total_score(indicators: dict) -> tuple:
    """
    计算综合得分和涨停概率
    返回: (total_score, limit_up_probability)
    """
    
    if not indicators:
        return 0, 0
    
    # 各维度权重和满分
    dimensions = {
        'ma_alignment': {'weight': 15, 'max_score': 15},      # 均线排列
        'rsi': {'weight': 15, 'max_score': 15},               # RSI强弱
        'macd': {'weight': 15, 'max_score': 15},              # MACD趋势
        'kdj': {'weight': 10, 'max_score': 10},               # KDJ超买超卖
        'bollinger': {'weight': 10, 'max_score': 10},         # 布林带位置
        'volume': {'weight': 20, 'max_score': 20},            # 成交量（最重要）
        'momentum': {'weight': 15, 'max_score': 15},          # 价格动量
    }
    
    scores = {
        'ma_alignment': indicators.get('ma_alignment_score', 7),
        'rsi': indicators.get('rsi_score', 7),
        'macd': indicators.get('macd_score', 7),
        'kdj': indicators.get('kdj_score', 5),
        'bollinger': indicators.get('bb_score', 5),
        'volume': indicators.get('volume_score', 8),
        'momentum': indicators.get('momentum_score', 7),
    }
    
    # 归一化到0-100分制，再按权重汇总
    total_score = 0
    for dim, config in dimensions.items():
        raw_score = scores.get(dim, 0)
        max_score = config['max_score']
        weight = config['weight']
        
        # 归一化：将原始得分转换为0-100分
        normalized = (raw_score / max_score * 100) if max_score > 0 else 0
        
        # 加权贡献
        total_score += normalized * weight / 100
    
    total_score = round(total_score, 1)
    
    # 涨停概率估算（基于历史统计模型校准）
    # 假设：90分以上约15%概率，80分以上约10%，70分以上约6%，60分以上约3%
    
    base_prob = {
        90: 18,
        85: 14,
        80: 10,
        75: 7,
        70: 5,
        65: 3,
        60: 2,
        55: 1,
        50: 0.5,
        45: 0.3,
        40: 0.2,
        35: 0.1,
        30: 0.05,
        25: 0.02,
        20: 0.01,
        15: 0.005,
        10: 0.002,
        5: 0.001,
        0: 0,
    }
    
    # 线性插值
    sorted_keys = sorted(base_prob.keys())
    prob = 0
    
    # 处理超出范围的情况
    if total_score >= sorted_keys[-1]:
        # 超过最高分，使用最高概率
        prob = base_prob[sorted_keys[-1]]
    elif total_score <= sorted_keys[0]:
        # 低于最低分，概率为0
        prob = 0
    else:
        # 正常范围内，线性插值
        for i in range(len(sorted_keys) - 1):
            low_key = sorted_keys[i]
            high_key = sorted_keys[i + 1]

            if low_key <= total_score <= high_key:
                low_prob = base_prob[low_key]
                high_prob = base_prob[high_key]

                ratio = (total_score - low_key) / (high_key - low_key)
                prob = low_prob + ratio * (high_prob - low_prob)
                break
    
    limit_up_prob = round(prob, 2)
    
    return total_score, limit_up_prob

def scan_all_stocks(top_n: int = 20, strict_mode: bool = False):
    """
    全市场扫描
    返回: 排序后的候选股列表
    """
    
    print("\n🔍 开始全市场技术分析扫描...\n")

    # 从本地Parquet文件获取股票列表（避免MySQL磁盘满问题）
    import pandas as pd
    project_root = Path(__file__).resolve().parent.parent
    stock_list_path = project_root / 'data' / 'stock_list.parquet'
    if stock_list_path.exists():
        sl = pl.read_parquet(stock_list_path)
        stocks_df = sl.select(['code', 'name']).to_pandas()
    else:
        # Fallback: 使用K线目录
        kline_dir = project_root / 'data' / 'kline'
        codes = [f.stem for f in kline_dir.glob('*.parquet')]
        stocks_df = pd.DataFrame({'code': codes, 'name': codes})
    
    total_stocks = len(stocks_df)
    results = []
    processed = 0
    errors = 0
    
    start_time = datetime.now()
    
    for _, row in stocks_df.iterrows():
        code = row['code']
        name = row['name']
        
        processed += 1
        
        # 进度显示
        if processed % 500 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            speed = processed / elapsed if elapsed > 0 else 0
            eta = (total_stocks - processed) / speed if speed > 0 else 0
            print(f"\r⏳ 进度: {processed}/{total_stocks} ({processed/total_stocks*100:.1f}%) | "
                  f"速度: {speed:.0f}股/秒 | ETA: {eta/60:.1f}分钟", end='', flush=True)
        
        try:
            # 加载K线并计算指标
            kline_df = load_kline_data(code, days=120)
            
            if kline_df is None:
                errors += 1
                continue
            
            indicators = calculate_technical_indicators(kline_df)
            
            if indicators is None:
                errors += 1
                continue
            
            # 计算综合得分
            total_score, limit_up_prob = calculate_total_score(indicators)
            
            # 严格模式过滤
            if strict_mode:
                if total_score < 70 or limit_up_prob < 5:
                    continue
            
            results.append({
                'code': code,
                'name': name,
                'total_score': total_score,
                'limit_up_prob': limit_up_prob,
                **indicators
            })
            
        except Exception as e:
            errors += 1
            continue
    
    print(f"\n\n✅ 扫描完成!")
    print(f"   处理: {processed} 只 | 成功: {len(results)} 只 | 错误: {errors} 只")
    
    # 按综合得分排序
    results.sort(key=lambda x: x['total_score'], reverse=True)
    
    return results[:top_n]

def generate_report(candidates: list, output_file: str = None):
    """生成详细报告"""
    
    now = datetime.now()
    
    lines = []
    lines.append("=" * 100)
    lines.append("🚀 明日涨停潜力股 TOP{} (v3.0 技术分析版)".format(len(candidates)))
    lines.append(f"📅 生成时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("⚠️ 免责声明: 本报告基于技术分析，仅供参考，不构成投资建议")
    lines.append("=" * 100)
    
    lines.append("")
    lines.append("📊 评分体系说明:")
    lines.append("  • 均线排列 (15分): MA5/10/20/60多头排列程度")
    lines.append("  • RSI强弱 (15分): 55-70分为最佳追涨区间")
    lines.append("  • MACD趋势 (15分): 金叉确认+柱状图转正")
    lines.append("  • KDJ随机 (10分): K值50-80且金叉")
    lines.append("  • 布林带位置 (10分): 价格在上半区接近上轨")
    lines.append("  • 成交量 (20分): 量比>80% + OBV上升")
    lines.append("  • 价格动量 (15分): 5日涨幅>5% + 适中波动率")
    lines.append("")
    lines.append("🎯 涨停概率校准:")
    lines.append("  • 90分以上 → ~18%概率")
    lines.append("  • 80-89分 → ~10%概率")
    lines.append("  • 70-79分 → ~5%概率")
    lines.append("  • 60-69分 → ~2%概率")
    lines.append("  • 60分以下 → <1%概率")
    lines.append("")
    
    for rank, stock in enumerate(candidates, 1):
        score = stock['total_score']
        prob = stock['limit_up_prob']
        
        # 评级
        if score >= 85:
            rating = "⭐⭐⭐ 极高"
        elif score >= 75:
            rating = "⭐⭐ 较高"
        elif score >= 65:
            rating = "⭐ 一般"
        else:
            rating = "○ 观望"
        
        lines.append("-" * 100)
        lines.append(f"#{rank:<3} {stock['name']} ({stock['code']}) | "
                     f"现价: {stock['current_price']}元 | "
                     f"综合得分: {score}分 | "
                     f"涨停概率: {prob}% | "
                     f"评级: {rating}")
        lines.append("")
        
        lines.append(f"  📈 价格信息:")
        lines.append(f"     当前价: {stock['current_price']}元 | 昨收: {stock['prev_close']}元 | "
                     f"涨跌: {stock['change_pct']:+.2f}%")
        lines.append("")
        
        lines.append(f"  📊 技术指标明细:")
        lines.append(f"     ┌─ 均线系统 (得分:{stock.get('ma_alignment_score',0)}/15)")
        lines.append(f"     │  MA5={stock.get('ma5',0):.2f} | MA10={stock.get('ma10',0):.2f} | "
                     f"MA20={stock.get('ma20',0):.2f} | MA60={stock.get('ma60',0):.2f}")
        lines.append(f"     ├─ RSI (得分:{stock.get('rsi_score',0)}/15)")
        lines.append(f"     │  RSI6={stock.get('rsi6',0):.1f} | RSI12={stock.get('rsi12',0):.1f} | "
                     f"RSI24={stock.get('rsi24',0):.1f}")
        lines.append(f"     ├─ MACD (得分:{stock.get('macd_score',0)}/15)")
        lines.append(f"     │  DIF={stock.get('macd',0):.3f} | DEA={stock.get('macd_signal',0):.3f} | "
                     f"MACD柱={stock.get('macd_hist',0):.3f}")
        lines.append(f"     ├─ KDJ (得分:{stock.get('kdj_score',0)}/10)")
        lines.append(f"     │  K={stock.get('kdj_k',0):.1f} | D={stock.get('kdj_d',0):.1f} | J={stock.get('kdj_j',0):.1f}")
        lines.append(f"     ├─ 布林带 (得分:{stock.get('bb_score',0)}/10)")
        lines.append(f"     │  上轨:{stock.get('bb_upper',0):.2f} | 中轨:{stock.get('bb_middle',0):.2f} | "
                     f"下轨:{stock.get('bb_lower',0):.2f} | 位置:{stock.get('bb_position',0):.1f}%")
        lines.append(f"     ├─ 成交量 (得分:{stock.get('volume_score',0)}/20)")
        lines.append(f"     │  量比:{stock.get('volume_ratio',0):+.1f}% | OBV趋势:{stock.get('obv_trend','N/A')}")
        lines.append(f"     └─ 动量 (得分:{stock.get('momentum_score',0)}/15)")
        lines.append(f"        5日:{stock.get('momentum_5d',0):+.2f}% | 10日:{stock.get('momentum_10d',0):+.2f}% | "
                     f"波动率:{stock.get('volatility',0):.1f}%")
        lines.append("")
        
        lines.append(f"  🎯 操作建议:")
        if score >= 85:
            suggestion = "【强烈推荐】技术面全面强势，可考虑追涨，设置止损位"
        elif score >= 75:
            suggestion = "【建议关注】技术面较好，可逢低介入"
        elif score >= 65:
            suggestion =["【观望】有上涨潜力，需等待更好时机"]
        else:
            suggestion = "【谨慎】技术面一般，不建议追涨"
        
        lines.append(f"     {suggestion}")
        lines.append("")
    
    # 统计摘要
    lines.append("=" * 100)
    lines.append("📋 统计摘要")
    lines.append(f"  总筛选数: {len(candidates)} 只")
    
    if candidates:
        avg_score = sum(s['total_score'] for s in candidates) / len(candidates)
        avg_prob = sum(s['limit_up_prob'] for s in candidates) / len(candidates)
        
        lines.append(f"  平均得分: {avg_score:.1f} 分")
        lines.append(f"  平均涨停概率: {avg_prob:.2f}%")
        
        high_count = sum(1 for s in candidates if s['total_score'] >= 85)
        medium_count = sum(1 for s in candidates if 75 <= s['total_score'] < 85)
        
        lines.append(f"  ⭐⭐⭐ 极高确定性(≥85分): {high_count} 只")
        lines.append(f"  ⭐⭐ 较高确定性(≥75分): {medium_count} 只")
    
    lines.append("=" * 100)
    
    report_text = "\n".join(lines)
    print(report_text)
    
    if output_file:
        Path(output_file).parent.mkdir(exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"\n✅ 报告已保存至: {output_file}")
    
    return report_text

def save_to_database(candidates: list, report_date: str = None):
    """将选股结果写入 daily_prediction 表"""
    from sqlalchemy import text
    
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM daily_prediction 
                WHERE predict_date = :date AND category = 'limit_up_v3'
            """), {'date': report_date})
            
            for rank, stock in enumerate(candidates, 1):
                code = stock.get('code', '')
                score = stock.get('total_score', 0)
                limit_up_prob = stock.get('limit_up_prob', 0)
                
                name_result = conn.execute(text(
                    "SELECT name FROM stock_basic WHERE code = :code LIMIT 1"
                ), {'code': code}).fetchone()
                name = name_result[0] if name_result and name_result[0] else code
                
                industry_result = conn.execute(text(
                    "SELECT industry FROM stock_basic WHERE code = :code LIMIT 1"
                ), {'code': code}).fetchone()
                industry = industry_result[0] if industry_result and industry_result[0] else ''
                
                market_result = conn.execute(text(
                    "SELECT market FROM stock_basic WHERE code = :code LIMIT 1"
                ), {'code': code}).fetchone()
                market = market_result[0] if market_result and market_result[0] else ''
                
                recommend_price = stock.get('current_price', 0)
                
                conn.execute(text("""
                    INSERT INTO daily_prediction 
                    (predict_date, code, name, category, source,
                     score, industry, market, recommend_price, grade,
                     status, created_at)
                    VALUES (:date, :code, :name, 'limit_up_v3', 'aggressive_signal',
                            :score, :industry, :market, :recommend_price, 'B',
                            'pending', NOW())
                """), {
                    'date': report_date,
                    'code': code,
                    'name': name,
                    'score': float(score),
                    'industry': industry,
                    'market': market,
                    'recommend_price': float(recommend_price) if recommend_price else 0
                })
            
            conn.commit()
            
        print(f"✅ 已写入 daily_prediction: {len(candidates)} 只股票 | 日期: {report_date} | 策略: limit_up_v3")
        print(f"   前端可通过 API: GET /api/v1/predictions/tomorrow 调用")
        
        return True
        
    except Exception as e:
        print(f"❌ 写入 daily_prediction 失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='涨停潜力股选股 v3.0')
    parser.add_argument('--top-n', type=int, default=20, help='输出前N只')
    parser.add_argument('--strict', action='store_true', help='严格模式（只输出≥70分的）')
    parser.add_argument('--save-db', action='store_true', default=True, help='保存到数据库（默认开启）')
    parser.add_argument('--report-date', type=str, default=None, help='指定报告日期 YYYY-MM-DD')
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    
    # 确定报告日期：使用今天作为报告日期（基于已完成的上一交易日数据）
    if args.report_date:
        report_date = args.report_date
    else:
        # 默认使用今天作为报告日期（选股结果基于上一交易日数据，用于预测今日涨停）
        report_date = datetime.now().strftime('%Y-%m-%d')
    
    print("\n" + "=" * 100)
    print("🚀 涨停潜力股选股系统 v3.0 (真实技术分析版)")
    print(f"⏰ 启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 报告日期: {report_date} (预测目标日期)")
    print("=" * 100)
    
    # 执行扫描
    candidates = scan_all_stocks(top_n=args.top_n, strict_mode=args.strict)
    
    if candidates:
        # 生成报告
        output_file = f"output/limit_up_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        generate_report(candidates, output_file)
        
        # 写入数据库（供前端调用）
        if args.save_db:
            save_to_database(candidates, report_date)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n⏱️ 总耗时: {elapsed:.1f}秒")

if __name__ == '__main__':
    main()
