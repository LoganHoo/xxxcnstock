"""大盘指数分析"""
import sys
sys.path.insert(0, 'D:/workstation/xcnstock')

import requests
import json
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime

# 大盘指数代码
INDEX_CODES = {
    'sh000001': '上证指数',
    'sz399001': '深证成指',
    'sz399006': '创业板指',
    'sh000300': '沪深300',
    'sh000016': '上证50',
    'sh000905': '中证500',
    'sh000688': '科创50',
    'sh000852': '中证1000'
}


def fetch_index_kline(code, days=120):
    """获取指数K线数据"""
    url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
    params = {
        '_var': f'kline_day_{code}',
        'param': f'{code},day,,,{days},',
        'r': str(int(time.time() * 1000))
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://gu.qq.com/'
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        text = r.text
        match = re.match(r'kline_day_\w+=(.*)', text)
        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][code].get('day', [])
                records = []
                for k in klines:
                    records.append({
                        'date': k[0],
                        'open': float(k[1]),
                        'close': float(k[2]),
                        'high': float(k[3]),
                        'low': float(k[4]),
                        'volume': float(k[5]),
                    })
                if records:
                    return pd.DataFrame(records)
    except Exception as e:
        print(f'获取{code}失败: {e}')
    return None


def calculate_ma(prices, period):
    return prices.rolling(window=period).mean()


def calculate_ema(prices, period):
    return prices.ewm(span=period, adjust=False).mean()


def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else 50


def calculate_macd(prices):
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return macd.iloc[-1], signal.iloc[-1], histogram.iloc[-1]


def calculate_kdj(high, low, close, n=9):
    low_n = low.rolling(window=n).min()
    high_n = high.rolling(window=n).max()
    rsv = (close - low_n) / (high_n - low_n) * 100
    rsv = rsv.fillna(50)
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    j = 3 * k - 2 * d
    return k.iloc[-1], d.iloc[-1], j.iloc[-1]


def calculate_bollinger(prices, period=20):
    mid = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    return upper.iloc[-1], mid.iloc[-1], lower.iloc[-1]


def calculate_atr(high, low, close, period=14):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr.iloc[-1] if not atr.empty else 0


def analyze_index(code, name):
    """分析单个指数"""
    df = fetch_index_kline(code, days=120)
    
    if df is None or len(df) < 30:
        return None
    
    close = df['close']
    high = df['high']
    low = df['low']
    volume = df['volume']
    
    # 计算各项指标
    ma5 = calculate_ma(close, 5).iloc[-1]
    ma10 = calculate_ma(close, 10).iloc[-1]
    ma20 = calculate_ma(close, 20).iloc[-1]
    ma60 = calculate_ma(close, 60).iloc[-1]
    
    rsi = calculate_rsi(close, 14)
    macd, signal, hist = calculate_macd(close)
    k, d, j = calculate_kdj(high, low, close)
    upper, mid, lower = calculate_bollinger(close)
    atr = calculate_atr(high, low, close, 14)
    
    # 动量
    momentum_3d = (close.iloc[-1] - close.iloc[-3]) / close.iloc[-3] * 100
    momentum_5d = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
    momentum_10d = (close.iloc[-1] - close.iloc[-10]) / close.iloc[-10] * 100
    momentum_20d = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100
    
    # 趋势判断
    current = close.iloc[-1]
    if ma5 > ma10 > ma20:
        trend = '多头排列'
        trend_score = 100
    elif ma5 > ma20:
        trend = '偏多趋势'
        trend_score = 75
    elif ma5 < ma10 < ma20:
        trend = '空头排列'
        trend_score = 20
    else:
        trend = '震荡整理'
        trend_score = 50
    
    # 量价关系
    vol_ma5 = calculate_ma(volume, 5).iloc[-1]
    vol_ratio = volume.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1
    
    # 综合评分
    score = 0
    
    # 趋势分 25%
    score += trend_score * 0.25
    
    # 动量分 20%
    if momentum_5d > 0 and momentum_10d > 0:
        score += 100 * 0.20
    elif momentum_5d > 0:
        score += 70 * 0.20
    elif momentum_10d < -5:
        score += 20 * 0.20
    else:
        score += 50 * 0.20
    
    # MACD分 15%
    if hist > 0 and macd > signal:
        score += 100 * 0.15
    elif macd > signal:
        score += 70 * 0.15
    else:
        score += 30 * 0.15
    
    # RSI分 15%
    if 40 <= rsi <= 70:
        score += 100 * 0.15
    elif 30 <= rsi <= 80:
        score += 70 * 0.15
    elif rsi < 30:
        score += 50 * 0.15  # 超卖
    else:
        score += 30 * 0.15
    
    # KDJ分 15%
    if k > d and j > k:
        score += 100 * 0.15
    elif k > d:
        score += 70 * 0.15
    else:
        score += 40 * 0.15
    
    # 成交量分 10%
    if vol_ratio > 1.5:
        score += 90 * 0.10
    elif vol_ratio > 1:
        score += 70 * 0.10
    else:
        score += 50 * 0.10
    
    # 涨跌判断
    change_1d = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
    
    return {
        'code': code,
        'name': name,
        'price': round(current, 2),
        'change_1d': round(change_1d, 2),
        'change_3d': round(momentum_3d, 2),
        'change_5d': round(momentum_5d, 2),
        'change_10d': round(momentum_10d, 2),
        'change_20d': round(momentum_20d, 2),
        'ma5': round(ma5, 2),
        'ma10': round(ma10, 2),
        'ma20': round(ma20, 2),
        'ma60': round(ma60, 2) if not pd.isna(ma60) else None,
        'rsi': round(rsi, 1),
        'macd': round(macd, 3),
        'macd_signal': round(signal, 3),
        'macd_hist': round(hist, 3),
        'kdj_k': round(k, 1),
        'kdj_d': round(d, 1),
        'kdj_j': round(j, 1),
        'boll_upper': round(upper, 2),
        'boll_mid': round(mid, 2),
        'boll_lower': round(lower, 2),
        'atr': round(atr, 2),
        'vol_ratio': round(vol_ratio, 2),
        'trend': trend,
        'score': round(score, 1),
        'grade': 'S' if score >= 80 else 'A' if score >= 70 else 'B' if score >= 55 else 'C'
    }


def main():
    print('='*70)
    print('大盘指数技术分析')
    print(f'分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*70)
    print()
    
    results = []
    
    for code, name in INDEX_CODES.items():
        print(f'分析 {name} ({code})...')
        result = analyze_index(code, name)
        if result:
            results.append(result)
        time.sleep(0.3)
    
    print()
    print('='*70)
    print('指数分析结果')
    print('='*70)
    print()
    
    # 表格输出
    print(f"{'指数':<10} {'价格':>8} {'日涨':>6} {'5日':>6} {'10日':>6} {'RSI':>5} {'趋势':<8} {'评分':>5}")
    print('-'*70)
    
    for r in sorted(results, key=lambda x: x['score'], reverse=True):
        print(f"{r['name']:<10} {r['price']:>8.2f} {r['change_1d']:>+5.2f}% {r['change_5d']:>+5.2f}% {r['change_10d']:>+5.2f}% {r['rsi']:>5.1f} {r['trend']:<8} {r['score']:>5.1f}")
    
    print()
    print('='*70)
    print('技术指标详情')
    print('='*70)
    
    for r in results:
        print(f"""
{r['name']} ({r['code']})
  价格: {r['price']}  日涨跌: {r['change_1d']:+.2f}%
  均线: MA5={r['ma5']} MA10={r['ma10']} MA20={r['ma20']} MA60={r['ma60']}
  趋势: {r['trend']} (评分: {r['score']}, 等级: {r['grade']})
  RSI(14): {r['rsi']}
  MACD: DIF={r['macd']} DEA={r['macd_signal']} 柱={r['macd_hist']}
  KDJ: K={r['kdj_k']} D={r['kdj_d']} J={r['kdj_j']}
  布林: 上={r['boll_upper']} 中={r['boll_mid']} 下={r['boll_lower']}
  ATR: {r['atr']}  量比: {r['vol_ratio']}
  动量: 3日={r['change_3d']:+.2f}% 5日={r['change_5d']:+.2f}% 10日={r['change_10d']:+.2f}% 20日={r['change_20d']:+.2f}%
""")
    
    # 保存结果
    df = pd.DataFrame(results)
    df.to_parquet('data/index_analysis_20260316.parquet', index=False)
    df.to_csv('data/results/index_analysis_20260316.csv', index=False, encoding='utf-8-sig')
    print(f'结果已保存到 data/index_analysis_20260316.parquet')
    
    return results


if __name__ == '__main__':
    main()
