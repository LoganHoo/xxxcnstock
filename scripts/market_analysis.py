"""
大盘关键位与CVD综合分析

结合关键位（支撑/压力、均线）和CVD（累积成交量差）指标，
每日生成大盘分析结论
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import json

def calculate_key_levels(df: pl.DataFrame) -> dict:
    """计算关键位"""
    recent = df.tail(60)
    latest = df.tail(1)
    
    close = latest['close'].item()
    high_60 = recent['high'].max()
    low_60 = recent['low'].min()
    high_20 = recent.tail(20)['high'].max()
    low_20 = recent.tail(20)['low'].min()
    
    ma5 = recent.tail(5)['close'].mean()
    ma10 = recent.tail(10)['close'].mean()
    ma20 = recent.tail(20)['close'].mean()
    ma60 = recent['close'].mean()
    
    return {
        'close': round(close, 2),
        'high_60': round(high_60, 2),
        'low_60': round(low_60, 2),
        'high_20': round(high_20, 2),
        'low_20': round(low_20, 2),
        'ma5': round(ma5, 2),
        'ma10': round(ma10, 2),
        'ma20': round(ma20, 2),
        'ma60': round(ma60, 2),
        'resistance_1': round(high_20, 2),
        'resistance_2': round(high_60, 2),
        'support_1': round(low_20, 2),
        'support_2': round(low_60, 2),
    }

def calculate_cvd(df: pl.DataFrame, lookback: int = 20) -> dict:
    """计算CVD指标"""
    df = df.tail(lookback + 10)
    
    df = df.with_columns([
        ((pl.col('close') - pl.col('open')) / (pl.col('high') - pl.col('low'))).alias('body_ratio')
    ])
    
    df = df.with_columns([
        (pl.col('volume') * pl.col('body_ratio')).alias('cvd_daily')
    ])
    
    df = df.fill_null(0)
    
    cvd_values = df['cvd_daily'].to_list()
    cvd_cumsum = sum(cvd_values)
    cvd_mean = cvd_cumsum / len(cvd_values) if cvd_values else 0
    
    cvd_5 = sum(cvd_values[-5:]) if len(cvd_values) >= 5 else 0
    cvd_10 = sum(cvd_values[-10:]) if len(cvd_values) >= 10 else 0
    
    recent_list = df['cvd_daily'].to_list()
    if len(recent_list) >= 5:
        recent_cvd = sum(recent_list[-3:])
        prev_cvd = sum(recent_list[-6:-3]) if len(recent_list) >= 6 else recent_cvd
        cvd_trend = 'up' if recent_cvd > prev_cvd else 'down'
    else:
        cvd_trend = 'neutral'
    
    if cvd_cumsum > 0:
        signal = 'buy_dominant'
    elif cvd_cumsum < 0:
        signal = 'sell_dominant'
    else:
        signal = 'neutral'
    
    return {
        'cvd_cumsum': round(cvd_cumsum, 2),
        'cvd_mean': round(cvd_mean, 2),
        'cvd_5d': round(cvd_5, 2),
        'cvd_10d': round(cvd_10, 2),
        'cvd_trend': cvd_trend,
        'signal': signal,
    }

def analyze_position(levels: dict, cvd: dict) -> dict:
    """综合分析位置和信号"""
    close = levels['close']
    
    position_score = 0
    position_reasons = []
    
    if close > levels['ma5']:
        position_score += 1
        position_reasons.append('站上MA5')
    else:
        position_reasons.append('低于MA5')
    
    if close > levels['ma20']:
        position_score += 1
        position_reasons.append('站上MA20')
    else:
        position_reasons.append('低于MA20')
    
    if close > levels['ma60']:
        position_score += 1
        position_reasons.append('站上MA60')
    else:
        position_reasons.append('低于MA60')
    
    ma_status = '多头排列' if levels['ma5'] > levels['ma10'] > levels['ma20'] else '空头排列'
    if ma_status == '多头排列':
        position_score += 1
        position_reasons.append('均线多头排列')
    else:
        position_reasons.append('均线空头排列')
    
    distance_to_resistance = (levels['resistance_1'] - close) / close * 100
    distance_to_support = (close - levels['support_1']) / close * 100
    
    if distance_to_resistance < 3:
        position_reasons.append(f'接近压力位({distance_to_resistance:.1f}%)')
    if distance_to_support < 3:
        position_reasons.append(f'接近支撑位({distance_to_support:.1f}%)')
    
    cvd_score = 0
    cvd_reasons = []
    
    if cvd['signal'] == 'buy_dominant':
        cvd_score += 2
        cvd_reasons.append('买方力量占优')
    elif cvd['signal'] == 'sell_dominant':
        cvd_score -= 2
        cvd_reasons.append('卖方力量占优')
    
    if cvd['cvd_trend'] == 'up':
        cvd_score += 1
        cvd_reasons.append('CVD趋势向上')
    elif cvd['cvd_trend'] == 'down':
        cvd_score -= 1
        cvd_reasons.append('CVD趋势向下')
    
    if cvd['cvd_5d'] > 0:
        cvd_reasons.append('近5日净买入')
    else:
        cvd_reasons.append('近5日净卖出')
    
    total_score = position_score + cvd_score
    
    if total_score >= 4:
        conclusion = '看多'
        action = '可考虑加仓或持股'
    elif total_score >= 2:
        conclusion = '偏多'
        action = '可考虑持股观望'
    elif total_score >= 0:
        conclusion = '中性'
        action = '建议观望为主'
    elif total_score >= -2:
        conclusion = '偏空'
        action = '建议减仓或观望'
    else:
        conclusion = '看空'
        action = '建议减仓或空仓'
    
    return {
        'position_score': position_score,
        'cvd_score': cvd_score,
        'total_score': total_score,
        'position_reasons': position_reasons,
        'cvd_reasons': cvd_reasons,
        'conclusion': conclusion,
        'action': action,
        'ma_status': ma_status,
        'distance_to_resistance': round(distance_to_resistance, 2),
        'distance_to_support': round(distance_to_support, 2),
    }

def analyze_index(code: str, name: str, index_dir: Path) -> dict:
    """分析单个指数"""
    file_path = index_dir / f'{code}.parquet'
    
    if not file_path.exists():
        return None
    
    df = pl.read_parquet(file_path)
    df = df.sort('trade_date')
    
    latest_date = df.tail(1)['trade_date'].item()
    
    levels = calculate_key_levels(df)
    cvd = calculate_cvd(df)
    analysis = analyze_position(levels, cvd)
    
    return {
        'code': code,
        'name': name,
        'date': str(latest_date),
        'levels': levels,
        'cvd': cvd,
        'analysis': analysis,
    }

def main():
    print('=== 大盘关键位与CVD综合分析 ===')
    print(f'分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()
    
    index_dir = Path('data/index')
    
    indices = [
        ('000001', '上证指数'),
        ('399001', '深证成指'),
        ('399006', '创业板指'),
        ('000300', '沪深300'),
        ('000016', '上证50'),
        ('000905', '中证500'),
    ]
    
    results = []
    
    for code, name in indices:
        result = analyze_index(code, name, index_dir)
        if result:
            results.append(result)
            
            print(f'【{name}】 {code}')
            print(f'  日期: {result["date"]}')
            print(f'  收盘: {result["levels"]["close"]}')
            print(f'  均线: MA5={result["levels"]["ma5"]} MA20={result["levels"]["ma20"]} MA60={result["levels"]["ma60"]}')
            print(f'  压力位: R1={result["levels"]["resistance_1"]} R2={result["levels"]["resistance_2"]}')
            print(f'  支撑位: S1={result["levels"]["support_1"]} S2={result["levels"]["support_2"]}')
            print(f'  CVD: {result["cvd"]["signal"]} (趋势: {result["cvd"]["cvd_trend"]})')
            print(f'  结论: {result["analysis"]["conclusion"]} - {result["analysis"]["action"]}')
            print()
    
    print('=' * 50)
    print('【综合结论】')
    
    total_scores = [r['analysis']['total_score'] for r in results]
    avg_score = sum(total_scores) / len(total_scores) if total_scores else 0
    
    bullish_count = sum(1 for r in results if r['analysis']['conclusion'] in ['看多', '偏多'])
    bearish_count = sum(1 for r in results if r['analysis']['conclusion'] in ['看空', '偏空'])
    
    print(f'  平均得分: {avg_score:.1f}')
    print(f'  看多指数: {bullish_count}/{len(results)}')
    print(f'  看空指数: {bearish_count}/{len(results)}')
    
    if avg_score >= 3:
        market_view = '市场整体偏多，可积极参与'
    elif avg_score >= 1:
        market_view = '市场中性偏多，谨慎参与'
    elif avg_score >= -1:
        market_view = '市场震荡，建议观望'
    elif avg_score >= -3:
        market_view = '市场偏弱，控制仓位'
    else:
        market_view = '市场弱势，建议空仓观望'
    
    print(f'  市场观点: {market_view}')
    
    report = {
        'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'indices': results,
        'summary': {
            'avg_score': round(avg_score, 2),
            'bullish_count': bullish_count,
            'bearish_count': bearish_count,
            'market_view': market_view,
        }
    }
    
    report_dir = Path('reports')
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f'market_analysis_{datetime.now().strftime("%Y%m%d")}.json'
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    
    print(f'\n报告已保存: {report_file}')

if __name__ == '__main__':
    main()
