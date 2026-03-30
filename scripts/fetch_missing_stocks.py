"""补充采集缺失股票的K线数据"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import requests
import json
import re
import time
from datetime import datetime

def fetch_kline_tencent(code, days=90):
    """获取3个月K线数据"""
    if code.startswith('6'):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'
    
    url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
    params = {
        '_var': f'kline_dayqfq_{symbol}',
        'param': f'{symbol},day,,,{days},qfq',
        'r': str(int(time.time() * 1000))
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://gu.qq.com/'
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        text = r.text
        match = re.match(r'kline_dayqfq_\w+=(.*)', text)
        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][symbol].get('qfqday', [])
                if len(klines) >= 60:  # 至少60天数据
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
                    return pd.DataFrame(records)
    except Exception as e:
        pass
    return None

def main():
    print('='*60)
    print('补充采集缺失股票的K线数据 (3个月历史)')
    print('='*60)
    
    # 读取已分析的数据
    analyzed = pd.read_parquet('data/enhanced_full_temp.parquet')
    analyzed_codes = set(analyzed['code'].tolist())
    
    # 读取全部股票
    realtime = pd.read_parquet('data/realtime/20260316.parquet')
    realtime = realtime[realtime['volume'] > 0]
    realtime = realtime[~realtime['name'].str.contains('ST', case=False, na=False)]
    realtime = realtime[realtime['price'] > 1]
    
    all_codes = set(realtime['code'].tolist())
    
    # 找出未分析的股票
    missing_codes = all_codes - analyzed_codes
    missing_stocks = realtime[realtime['code'].isin(missing_codes)][['code', 'name', 'price', 'change_pct', 'volume']].to_dict('records')
    
    print(f'\n未分析股票数: {len(missing_codes)}')
    
    if not missing_stocks:
        print('所有股票已分析完成!')
        return
    
    # 检查每只股票的K线数据天数
    print('\n检查K线数据可用性...')
    available = []
    unavailable = []
    
    for i, s in enumerate(missing_stocks):
        df = fetch_kline_tencent(s['code'], days=90)
        if df is not None:
            available.append({
                'code': s['code'],
                'name': s['name'],
                'kline_days': len(df)
            })
            print(f"[{i+1}/{len(missing_stocks)}] {s['code']} {s['name']}: {len(df)}天K线数据")
        else:
            unavailable.append(s)
            print(f"[{i+1}/{len(missing_stocks)}] {s['code']} {s['name']}: 无数据")
        time.sleep(0.3)
    
    print()
    print('='*60)
    print('检查结果')
    print('='*60)
    print(f'有数据: {len(available)} 只')
    print(f'无数据: {len(unavailable)} 只')
    
    if unavailable:
        print('\n无数据股票列表:')
        for s in unavailable[:20]:
            print(f"  {s['code']} {s['name']}")
        if len(unavailable) > 20:
            print(f'  ... 还有 {len(unavailable)-20} 只')
    
    # 分析有数据的股票
    if available:
        print('\n开始分析可用数据的股票...')
        from services.stock_service.filters.volume_price import VolumePriceFilter
        vp_filter = VolumePriceFilter()
        
        results = []
        for item in available:
            code = item['code']
            name = item['name']
            stock_info = next((s for s in missing_stocks if s['code'] == code), None)
            
            df = fetch_kline_tencent(code, days=90)
            if df is not None and len(df) >= 30:
                analysis = vp_filter.calculate_enhanced_score(df)
                total = analysis.get('total', 0)
                grade = 'S' if total >= 80 else 'A' if total >= 70 else 'B' if total >= 55 else 'C'
                
                results.append({
                    'code': code,
                    'name': name,
                    'price': stock_info['price'] if stock_info else 0,
                    'change_pct': stock_info['change_pct'] if stock_info else 0,
                    'volume': stock_info['volume'] if stock_info else 0,
                    'enhanced_score': total,
                    'grade': grade,
                    'trend': analysis.get('scores', {}).get('trend', 0),
                    'momentum': analysis.get('scores', {}).get('momentum', 0),
                    'tech': analysis.get('scores', {}).get('tech', 0),
                    'rsi': analysis.get('indicators', {}).get('rsi', 50),
                    'momentum_3d': analysis.get('indicators', {}).get('momentum_3d', 0),
                    'momentum_10d': analysis.get('indicators', {}).get('momentum_10d', 0),
                    'momentum_20d': analysis.get('indicators', {}).get('momentum_20d', 0),
                    'position': analysis.get('indicators', {}).get('position', 0.5),
                    'reasons': ','.join(analysis.get('reasons', [])[:3])
                })
                print(f"  {code} {name}: {total}分 ({grade}级)")
            time.sleep(0.2)
        
        # 合并到原有数据
        if results:
            new_df = pd.DataFrame(results)
            existing_df = pd.read_parquet('data/enhanced_full_temp.parquet')
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.sort_values('enhanced_score', ascending=False)
            combined.to_parquet('data/enhanced_full_temp.parquet', index=False)
            combined.to_parquet('data/enhanced_scores_full.parquet', index=False)
            
            print(f'\n新增 {len(results)} 只股票分析结果')
            print(f'总计: {len(combined)} 只股票')

if __name__ == '__main__':
    main()