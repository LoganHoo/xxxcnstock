"""使用腾讯API获取K线并进行技术分析 - 全量版本"""
import sys
sys.path.insert(0, 'D:\\workstation\\xcnstock')

import requests
import pandas as pd
import numpy as np
import time
import json
import re
import os

from services.stock_service.filters.volume_price import VolumePriceFilter


def fetch_kline_tencent(code, days=120):
    """使用腾讯API获取K线数据"""
    # 判断市场
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
        
        # 解析JSONP
        text = r.text
        match = re.match(r'kline_dayqfq_\w+=(.*)', text)
        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][symbol].get('qfqday', [])
                
                # 转换为DataFrame
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
        pass
    
    return None


def main():
    print('=== 全量股票K线采集与技术分析 ===')
    print()
    
    # 读取实时行情数据
    realtime = pd.read_parquet('data/realtime/20260316.parquet')
    
    # 过滤有效股票
    realtime = realtime[realtime['volume'] > 0]  # 有成交量
    realtime = realtime[~realtime['name'].str.contains('ST', case=False, na=False)]  # 非ST
    realtime = realtime[realtime['price'] > 1]  # 价格大于1元
    
    stocks = realtime[['code', 'name', 'price', 'change_pct', 'volume']].to_dict('records')
    
    print(f'待分析股票: {len(stocks)} 只')
    print()
    
    # 检查是否有中断续传
    temp_file = 'data/enhanced_scores_temp.parquet'
    results = []
    processed_codes = set()
    
    if os.path.exists(temp_file):
        temp_df = pd.read_parquet(temp_file)
        results = temp_df.to_dict('records')
        processed_codes = set(temp_df['code'].tolist())
        print(f'续传: 已处理 {len(processed_codes)} 只')
    
    # 技术分析
    vp_filter = VolumePriceFilter()
    success = 0
    failed = 0
    
    for i, stock in enumerate(stocks):
        code = stock['code']
        name = stock['name']
        
        # 跳过已处理
        if code in processed_codes:
            continue
        
        # 获取K线
        df = fetch_kline_tencent(code, days=120)
        
        if df is not None and len(df) >= 30:
            # 运行技术分析
            analysis = vp_filter.calculate_enhanced_score(df)
            
            total = analysis.get('total', 0)
            grade = 'S' if total >= 80 else 'A' if total >= 70 else 'B' if total >= 55 else 'C'
            
            results.append({
                'code': code,
                'name': name,
                'price': stock['price'],
                'change_pct': stock['change_pct'],
                'volume': stock['volume'],
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
            success += 1
            
            if total >= 70:  # 只显示高分股票
                print(f'[{success}] {code} {name}: {total}分 ({grade}级)')
        else:
            failed += 1
        
        # 每50只保存一次
        if len(results) % 50 == 0:
            temp_df = pd.DataFrame(results)
            temp_df.to_parquet(temp_file, index=False)
            print(f'进度: {len(results)} 只 (成功:{success} 失败:{failed})')
        
        time.sleep(0.15)  # 控制请求频率
    
    # 最终结果
    if results:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('enhanced_score', ascending=False)
        results_df.to_parquet('data/enhanced_scores_full_20260316.parquet', index=False)
        
        print()
        print('=' * 70)
        print('=== 全量技术分析结果 ===')
        print('=' * 70)
        print()
        print(f'总分析: {len(results)} 只')
        print(f'成功率: {success/(success+failed)*100:.1f}%')
        print()
        
        # 评分分布
        print('=== 评分分布 ===')
        for g in ['S', 'A', 'B', 'C']:
            n = len(results_df[results_df['grade'] == g])
            pct = n/len(results_df)*100
            print(f'{g}级: {n} 只 ({pct:.1f}%)')
        
        print()
        
        # S级股票
        s_grade = results_df[results_df['grade'] == 'S']
        print(f'=== S级推荐 ({len(s_grade)}只) ===')
        if len(s_grade) > 0:
            print(s_grade[['code', 'name', 'price', 'change_pct', 'enhanced_score', 'rsi', 'momentum_10d', 'reasons']].to_string(index=False))
        
        print()
        
        # A级股票
        a_grade = results_df[results_df['grade'] == 'A']
        print(f'=== A级推荐 ({len(a_grade)}只) ===')
        if len(a_grade) > 0:
            print(a_grade[['code', 'name', 'price', 'change_pct', 'enhanced_score', 'rsi', 'momentum_10d', 'reasons']].head(30).to_string(index=False))
        
        print()
        print(f'数据已保存: data/enhanced_scores_full_20260316.parquet')


if __name__ == '__main__':
    main()
