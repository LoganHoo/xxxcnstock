"""验证历史K线数据天数"""
import sys
sys.path.insert(0, 'D:/workstation/xcnstock')

import pandas as pd
import requests
import json
import re
import time
import numpy as np

def get_kline_days(code):
    if code.startswith('6'):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'
    
    url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
    params = {
        '_var': f'kline_dayqfq_{symbol}',
        'param': f'{symbol},day,,,100,qfq',
        'r': str(int(time.time() * 1000))
    }
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        text = r.text
        match = re.match(r'kline_dayqfq_\w+=(.*)', text)
        if match:
            data = json.loads(match.group(1))
            if data.get('code') == 0:
                klines = data['data'][symbol].get('qfqday', [])
                return len(klines)
    except:
        pass
    return 0

def main():
    # 读取分析结果
    df = pd.read_parquet('data/enhanced_scores_full.parquet')
    
    print('='*60)
    print('历史K线数据天数验证')
    print('='*60)
    print()
    
    # 统计所有股票的K线天数
    print('正在检查所有股票的K线天数 (这需要几分钟)...')
    
    days_list = []
    for i, row in df.iterrows():
        days = get_kline_days(row['code'])
        days_list.append(days)
        
        if (i + 1) % 500 == 0:
            print(f'  已检查 {i+1}/{len(df)} 只股票...')
        
        time.sleep(0.15)
    
    df['kline_days'] = days_list
    
    print()
    print('='*60)
    print('统计结果')
    print('='*60)
    
    # 整体统计
    print(f'\n总股票数: {len(df)}')
    print(f'平均K线天数: {np.mean(days_list):.1f}')
    print(f'最少K线天数: {np.min(days_list)}')
    print(f'最多K线天数: {np.max(days_list)}')
    
    # 分布统计
    print('\nK线天数分布:')
    bins = [0, 30, 60, 90, 100]
    labels = ['<30天', '30-60天', '60-90天', '>90天']
    
    for i in range(len(bins)-1):
        count = len(df[(df['kline_days'] >= bins[i]) & (df['kline_days'] < bins[i+1])])
        pct = count / len(df) * 100
        print(f'  {labels[i]}: {count}只 ({pct:.1f}%)')
    
    # 60天以上占比
    valid_count = len(df[df['kline_days'] >= 60])
    print(f'\n有效分析(>=60天): {valid_count}只 ({valid_count/len(df)*100:.1f}%)')
    
    # 按等级统计
    print('\n各等级K线天数:')
    for grade in ['S', 'A', 'B', 'C']:
        grade_df = df[df['grade'] == grade]
        avg_days = grade_df['kline_days'].mean()
        min_days = grade_df['kline_days'].min()
        max_days = grade_df['kline_days'].max()
        print(f'  {grade}级: 平均{avg_days:.0f}天, 范围[{min_days}-{max_days}]')
    
    # 列出K线天数不足的股票
    insufficient = df[df['kline_days'] < 60]
    if len(insufficient) > 0:
        print(f'\nK线天数不足60天的股票 ({len(insufficient)}只):')
        for _, row in insufficient.head(20).iterrows():
            print(f"  {row['code']} {row['name']}: {row['kline_days']}天")
        if len(insufficient) > 20:
            print(f'  ... 还有{len(insufficient)-20}只')

if __name__ == '__main__':
    main()