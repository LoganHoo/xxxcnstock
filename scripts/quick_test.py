"""快速测试单只股票的3个月K线分析"""
import sys
sys.path.insert(0, 'D:\\workstation\\xcnstock')

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
from services.stock_service.filters.volume_price import VolumePriceFilter


def test_single_stock(code, name=''):
    """测试单只股票"""
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')
    
    print(f'股票: {code} {name}')
    print(f'日期: {start_date} ~ {end_date}')
    
    for i in range(5):
        try:
            print(f'尝试 {i+1}/5...', end=' ')
            df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date, end_date=end_date, adjust='qfq')
            if df is not None and len(df) >= 30:
                print(f'成功: {len(df)} 条')
                
                # 技术分析
                vp = VolumePriceFilter()
                result = vp.calculate_enhanced_score(df)
                
                total = result.get('total', 0)
                grade = 'S' if total >= 80 else 'A' if total >= 70 else 'B' if total >= 55 else 'C'
                
                return {
                    'code': code,
                    'name': name,
                    'total': total,
                    'grade': grade,
                    'scores': result.get('scores', {}),
                    'indicators': result.get('indicators', {}),
                    'reasons': result.get('reasons', []),
                    'kline_count': len(df)
                }
        except Exception as e:
            print(f'失败: {type(e).__name__}')
            time.sleep(2)
    
    return None


if __name__ == '__main__':
    print('=== 测试基础版S级股票 ===')
    print()
    
    # 读取基础版S级股票
    basic = pd.read_parquet('data/stock_scores_20260316.parquet')
    s_stocks = basic[basic['grade'] == 'S'][['code', 'name']].head(20).to_dict('records')
    
    results = []
    for stock in s_stocks:
        result = test_single_stock(stock['code'], stock['name'])
        if result:
            results.append(result)
            print(f"总分: {result['total']} ({result['grade']}级)")
            print(f"理由: {result['reasons']}")
        print()
        
        if len(results) >= 10:
            break
        
        time.sleep(1)
    
    # 汇总结果
    if results:
        print()
        print('=' * 50)
        print('=== 增强版分析结果汇总 ===')
        print('=' * 50)
        
        for r in sorted(results, key=lambda x: x['total'], reverse=True):
            print(f"{r['code']} {r['name']}: {r['total']}分 ({r['grade']}级) - {', '.join(r['reasons'][:2])}")