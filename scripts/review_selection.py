#!/usr/bin/env python3
"""
选股复盘工具

复盘前一天的选股结果，对比实际表现
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import pandas as pd


def load_selection_result(date: str):
    """加载选股结果"""
    result_file = Path(f'data/workflow_results/real_selection_comprehensive_{date}.json')
    if not result_file.exists():
        print(f'❌ 未找到选股记录: {result_file}')
        return None
    
    with open(result_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_stock_performance(code: str, selection_date: str, review_date: str):
    """获取股票在选股后的表现"""
    try:
        kline_path = Path(f'data/kline/{code}.parquet')
        if not kline_path.exists():
            return None
        
        df = pl.read_parquet(kline_path)
        df = df.filter(
            (pl.col('trade_date') >= selection_date) & 
            (pl.col('trade_date') <= review_date)
        ).sort('trade_date')
        
        if len(df) < 2:
            return None
        
        # 选股日数据
        selection_day = df.filter(pl.col('trade_date') == selection_date)
        if len(selection_day) == 0:
            return None
        
        selection_price = float(selection_day[0, 'close'])
        
        # 后续表现
        next_days = df.filter(pl.col('trade_date') > selection_date)
        
        if len(next_days) == 0:
            return None
        
        # 计算表现
        latest_price = float(next_days[-1, 'close'])
        max_price = float(next_days['high'].max())
        min_price = float(next_days['low'].min())
        
        return {
            'selection_price': selection_price,
            'latest_price': latest_price,
            'max_price': max_price,
            'min_price': min_price,
            'return_pct': (latest_price - selection_price) / selection_price * 100,
            'max_return_pct': (max_price - selection_price) / selection_price * 100,
            'days': len(next_days)
        }
    except Exception as e:
        return None


def review_selection(selection_date: str, review_date: str = None):
    """复盘选股结果"""
    if review_date is None:
        review_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f'\n📊 选股复盘: {selection_date} 选股 → {review_date} 复盘')
    print('=' * 100)
    
    # 加载选股结果
    result = load_selection_result(selection_date)
    if not result:
        return
    
    top_stocks = result.get('top_stocks', [])
    if not top_stocks:
        print('❌ 没有选股记录')
        return
    
    print(f'\n选股策略: {result.get("strategy_type")}')
    print(f'选股日期: {result.get("end_time", "")[:10]}')
    print(f'选中股票数: {result.get("selected_stocks")}')
    print()
    
    # 复盘每只股票
    performances = []
    
    print(f'{"排名":<6}{"代码":<10}{"选股评分":<10}{"选股价":<10}{"最新价":<10}{"收益":<10}{"最高收益":<10}{"状态":<10}')
    print('-' * 100)
    
    for i, stock in enumerate(top_stocks[:20], 1):
        code = stock['code']
        score = stock['total_score']
        
        perf = get_stock_performance(code, selection_date, review_date)
        
        if perf:
            return_pct = perf['return_pct']
            max_return_pct = perf['max_return_pct']
            
            # 状态判断
            if return_pct > 5:
                status = '🚀 大涨'
            elif return_pct > 0:
                status = '📈 盈利'
            elif return_pct > -3:
                status = '➡️ 持平'
            else:
                status = '📉 亏损'
            
            print(f'{i:<6}{code:<10}{score:<10.1f}{perf["selection_price"]:<10.2f}'
                  f'{perf["latest_price"]:<10.2f}{return_pct:>+8.2f}%{max_return_pct:>+8.2f}%{status:<10}')
            
            performances.append({
                'rank': i,
                'code': code,
                'score': score,
                **perf,
                'status': status
            })
        else:
            print(f'{i:<6}{code:<10}{score:<10.1f}{"无数据":<10}{"无数据":<10}{"-":<10}{"-":<10}{"❌ 无数据":<10}')
    
    # 统计
    if performances:
        print()
        print('=' * 100)
        print('📈 复盘统计')
        print('=' * 100)
        
        avg_return = sum(p['return_pct'] for p in performances) / len(performances)
        max_return = max(p['max_return_pct'] for p in performances)
        min_return = min(p['return_pct'] for p in performances)
        
        win_count = sum(1 for p in performances if p['return_pct'] > 0)
        loss_count = len(performances) - win_count
        
        print(f'股票数量: {len(performances)}')
        print(f'平均收益: {avg_return:+.2f}%')
        print(f'最高收益: {max_return:+.2f}%')
        print(f'最低收益: {min_return:+.2f}%')
        print(f'盈利股票: {win_count} 只 ({win_count/len(performances)*100:.1f}%)')
        print(f'亏损股票: {loss_count} 只 ({loss_count/len(performances)*100:.1f}%)')
        
        # 评分准确率分析
        print()
        print('🎯 评分准确率分析')
        print('-' * 100)
        
        # 按评分分组
        high_score = [p for p in performances if p['score'] >= 85]
        mid_score = [p for p in performances if 70 <= p['score'] < 85]
        low_score = [p for p in performances if p['score'] < 70]
        
        if high_score:
            avg_high = sum(p['return_pct'] for p in high_score) / len(high_score)
            print(f'高分组 (>=85分): {len(high_score)}只, 平均收益 {avg_high:+.2f}%')
        
        if mid_score:
            avg_mid = sum(p['return_pct'] for p in mid_score) / len(mid_score)
            print(f'中分组 (70-84分): {len(mid_score)}只, 平均收益 {avg_mid:+.2f}%')
        
        if low_score:
            avg_low = sum(p['return_pct'] for p in low_score) / len(low_score)
            print(f'低分组 (<70分): {len(low_score)}只, 平均收益 {avg_low:+.2f}%')


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='选股复盘工具')
    parser.add_argument('--date', required=True, help='选股日期 (YYYY-MM-DD)')
    parser.add_argument('--review-date', help='复盘日期 (YYYY-MM-DD)，默认为今天')
    
    args = parser.parse_args()
    
    review_selection(args.date, args.review_date)


if __name__ == '__main__':
    main()
