"""获取K线数据并进行增强版技术分析 (优化版)"""
import sys
sys.path.insert(0, 'D:\\workstation\\xcnstock')

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
import pickle

from services.stock_service.filters.volume_price import VolumePriceFilter


def fetch_kline(code: str, start_date: str, end_date: str, retries: int = 5) -> pd.DataFrame:
    """获取单只股票K线数据"""
    for i in range(retries):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_date,
                end_date=end_date,
                adjust='qfq'
            )
            if df is not None and len(df) >= 30:
                return df
        except Exception as e:
            if i < retries - 1:
                time.sleep(1 + i)  # 递增等待时间
    return None


def analyze_stocks(stock_codes: list, days: int = 120, max_stocks: int = 50):
    """批量分析股票 - 优化版"""
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    
    print(f'日期范围: {start_date} ~ {end_date}')
    print(f'分析股票: {len(stock_codes)} 只 (实际: {max_stocks} 只)')
    
    vp_filter = VolumePriceFilter()
    results = []
    klines = {}
    
    # 限制数量
    codes_to_analyze = stock_codes[:max_stocks]
    
    for i, code in enumerate(codes_to_analyze):
        df = fetch_kline(code, start_date, end_date)
        
        if df is not None:
            klines[code] = df
            
            try:
                # 运行增强版分析
                analysis = vp_filter.calculate_enhanced_score(df)
                
                # 获取股票名称
                name = df['股票名称'].iloc[-1] if '股票名称' in df.columns else ''
                price = df['收盘'].iloc[-1] if '收盘' in df.columns else df['close'].iloc[-1]
                change_pct = df['涨跌幅'].iloc[-1] if '涨跌幅' in df.columns else 0
                volume = df['成交量'].iloc[-1] if '成交量' in df.columns else df['volume'].iloc[-1]
                
                results.append({
                    'code': code,
                    'name': name,
                    'price': price,
                    'change_pct': change_pct,
                    'volume': volume,
                    'total_score': analysis.get('total', 0),
                    'trend_score': analysis.get('scores', {}).get('trend', 0),
                    'momentum_score': analysis.get('scores', {}).get('momentum', 0),
                    'volume_score': analysis.get('scores', {}).get('volume', 0),
                    'tech_score': analysis.get('scores', {}).get('tech', 0),
                    'position_score': analysis.get('scores', {}).get('position', 0),
                    'volatility_score': analysis.get('scores', {}).get('volatility', 0),
                    'rsi': analysis.get('indicators', {}).get('rsi', 50),
                    'momentum_3d': analysis.get('indicators', {}).get('momentum_3d', 0),
                    'momentum_10d': analysis.get('indicators', {}).get('momentum_10d', 0),
                    'momentum_20d': analysis.get('indicators', {}).get('momentum_20d', 0),
                    'position': analysis.get('indicators', {}).get('position', 0.5),
                    'reasons': ','.join(analysis.get('reasons', [])[:3])
                })
                
                print(f'[{i+1}/{len(codes_to_analyze)}] {code}: {analysis.get("total", 0)}分 - {name}')
            except Exception as e:
                print(f'[{i+1}/{len(codes_to_analyze)}] {code}: 分析错误 - {e}')
        else:
            print(f'[{i+1}/{len(codes_to_analyze)}] {code}: 获取失败')
        
        time.sleep(0.5)  # 适当延迟
        
        # 每处理10只保存一次
        if (i + 1) % 10 == 0 and results:
            temp_df = pd.DataFrame(results)
            temp_df.to_parquet('data/enhanced_scores_temp.parquet', index=False)
            with open('data/klines_temp.pkl', 'wb') as f:
                pickle.dump(klines, f)
    
    # 转换为DataFrame
    results_df = pd.DataFrame(results)
    
    if len(results_df) > 0:
        # 添加评级
        results_df['grade'] = results_df['total_score'].apply(
            lambda x: 'S' if x >= 80 else ('A' if x >= 70 else ('B' if x >= 55 else 'C'))
        )
        
        # 排序
        results_df = results_df.sort_values('total_score', ascending=False)
    
    return results_df, klines


def main():
    print('=== 增强版选股分析 (3个月历史数据) ===')
    print()
    
    # 读取评分结果
    scores = pd.read_parquet('data/stock_scores_20260316.parquet')
    
    # 获取S级和A级股票
    candidates = scores[scores['grade'].isin(['S', 'A'])]['code'].tolist()
    print(f'候选股票 (S/A级): {len(candidates)} 只')
    print()
    
    # 分析 (限制50只)
    results_df, klines = analyze_stocks(candidates, days=120, max_stocks=50)
    
    if len(results_df) > 0:
        # 保存结果
        results_df.to_parquet('data/enhanced_scores_20260316.parquet', index=False)
        print()
        print(f'保存结果: data/enhanced_scores_20260316.parquet')
        
        # 保存K线数据
        with open('data/klines_3month.pkl', 'wb') as f:
            pickle.dump(klines, f)
        print(f'保存K线: data/klines_3month.pkl ({len(klines)}只)')
        
        # 显示TOP 20
        print()
        print('=== 增强版评分 TOP 20 ===')
        print()
        top20 = results_df.head(20)
        cols = ['code', 'name', 'price', 'change_pct', 'total_score', 'trend_score', 'tech_score', 'grade', 'reasons']
        print(top20[cols].to_string(index=False))
        
        # 统计
        print()
        print('=== 评分分布 ===')
        for grade in ['S', 'A', 'B', 'C']:
            count = len(results_df[results_df['grade'] == grade])
            print(f'{grade}级: {count} 只')
        
        # 显示详细指标
        print()
        print('=== TOP 5 详细指标 ===')
        for _, row in results_df.head(5).iterrows():
            print(f"\n{row['code']} {row['name']}")
            print(f"  总分: {row['total_score']} ({row['grade']}级)")
            print(f"  趋势: {row['trend_score']} | 动量: {row['momentum_score']} | 量价: {row['volume_score']}")
            print(f"  技术: {row['tech_score']} | 位置: {row['position_score']} | 波动: {row['volatility_score']}")
            print(f"  RSI: {row['rsi']:.1f} | 3日动量: {row['momentum_3d']:.2f}% | 10日动量: {row['momentum_10d']:.2f}%")
            print(f"  位置: {row['position']:.2f} (0=支撑, 1=压力)")
            print(f"  理由: {row['reasons']}")


if __name__ == '__main__':
    main()
