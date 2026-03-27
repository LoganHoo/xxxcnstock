"""
分析个股与大盘的相关性和超额收益

- 跟大盘: 与大盘走势高度相关
- 跑赢大盘: 相对大盘有正超额收益
- 跑输大盘: 相对大盘有负超额收益
"""
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import json

def get_index_returns(index_path: Path, days: int = 60) -> pl.DataFrame:
    """获取大盘收益率"""
    df = pl.read_parquet(index_path)
    df = df.sort('trade_date').tail(days + 1)
    
    df = df.with_columns([
        pl.col('trade_date').cast(pl.Utf8).alias('trade_date_str')
    ])
    
    df = df.with_columns([
        (pl.col('close') / pl.col('close').shift(1) - 1).alias('daily_return')
    ])
    
    df = df.with_columns([
        pl.col('daily_return').cum_sum().alias('cum_return')
    ])
    
    return df.filter(pl.col('daily_return').is_not_null())

def analyze_stock_vs_index(stock_path: Path, index_df: pl.DataFrame, days: int = 60) -> dict:
    """分析个股相对于大盘的表现"""
    if not stock_path.exists():
        return None
    
    try:
        stock_df = pl.read_parquet(stock_path)
        stock_df = stock_df.sort('trade_date').tail(days + 1)
        
        if len(stock_df) < days * 0.8:
            return None
        
        stock_df = stock_df.with_columns([
            (pl.col('close') / pl.col('close').shift(1) - 1).alias('daily_return')
        ])
        
        stock_df = stock_df.with_columns([
            pl.col('daily_return').cum_sum().alias('cum_return')
        ])
        
        stock_df = stock_df.filter(pl.col('daily_return').is_not_null())
        
        if len(stock_df) < days * 0.8:
            return None
        
        index_dates = set(index_df['trade_date_str'].to_list())
        stock_dates = set(stock_df['trade_date'].to_list())
        common_dates = index_dates & stock_dates
        
        if len(common_dates) < days * 0.8:
            return None
        
        stock_df = stock_df.filter(pl.col('trade_date').is_in(common_dates))
        index_df_filtered = index_df.filter(pl.col('trade_date_str').is_in(common_dates))
        
        stock_df = stock_df.sort('trade_date')
        index_df_filtered = index_df_filtered.sort('trade_date_str')
        
        stock_returns = stock_df['daily_return'].to_list()
        index_returns = index_df_filtered['daily_return'].to_list()
        
        if len(stock_returns) != len(index_returns) or len(stock_returns) < 10:
            return None
        
        mean_stock = sum(stock_returns) / len(stock_returns)
        mean_index = sum(index_returns) / len(index_returns)
        
        std_stock = (sum((r - mean_stock)**2 for r in stock_returns) / len(stock_returns)) ** 0.5
        std_index = (sum((r - mean_index)**2 for r in index_returns) / len(index_returns)) ** 0.5
        
        if std_stock == 0 or std_index == 0:
            return None
        
        covariance = sum((stock_returns[i] - mean_stock) * (index_returns[i] - mean_index) 
                        for i in range(len(stock_returns))) / len(stock_returns)
        
        correlation = covariance / (std_stock * std_index)
        
        excess_returns = [stock_returns[i] - index_returns[i] for i in range(len(stock_returns))]
        avg_excess = sum(excess_returns) / len(excess_returns)
        
        stock_cum = stock_df.tail(1)['cum_return'].item()
        index_cum = index_df_filtered.tail(1)['cum_return'].item()
        
        excess_cum = stock_cum - index_cum
        
        beta = covariance / (std_index ** 2)
        
        if correlation > 0.7:
            relation = '跟大盘'
        elif correlation > 0.4:
            relation = '弱相关'
        else:
            relation = '独立走势'
        
        if excess_cum > 0.1:
            performance = '大幅跑赢'
        elif excess_cum > 0.03:
            performance = '跑赢大盘'
        elif excess_cum > -0.03:
            performance = '持平大盘'
        elif excess_cum > -0.1:
            performance = '跑输大盘'
        else:
            performance = '大幅跑输'
        
        code = stock_path.stem
        latest = stock_df.tail(1)
        
        return {
            'code': code,
            'latest_date': str(latest['trade_date'].item()),
            'close': round(latest['close'].item(), 2),
            'stock_return': round(stock_cum * 100, 2),
            'index_return': round(index_cum * 100, 2),
            'excess_return': round(excess_cum * 100, 2),
            'correlation': round(correlation, 3),
            'beta': round(beta, 3),
            'relation': relation,
            'performance': performance,
            'avg_daily_excess': round(avg_excess * 100, 4),
        }
        
    except Exception as e:
        return None

def main():
    print('=== 个股与大盘对比分析 ===')
    print(f'分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()
    
    index_path = Path('data/index/000001.parquet')
    kline_dir = Path('data/kline')
    
    days = 250
    
    print(f'分析周期: 最近{days}个交易日')
    print()
    
    index_df = get_index_returns(index_path, days)
    print(f'大盘数据: {len(index_df)} 个交易日')
    print(f'大盘区间收益: {index_df.tail(1)["cum_return"].item() * 100:.2f}%')
    print()
    
    results = []
    
    stock_files = list(kline_dir.glob('*.parquet'))
    print(f'股票数量: {len(stock_files)}')
    print()
    
    for i, stock_path in enumerate(stock_files):
        if (i + 1) % 500 == 0:
            print(f'处理进度: {i + 1}/{len(stock_files)}')
        
        result = analyze_stock_vs_index(stock_path, index_df, days)
        if result:
            results.append(result)
    
    print(f'有效分析: {len(results)} 只股票')
    print()
    
    if len(results) == 0:
        print('没有有效数据')
        return
    
    results_df = pl.DataFrame(results)
    
    print('=' * 60)
    print('【跑赢大盘 TOP 20】')
    print('=' * 60)
    
    outperform = results_df.sort('excess_return', descending=True).head(20)
    for row in outperform.iter_rows(named=True):
        print(f'{row["code"]}  收益: {row["stock_return"]:+.2f}%  超额: {row["excess_return"]:+.2f}%  相关: {row["correlation"]:.2f}  {row["performance"]}')
    
    print()
    print('=' * 60)
    print('【跑输大盘 TOP 20】')
    print('=' * 60)
    
    underperform = results_df.sort('excess_return', descending=False).head(20)
    for row in underperform.iter_rows(named=True):
        print(f'{row["code"]}  收益: {row["stock_return"]:+.2f}%  超额: {row["excess_return"]:+.2f}%  相关: {row["correlation"]:.2f}  {row["performance"]}')
    
    print()
    print('=' * 60)
    print('【跟大盘股票 (相关性>0.7)】')
    print('=' * 60)
    
    follow_index = results_df.filter(pl.col('correlation') > 0.7).sort('correlation', descending=True).head(20)
    if len(follow_index) > 0:
        for row in follow_index.iter_rows(named=True):
            print(f'{row["code"]}  相关: {row["correlation"]:.3f}  Beta: {row["beta"]:.2f}  超额: {row["excess_return"]:+.2f}%  {row["relation"]}')
    else:
        print('没有高相关性股票')
    
    print()
    print('=' * 60)
    print('【统计汇总】')
    print('=' * 60)
    
    performance_counts = results_df.group_by('performance').len().sort('len', descending=True)
    print('\n表现分布:')
    for row in performance_counts.iter_rows(named=True):
        print(f'  {row["performance"]}: {row["len"]} 只')
    
    relation_counts = results_df.group_by('relation').len().sort('len', descending=True)
    print('\n相关性分布:')
    for row in relation_counts.iter_rows(named=True):
        print(f'  {row["relation"]}: {row["len"]} 只')
    
    avg_excess = results_df['excess_return'].mean()
    avg_correlation = results_df['correlation'].mean()
    
    print(f'\n平均超额收益: {avg_excess:.2f}%')
    print(f'平均相关性: {avg_correlation:.3f}')
    
    report = {
        'analysis_time': datetime.now().isoformat(),
        'period_days': days,
        'total_stocks': len(results),
        'index_return': round(index_df.tail(1)['cum_return'].item() * 100, 2),
        'avg_excess_return': round(avg_excess, 2),
        'avg_correlation': round(avg_correlation, 3),
        'performance_distribution': {row['performance']: row['len'] for row in performance_counts.iter_rows(named=True)},
        'relation_distribution': {row['relation']: row['len'] for row in relation_counts.iter_rows(named=True)},
        'outperform_top20': outperform.to_dicts(),
        'underperform_top20': underperform.to_dicts(),
        'follow_index_top20': follow_index.to_dicts() if len(follow_index) > 0 else [],
    }
    
    report_dir = Path('reports')
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f'stock_vs_index_{datetime.now().strftime("%Y%m%d")}.json'
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    
    print(f'\n报告已保存: {report_file}')

if __name__ == '__main__':
    main()
