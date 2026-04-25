#!/usr/bin/env python3
"""
全面数据质量分析报告
"""
import sys
from pathlib import Path
import polars as pl
from datetime import datetime
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

def analyze_data_quality():
    print('='*100)
    print('📊 全面数据质量分析报告')
    print('='*100)
    print(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # 1. 股票列表分析
    print('='*100)
    print('📋 一、股票列表数据质量')
    print('='*100)

    stock_list_path = Path('data/stock_list.parquet')
    if stock_list_path.exists():
        stock_list = pl.read_parquet(stock_list_path)
        print(f'✅ 文件存在')
        print(f'   总股票数: {len(stock_list)}')
        print(f'   列名: {stock_list.columns}')

        # 检查空值
        null_counts = {}
        for col in stock_list.columns:
            null_count = stock_list[col].is_null().sum()
            if null_count > 0:
                null_counts[col] = null_count

        if null_counts:
            print(f'   ⚠️  空值统计:')
            for col, count in null_counts.items():
                print(f'      {col}: {count}')
        else:
            print(f'   ✅ 无空值')

        # 检查重复
        dup_count = len(stock_list) - len(stock_list.unique(subset=['code']))
        if dup_count > 0:
            print(f'   ⚠️  重复股票代码: {dup_count} 只')
        else:
            print(f'   ✅ 无重复股票代码')
    else:
        print('❌ 股票列表文件不存在')

    print()

    # 2. K线数据分析
    print('='*100)
    print('📈 二、K线数据质量分析')
    print('='*100)

    kline_dir = Path('data/kline')
    if kline_dir.exists():
        parquet_files = list(kline_dir.glob('*.parquet'))
        print(f'✅ K线数据目录存在')
        print(f'   总文件数: {len(parquet_files)}')

        # 检查文件状态
        empty_files = []
        corrupt_files = []
        valid_count = 0
        file_sizes = []

        for f in parquet_files[:2000]:  # 抽样检查2000个
            size = f.stat().st_size
            file_sizes.append(size)

            if size == 0:
                empty_files.append(f.name)
            else:
                try:
                    df = pl.read_parquet(f)
                    valid_count += 1
                except Exception as e:
                    corrupt_files.append((f.name, str(e)))

        print(f'   ✅ 有效文件(抽样): {valid_count}')
        print(f'   ⚠️  空文件: {len(empty_files)}')
        print(f'   ❌ 损坏文件: {len(corrupt_files)}')

        if corrupt_files:
            print(f'   损坏文件示例:')
            for name, error in corrupt_files[:3]:
                print(f'      {name}: {error[:50]}...')

        # 文件大小统计
        if file_sizes:
            avg_size = sum(file_sizes) / len(file_sizes)
            max_size = max(file_sizes)
            min_size = min([s for s in file_sizes if s > 0], default=0)
            print(f'\n   文件大小统计:')
            print(f'      平均: {avg_size/1024:.1f} KB')
            print(f'      最大: {max_size/1024:.1f} KB')
            print(f'      最小(非空): {min_size/1024:.1f} KB')
    else:
        print('❌ K线数据目录不存在')

    print()

    # 3. 数据日期分析
    print('='*100)
    print('📅 三、数据日期覆盖分析')
    print('='*100)

    if kline_dir.exists():
        date_counts = Counter()
        total_checked = 0

        for f in parquet_files[:1000]:  # 抽样1000个
            try:
                df = pl.read_parquet(f, columns=['trade_date'])
                if len(df) > 0:
                    latest_date = df['trade_date'].max()
                    date_counts[latest_date] += 1
                    total_checked += 1
            except:
                pass

        print(f'   抽样检查: {total_checked} 只股票')
        print(f'   最新数据日期分布:')
        for date, count in sorted(date_counts.items(), reverse=True)[:10]:
            pct = count / total_checked * 100 if total_checked > 0 else 0
            print(f'      {date}: {count} 只 ({pct:.1f}%)')

    print()

    # 4. 评分数据分析
    print('='*100)
    print('⭐ 四、评分数据质量分析')
    print('='*100)

    scores_path = Path('data/enhanced_scores_full.parquet')
    if scores_path.exists():
        scores = pl.read_parquet(scores_path)
        print(f'✅ 评分文件存在')
        print(f'   总股票数: {len(scores)}')
        print(f'   列名: {scores.columns}')

        # 统计信息
        print(f'\n   评分统计:')
        print(f'      平均: {scores["enhanced_score"].mean():.2f}')
        print(f'      最高: {scores["enhanced_score"].max()}')
        print(f'      最低: {scores["enhanced_score"].min()}')
        print(f'      中位数: {scores["enhanced_score"].median():.2f}')

        # 分布
        high = scores.filter(scores['enhanced_score'] >= 80)
        mid = scores.filter((scores['enhanced_score'] >= 60) & (scores['enhanced_score'] < 80))
        low = scores.filter(scores['enhanced_score'] < 60)

        print(f'\n   评分分布:')
        print(f'      高分(>=80): {len(high)} 只 ({len(high)/len(scores)*100:.1f}%)')
        print(f'      中分(60-79): {len(mid)} 只 ({len(mid)/len(scores)*100:.1f}%)')
        print(f'      低分(<60): {len(low)} 只 ({len(low)/len(scores)*100:.1f}%)')

        # 检查空值
        null_stats = {}
        for col in ['price', 'change_pct', 'volume']:
            null_count = scores[col].is_null().sum()
            if null_count > 0:
                null_stats[col] = null_count

        if null_stats:
            print(f'\n   ⚠️  空值统计:')
            for col, count in null_stats.items():
                print(f'      {col}: {count}')
        else:
            print(f'\n   ✅ 无空值')
    else:
        print('❌ 评分文件不存在')

    print()

    # 5. 数据一致性分析
    print('='*100)
    print('🔗 五、数据一致性分析')
    print('='*100)

    if stock_list_path.exists() and kline_dir.exists():
        stock_codes = set(stock_list['code'].to_list())
        kline_codes = set([f.stem for f in parquet_files])

        in_stock_no_kline = stock_codes - kline_codes
        in_kline_no_stock = kline_codes - stock_codes

        print(f'   股票列表: {len(stock_codes)} 只')
        print(f'   K线数据: {len(kline_codes)} 只')
        print(f'   交集: {len(stock_codes & kline_codes)} 只')
        print(f'\n   ⚠️  在股票列表中但无K线数据: {len(in_stock_no_kline)} 只')
        if len(in_stock_no_kline) > 0:
            print(f'      示例: {list(in_stock_no_kline)[:10]}')

        print(f'\n   ⚠️  有K线数据但不在股票列表: {len(in_kline_no_stock)} 只')
        if len(in_kline_no_stock) > 0:
            print(f'      示例: {list(in_kline_no_stock)[:10]}')

    print()

    # 6. 数据新鲜度
    print('='*100)
    print('🕐 六、数据新鲜度评估')
    print('='*100)

    today = datetime.now().strftime('%Y-%m-%d')
    print(f'   今天日期: {today}')

    if scores_path.exists():
        latest_trade_date = scores['trade_date'].max()
        print(f'   最新交易日期: {latest_trade_date}')

        if latest_trade_date == today:
            print(f'   ✅ 数据已更新到今天')
        else:
            print(f'   ⚠️  数据未更新到今天')

    print()
    print('='*100)
    print('✅ 数据质量分析完成')
    print('='*100)


if __name__ == '__main__':
    analyze_data_quality()
