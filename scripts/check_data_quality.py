#!/usr/bin/env python3
"""
数据质量检查工具

检查内容:
- K线数据完整性
- 股票列表有效性
- 评分数据有效性
- 数据日期一致性
- 缺失值检测
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import pandas as pd


def check_kline_data_quality():
    """检查K线数据质量"""
    print("\n" + "="*100)
    print("📊 K线数据质量检查")
    print("="*100)
    
    kline_dir = Path('data/kline')
    if not kline_dir.exists():
        print("❌ K线数据目录不存在")
        return None
    
    all_files = list(kline_dir.glob("*.parquet"))
    total_files = len(all_files)
    
    print(f"\n总文件数: {total_files}")
    
    # 统计指标
    stats = {
        'total_files': total_files,
        'valid_files': 0,
        'empty_files': 0,
        'corrupt_files': 0,
        'date_stats': defaultdict(int),
        'missing_columns': [],
        'empty_data_codes': []
    }
    
    # 必要的列
    required_columns = ['trade_date', 'code', 'open', 'high', 'low', 'close', 'volume']
    
    # 检查每个文件
    latest_dates = []
    for i, f in enumerate(all_files):
        if i % 1000 == 0 and i > 0:
            print(f"  已检查 {i}/{total_files} 个文件...")
        
        try:
            df = pl.read_parquet(f)
            
            if df.is_empty():
                stats['empty_files'] += 1
                stats['empty_data_codes'].append(f.stem)
                continue
            
            # 检查必要列
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                stats['missing_columns'].append((f.stem, missing_cols))
                continue
            
            stats['valid_files'] += 1
            
            # 统计最新日期
            if 'trade_date' in df.columns:
                latest_date = df['trade_date'].max()
                stats['date_stats'][str(latest_date)] += 1
                latest_dates.append(latest_date)
                
        except Exception as e:
            stats['corrupt_files'] += 1
    
    # 输出结果
    print(f"\n✅ 有效文件: {stats['valid_files']}")
    print(f"⚠️  空文件: {stats['empty_files']}")
    print(f"❌ 损坏文件: {stats['corrupt_files']}")
    
    if stats['empty_data_codes']:
        print(f"\n⚠️  空数据股票 ({len(stats['empty_data_codes'])} 只):")
        print(f"   {', '.join(stats['empty_data_codes'][:10])}{'...' if len(stats['empty_data_codes']) > 10 else ''}")
    
    if stats['missing_columns']:
        print(f"\n⚠️  缺失列的股票 ({len(stats['missing_columns'])} 只):")
        for code, cols in stats['missing_columns'][:5]:
            print(f"   {code}: {cols}")
    
    # 日期分布
    print("\n📅 数据日期分布:")
    for date, count in sorted(stats['date_stats'].items(), reverse=True)[:10]:
        print(f"   {date}: {count} 只股票")
    
    # 数据新鲜度
    if latest_dates:
        overall_latest = max(latest_dates)
        days_old = (datetime.now() - pd.to_datetime(overall_latest)).days
        print(f"\n🕐 数据新鲜度:")
        print(f"   最新数据日期: {overall_latest}")
        print(f"   数据延迟: {days_old} 天")
        
        if days_old > 30:
            print(f"   ⚠️  警告: 数据过于陈旧!")
    
    return stats


def check_stock_list_quality():
    """检查股票列表质量"""
    print("\n" + "="*100)
    print("📋 股票列表质量检查")
    print("="*100)
    
    stock_list_file = Path('data/stock_list.parquet')
    if not stock_list_file.exists():
        print("❌ 股票列表文件不存在")
        return None
    
    try:
        df = pl.read_parquet(stock_list_file)
        print(f"\n总股票数: {len(df)}")
        print(f"列名: {df.columns}")
        
        # 检查必要列
        required_cols = ['code', 'name']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ 缺失必要列: {missing_cols}")
        else:
            print("✅ 必要列完整")
        
        # 检查空值
        null_stats = {}
        for col in df.columns:
            null_count = df[col].is_null().sum()
            if null_count > 0:
                null_stats[col] = null_count
        
        if null_stats:
            print("\n⚠️  空值统计:")
            for col, count in null_stats.items():
                print(f"   {col}: {count} 个空值")
        else:
            print("✅ 无空值")
        
        # 检查tradeStatus
        if 'tradeStatus' in df.columns:
            status_counts = df['tradeStatus'].value_counts()
            print(f"\n📊 交易状态分布:")
            for row in status_counts.iter_rows(named=True):
                status = row['tradeStatus']
                count = row['count']
                status_desc = '正常交易' if status == '1' else '停牌/退市'
                print(f"   {status} ({status_desc}): {count} 只")
        
        return {
            'total': len(df),
            'columns': df.columns,
            'null_stats': null_stats
        }
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return None


def check_score_data_quality():
    """检查评分数据质量"""
    print("\n" + "="*100)
    print("⭐ 评分数据质量检查")
    print("="*100)
    
    score_file = Path('data/enhanced_scores_full.parquet')
    if not score_file.exists():
        print("❌ 评分数据文件不存在")
        return None
    
    try:
        df = pl.read_parquet(score_file)
        print(f"\n总股票数: {len(df)}")
        print(f"列名: {df.columns}")
        
        # 检查必要列
        required_cols = ['code', 'enhanced_score']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"❌ 缺失必要列: {missing_cols}")
        else:
            print("✅ 必要列完整")
        
        # 评分统计
        if 'enhanced_score' in df.columns:
            scores = df['enhanced_score'].to_list()
            print(f"\n📊 评分统计:")
            print(f"   平均: {sum(scores)/len(scores):.2f}")
            print(f"   最高: {max(scores):.0f}")
            print(f"   最低: {min(scores):.0f}")
            print(f"   中位数: {sorted(scores)[len(scores)//2]:.2f}")
            
            # 分布
            high = sum(1 for s in scores if s >= 80)
            mid = sum(1 for s in scores if 60 <= s < 80)
            low = sum(1 for s in scores if s < 60)
            
            print(f"\n📈 评分分布:")
            print(f"   高分 (>=80): {high} 只 ({high/len(scores)*100:.1f}%)")
            print(f"   中分 (60-79): {mid} 只 ({mid/len(scores)*100:.1f}%)")
            print(f"   低分 (<60): {low} 只 ({low/len(scores)*100:.1f}%)")
        
        # 检查空值
        null_stats = {}
        for col in df.columns:
            null_count = df[col].is_null().sum()
            if null_count > 0:
                null_stats[col] = null_count
        
        if null_stats:
            print("\n⚠️  空值统计:")
            for col, count in null_stats.items():
                print(f"   {col}: {count} 个空值")
        else:
            print("✅ 无空值")
        
        return {
            'total': len(df),
            'columns': df.columns,
            'null_stats': null_stats
        }
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return None


def check_data_consistency():
    """检查数据一致性"""
    print("\n" + "="*100)
    print("🔗 数据一致性检查")
    print("="*100)
    
    # 检查股票列表和K线数据的一致性
    stock_list_file = Path('data/stock_list.parquet')
    kline_dir = Path('data/kline')
    
    if not stock_list_file.exists() or not kline_dir.exists():
        print("❌ 必要文件不存在")
        return
    
    try:
        stock_list = pl.read_parquet(stock_list_file)
        list_codes = set(stock_list['code'].to_list())
        
        kline_codes = set()
        for f in kline_dir.glob("*.parquet"):
            kline_codes.add(f.stem)
        
        # 检查差异
        in_list_not_kline = list_codes - kline_codes
        in_kline_not_list = kline_codes - list_codes
        
        print(f"\n股票列表: {len(list_codes)} 只")
        print(f"K线数据: {len(kline_codes)} 只")
        print(f"交集: {len(list_codes & kline_codes)} 只")
        
        if in_list_not_kline:
            print(f"\n⚠️  在股票列表中但无K线数据: {len(in_list_not_kline)} 只")
            print(f"   示例: {', '.join(list(in_list_not_kline)[:10])}")
        
        if in_kline_not_list:
            print(f"\n⚠️  有K线数据但不在股票列表: {len(in_kline_not_list)} 只")
            print(f"   示例: {', '.join(list(in_kline_not_list)[:10])}")
        
        if not in_list_not_kline and not in_kline_not_list:
            print("✅ 数据一致性良好")
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")


def generate_quality_report():
    """生成完整的数据质量报告"""
    print("\n" + "="*100)
    print("📋 数据质量总报告")
    print("="*100)
    print(f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行所有检查
    kline_stats = check_kline_data_quality()
    stock_stats = check_stock_list_quality()
    score_stats = check_score_data_quality()
    check_data_consistency()
    
    # 总体评估
    print("\n" + "="*100)
    print("🎯 总体评估")
    print("="*100)
    
    issues = []
    
    if kline_stats:
        if kline_stats['empty_files'] > 0:
            issues.append(f"K线数据: {kline_stats['empty_files']} 个空文件")
        if kline_stats['corrupt_files'] > 0:
            issues.append(f"K线数据: {kline_stats['corrupt_files']} 个损坏文件")
    
    if stock_stats and stock_stats['null_stats']:
        issues.append(f"股票列表: {len(stock_stats['null_stats'])} 列存在空值")
    
    if score_stats and score_stats['null_stats']:
        issues.append(f"评分数据: {len(score_stats['null_stats'])} 列存在空值")
    
    if issues:
        print("\n⚠️  发现的问题:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("\n✅ 数据质量良好，未发现明显问题")
    
    print("\n" + "="*100)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据质量检查工具')
    parser.add_argument('--kline', action='store_true', help='只检查K线数据')
    parser.add_argument('--stock-list', action='store_true', help='只检查股票列表')
    parser.add_argument('--scores', action='store_true', help='只检查评分数据')
    parser.add_argument('--consistency', action='store_true', help='只检查数据一致性')
    parser.add_argument('--all', action='store_true', help='检查所有')
    
    args = parser.parse_args()
    
    if args.kline:
        check_kline_data_quality()
    elif args.stock_list:
        check_stock_list_quality()
    elif args.scores:
        check_score_data_quality()
    elif args.consistency:
        check_data_consistency()
    else:
        generate_quality_report()


if __name__ == '__main__':
    main()
