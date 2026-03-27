"""
性能测试脚本
比较 Polars 和 Pandas 的性能差异
"""
import time
import polars as pl
import pandas as pd
from pathlib import Path


def test_polars_performance(data_path: str, iterations: int = 10):
    """测试 Polars 性能"""
    times = []
    
    for _ in range(iterations):
        start = time.time()
        
        # 加载数据
        df = pl.read_parquet(data_path)
        
        # 筛选 S 级股票
        s_grade = df.filter(pl.col('grade') == 'S').filter(pl.col('enhanced_score') >= 80)
        
        # 筛选 A 级股票
        a_grade = df.filter(pl.col('grade') == 'A').filter(
            (pl.col('enhanced_score') >= 75) & (pl.col('enhanced_score') < 80)
        )
        
        # 筛选多头排列
        bullish = df.filter(
            (pl.col('trend') == 100) & 
            (pl.col('change_pct') > 0) & 
            (pl.col('change_pct') < 8)
        )
        
        # 排序
        s_sorted = s_grade.sort('enhanced_score', descending=True)
        a_sorted = a_grade.sort('enhanced_score', descending=True)
        b_sorted = bullish.sort('enhanced_score', descending=True)
        
        end = time.time()
        times.append(end - start)
    
    return {
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': len(df)
    }


def test_pandas_performance(data_path: str, iterations: int = 10):
    """测试 Pandas 性能"""
    times = []
    
    for _ in range(iterations):
        start = time.time()
        
        # 加载数据
        df = pd.read_parquet(data_path)
        
        # 筛选 S 级股票
        s_grade = df[(df['grade'] == 'S') & (df['enhanced_score'] >= 80)]
        
        # 筛选 A 级股票
        a_grade = df[
            (df['grade'] == 'A') & 
            (df['enhanced_score'] >= 75) & 
            (df['enhanced_score'] < 80)
        ]
        
        # 筛选多头排列
        bullish = df[
            (df['trend'] == 100) & 
            (df['change_pct'] > 0) & 
            (df['change_pct'] < 8)
        ]
        
        # 排序
        s_sorted = s_grade.sort_values('enhanced_score', ascending=False)
        a_sorted = a_grade.sort_values('enhanced_score', ascending=False)
        b_sorted = bullish.sort_values('enhanced_score', ascending=False)
        
        end = time.time()
        times.append(end - start)
    
    return {
        'avg_time': sum(times) / len(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': len(df)
    }


def main():
    """运行性能测试"""
    print("=" * 70)
    print("股票推荐系统性能测试")
    print("=" * 70)
    
    # 数据路径
    project_root = Path(__file__).parent.parent
    data_path = project_root / "data" / "enhanced_scores_full.parquet"
    
    if not data_path.exists():
        print(f"❌ 数据文件不存在: {data_path}")
        return
    
    print(f"\n📁 数据文件: {data_path}")
    print(f"📊 测试迭代次数: 10")
    
    # 测试 Polars
    print("\n🔄 测试 Polars 性能...")
    polars_results = test_polars_performance(str(data_path))
    
    # 测试 Pandas
    print("🔄 测试 Pandas 性能...")
    pandas_results = test_pandas_performance(str(data_path))
    
    # 输出结果
    print("\n" + "=" * 70)
    print("性能测试结果")
    print("=" * 70)
    
    print(f"\n📊 数据规模: {polars_results['total_records']} 条记录")
    
    print("\n【Polars 性能】")
    print(f"  平均耗时: {polars_results['avg_time']*1000:.2f} ms")
    print(f"  最小耗时: {polars_results['min_time']*1000:.2f} ms")
    print(f"  最大耗时: {polars_results['max_time']*1000:.2f} ms")
    
    print("\n【Pandas 性能】")
    print(f"  平均耗时: {pandas_results['avg_time']*1000:.2f} ms")
    print(f"  最小耗时: {pandas_results['min_time']*1000:.2f} ms")
    print(f"  最大耗时: {pandas_results['max_time']*1000:.2f} ms")
    
    # 计算性能提升
    speedup = pandas_results['avg_time'] / polars_results['avg_time']
    
    print("\n【性能对比】")
    print(f"  🚀 Polars 比 Pandas 快 {speedup:.2f}x")
    print(f"  ⏱️  节省时间: {(pandas_results['avg_time'] - polars_results['avg_time'])*1000:.2f} ms")
    
    print("\n" + "=" * 70)
    print("性能测试完成")
    print("=" * 70)


if __name__ == '__main__':
    main()
