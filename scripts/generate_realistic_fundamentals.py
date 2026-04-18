#!/usr/bin/env python3
"""
生成逼真的模拟基本面数据（用于测试）
基于真实市场统计规律生成
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def generate_realistic_fundamentals():
    """生成逼真的基本面数据"""
    print("=" * 80)
    print("生成逼真的模拟基本面数据")
    print("=" * 80)
    
    # 读取股票列表
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    df_stocks = pl.read_parquet(stock_list_file)
    stock_codes = df_stocks['code'].to_list()
    industries = df_stocks['industry'].to_list() if 'industry' in df_stocks.columns else ['未知'] * len(stock_codes)
    
    print(f"为 {len(stock_codes)} 只股票生成基本面数据...")
    
    # 行业基准值 (基于A股市场真实统计)
    industry_benchmarks = {
        '银行': {'pe': 5.5, 'pb': 0.6, 'roe': 0.10, 'revenue_growth': 0.05},
        '证券': {'pe': 18.0, 'pb': 1.4, 'roe': 0.08, 'revenue_growth': 0.15},
        '保险': {'pe': 12.0, 'pb': 1.2, 'roe': 0.12, 'revenue_growth': 0.08},
        '房地产': {'pe': 10.0, 'pb': 0.9, 'roe': 0.08, 'revenue_growth': -0.05},
        '钢铁': {'pe': 8.0, 'pb': 0.8, 'roe': 0.06, 'revenue_growth': 0.02},
        '煤炭': {'pe': 7.0, 'pb': 1.0, 'roe': 0.15, 'revenue_growth': -0.10},
        '有色金属': {'pe': 20.0, 'pb': 2.0, 'roe': 0.12, 'revenue_growth': 0.20},
        '汽车': {'pe': 15.0, 'pb': 1.5, 'roe': 0.10, 'revenue_growth': 0.12},
        '家电': {'pe': 14.0, 'pb': 2.5, 'roe': 0.15, 'revenue_growth': 0.08},
        '食品饮料': {'pe': 25.0, 'pb': 5.0, 'roe': 0.18, 'revenue_growth': 0.15},
        '医药': {'pe': 30.0, 'pb': 3.5, 'roe': 0.12, 'revenue_growth': 0.18},
        '电子': {'pe': 35.0, 'pb': 3.0, 'roe': 0.10, 'revenue_growth': 0.25},
        '计算机': {'pe': 40.0, 'pb': 3.5, 'roe': 0.08, 'revenue_growth': 0.20},
        '新能源': {'pe': 25.0, 'pb': 2.5, 'roe': 0.12, 'revenue_growth': 0.30},
        '传媒': {'pe': 25.0, 'pb': 2.0, 'roe': 0.08, 'revenue_growth': 0.10},
        '通信': {'pe': 20.0, 'pb': 1.8, 'roe': 0.08, 'revenue_growth': 0.12},
        '创业板科技': {'pe': 45.0, 'pb': 4.0, 'roe': 0.10, 'revenue_growth': 0.35},
        '中小板科技': {'pe': 35.0, 'pb': 3.0, 'roe': 0.11, 'revenue_growth': 0.25},
    }
    
    default_benchmark = {'pe': 20.0, 'pb': 2.0, 'roe': 0.10, 'revenue_growth': 0.10}
    
    np.random.seed(42)  # 保证可重复
    
    valuation_data = []
    financial_data = []
    
    for code, industry in zip(stock_codes, industries):
        # 获取行业基准
        benchmark = industry_benchmarks.get(industry, default_benchmark)
        
        # 添加随机波动
        pe = benchmark['pe'] * np.random.lognormal(0, 0.3)
        pb = benchmark['pb'] * np.random.lognormal(0, 0.25)
        roe = benchmark['roe'] * np.random.lognormal(0, 0.2)
        revenue_growth = benchmark['revenue_growth'] + np.random.normal(0, 0.1)
        
        # 确保数值合理
        pe = max(3, min(pe, 100))
        pb = max(0.3, min(pb, 10))
        roe = max(-0.2, min(roe, 0.5))
        revenue_growth = max(-0.5, min(revenue_growth, 1.0))
        
        # 估值数据
        valuation_data.append({
            'code': code,
            'pe_ttm': round(pe, 2),
            'pb': round(pb, 2),
            'ps_ttm': round(pe * 0.5, 2),  # PS约为PE的一半
            'total_mv': np.random.uniform(5, 500),  # 市值5-500亿
        })
        
        # 财务数据
        profit_growth = revenue_growth * np.random.uniform(0.8, 1.2)
        financial_data.append({
            'code': code,
            'roe': round(roe * 100, 2),  # 转为百分比
            'roa': round(roe * 0.6, 2),  # ROA约为ROE的60%
            'revenue_growth': round(revenue_growth * 100, 2),
            'profit_growth': round(profit_growth * 100, 2),
            'gross_margin': round(np.random.uniform(15, 60), 2),
            'net_margin': round(np.random.uniform(5, 25), 2),
            'debt_ratio': round(np.random.uniform(30, 70), 2),
            'eps': round(np.random.uniform(0.1, 5), 2),
            'bps': round(pb * np.random.uniform(5, 50), 2),
        })
    
    # 保存估值数据
    df_val = pd.DataFrame(valuation_data)
    output_dir = PROJECT_ROOT / "data" / "fundamental"
    output_dir.mkdir(exist_ok=True)
    
    val_file = output_dir / "valuation_realistic.parquet"
    pl.from_pandas(df_val).write_parquet(val_file)
    print(f"\n估值数据已保存: {val_file}")
    print(f"共 {len(df_val)} 条记录")
    print("\n估值数据统计:")
    print(df_val[['pe_ttm', 'pb']].describe())
    
    # 保存财务数据
    df_fin = pd.DataFrame(financial_data)
    fin_file = output_dir / "financial_realistic.parquet"
    pl.from_pandas(df_fin).write_parquet(fin_file)
    print(f"\n财务数据已保存: {fin_file}")
    print(f"共 {len(df_fin)} 条记录")
    print("\n财务数据统计:")
    print(df_fin[['roe', 'revenue_growth']].describe())
    
    return df_val, df_fin


def merge_to_stock_list():
    """合并到股票列表"""
    print("\n" + "=" * 80)
    print("合并数据到股票列表")
    print("=" * 80)
    
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    valuation_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_realistic.parquet"
    financial_file = PROJECT_ROOT / "data" / "fundamental" / "financial_realistic.parquet"
    
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")
    
    # 合并估值数据
    if valuation_file.exists():
        df_val = pl.read_parquet(valuation_file)
        df_stocks = df_stocks.join(df_val, on='code', how='left')
        print(f"合并估值数据: {len(df_val)} 条")
    
    # 合并财务数据
    if financial_file.exists():
        df_fin = pl.read_parquet(financial_file)
        df_stocks = df_stocks.join(df_fin, on='code', how='left')
        print(f"合并财务数据: {len(df_fin)} 条")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表: {stock_list_file}")
    
    # 显示统计
    print("\n数据覆盖情况:")
    for col in ['pe_ttm', 'pb', 'roe', 'revenue_growth']:
        if col in df_stocks.columns:
            null_count = df_stocks.filter(pl.col(col).is_null()).shape[0]
            coverage = (len(df_stocks) - null_count) / len(df_stocks) * 100
            print(f"  {col}: {len(df_stocks) - null_count}/{len(df_stocks)} ({coverage:.1f}%)")
    
    # 显示数据示例
    print("\n数据示例:")
    sample = df_stocks.head(10)
    print(sample.select(['code', 'name', 'industry', 'pe_ttm', 'pb', 'roe', 'revenue_growth']))
    
    return df_stocks


if __name__ == "__main__":
    generate_realistic_fundamentals()
    merge_to_stock_list()
    print("\n完成!")
