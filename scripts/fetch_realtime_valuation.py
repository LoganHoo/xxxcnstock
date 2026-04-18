#!/usr/bin/env python3
"""
使用AKShare获取实时估值数据
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def fetch_realtime_valuation():
    """获取实时估值数据"""
    try:
        import akshare as ak
    except ImportError:
        print("安装 akshare...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "akshare", "-q"])
        import akshare as ak
    
    print("=" * 80)
    print("使用AKShare获取实时估值数据")
    print("=" * 80)
    
    # 获取A股实时行情（包含估值数据）
    print("\n正在获取A股实时估值数据...")
    try:
        df = ak.stock_zh_a_spot_em()
        print(f"获取到 {len(df)} 条数据")
        
        # 处理列名
        df.columns = [col.strip() for col in df.columns]
        
        # 查看可用列
        print("\n可用列:", df.columns.tolist())
        
        # 选择需要的列
        # 东方财富的列名可能不同，需要适配
        column_mapping = {
            '代码': 'code',
            '名称': 'name',
            '最新价': 'close',
            '涨跌幅': 'change_pct',
            '换手率': 'turnover',
            '市盈率-动态': 'pe_ttm',
            '市净率': 'pb',
            '总市值': 'total_mv',
            '流通市值': 'float_mv',
            '成交量': 'volume',
            '成交额': 'amount',
        }
        
        # 只保留存在的列
        available_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        print(f"\n匹配到 {len(available_cols)} 个有效列")
        
        df_selected = df[list(available_cols.keys())].copy()
        df_selected.columns = list(available_cols.values())
        
        # 处理代码格式
        df_selected['code'] = df_selected['code'].astype(str).str.zfill(6)
        
        # 过滤无效数据
        df_selected = df_selected[df_selected['code'].str.match(r'^\d{6}$')]
        
        # 转换数值类型
        for col in ['pe_ttm', 'pb', 'total_mv', 'float_mv', 'turnover']:
            if col in df_selected.columns:
                df_selected[col] = pd.to_numeric(df_selected[col], errors='coerce')
        
        # 过滤异常PE值
        if 'pe_ttm' in df_selected.columns:
            valid_pe = (df_selected['pe_ttm'] > 0) & (df_selected['pe_ttm'] < 1000)
            print(f"有效PE数据: {valid_pe.sum()}/{len(df_selected)}")
        
        # 保存数据
        output_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_realtime.parquet"
        output_file.parent.mkdir(exist_ok=True)
        pl.from_pandas(df_selected).write_parquet(output_file)
        print(f"\n实时估值数据已保存: {output_file}")
        print(f"共 {len(df_selected)} 条记录")
        
        # 显示数据示例
        print("\n数据示例:")
        print(df_selected[['code', 'name', 'pe_ttm', 'pb', 'total_mv', 'turnover']].head(10))
        
        # 显示统计
        print("\n数据统计:")
        print(df_selected[['pe_ttm', 'pb', 'total_mv']].describe())
        
        return df_selected
        
    except Exception as e:
        print(f"获取数据失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def merge_to_stock_list():
    """合并实时估值数据到股票列表"""
    print("\n" + "=" * 80)
    print("合并实时估值数据到股票列表")
    print("=" * 80)
    
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    valuation_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_realtime.parquet"
    
    if not valuation_file.exists():
        print("估值数据不存在")
        return
    
    df_stocks = pl.read_parquet(stock_list_file)
    df_val = pl.read_parquet(valuation_file)
    
    print(f"股票列表: {len(df_stocks)} 只")
    print(f"估值数据: {len(df_val)} 条")
    
    # 合并数据
    df_stocks = df_stocks.join(
        df_val.select(['code', 'pe_ttm', 'pb', 'total_mv', 'turnover']), 
        on='code', 
        how='left'
    )
    
    # 统计覆盖情况
    pe_coverage = df_stocks.filter(pl.col('pe_ttm').is_not_null()).shape[0]
    pb_coverage = df_stocks.filter(pl.col('pb').is_not_null()).shape[0]
    
    print(f"\n数据覆盖情况:")
    print(f"  PE覆盖率: {pe_coverage}/{len(df_stocks)} ({pe_coverage/len(df_stocks)*100:.1f}%)")
    print(f"  PB覆盖率: {pb_coverage}/{len(df_stocks)} ({pb_coverage/len(df_stocks)*100:.1f}%)")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表")
    
    # 显示有估值数据的股票示例
    print("\n有真实估值数据的股票示例:")
    sample = df_stocks.filter(
        pl.col('pe_ttm').is_not_null() & pl.col('pb').is_not_null()
    ).head(10)
    print(sample.select(['code', 'name', 'industry', 'pe_ttm', 'pb']))
    
    return df_stocks


if __name__ == "__main__":
    df = fetch_realtime_valuation()
    if df is not None:
        merge_to_stock_list()
        print("\n完成!")
