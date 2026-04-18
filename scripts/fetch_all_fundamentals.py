#!/usr/bin/env python3
"""
获取所有股票的真实基本面数据 - 完整版
使用 Baostock 获取 PE、PB、ROE 等数据
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def convert_code(code):
    """转换代码格式为 baostock 格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    return f"sz.{code}"

def fetch_all_data():
    """获取所有股票数据"""
    try:
        import baostock as bs
    except ImportError:
        print("安装 baostock...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "baostock", "-q"])
        import baostock as bs
    
    # 登录
    print("登录 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return
    print(f"登录成功!")
    
    # 读取股票列表
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    df_stocks = pl.read_parquet(stock_list_file)
    stock_codes = df_stocks['code'].to_list()
    print(f"总共 {len(stock_codes)} 只股票需要处理")
    
    # 获取估值数据 - 使用更简单的接口
    print("\n" + "=" * 80)
    print("获取估值数据 (PE/PB/PS)...")
    print("=" * 80)
    
    valuation_data = []
    batch_size = 100
    total_batches = (len(stock_codes) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(stock_codes))
        batch_codes = stock_codes[start_idx:end_idx]
        
        print(f"处理第 {batch_idx + 1}/{total_batches} 批 ({start_idx + 1}-{end_idx})...")
        
        for code in batch_codes:
            try:
                bs_code = convert_code(code)
                # 获取最新交易日的估值数据
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM,turnoverRatio",
                    start_date='2025-04-10',
                    end_date='2025-04-17',
                    frequency="d",
                    adjustflag="3"
                )
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        # 取最新一天的数据
                        latest = data_list[-1]
                        pe = float(latest[2]) if latest[2] and latest[2] != '' else None
                        pb = float(latest[3]) if latest[3] and latest[3] != '' else None
                        ps = float(latest[4]) if latest[4] and latest[4] != '' else None
                        
                        # 过滤异常值
                        if pe and 0 < pe < 1000:
                            valuation_data.append({
                                'code': code,
                                'pe_ttm': pe,
                                'pb': pb if pb and 0 < pb < 100 else None,
                                'ps': ps if ps and 0 < ps < 1000 else None,
                            })
                
                time.sleep(0.03)  # 控制请求频率
            except Exception as e:
                continue
        
        if (batch_idx + 1) % 10 == 0:
            print(f"  已获取 {len(valuation_data)} 条估值数据")
    
    # 保存估值数据
    if valuation_data:
        df_val = pd.DataFrame(valuation_data)
        output_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_real.parquet"
        pl.from_pandas(df_val).write_parquet(output_file)
        print(f"\n估值数据已保存: {output_file}")
        print(f"共 {len(df_val)} 条记录")
        print(f"数据预览:")
        print(df_val.describe())
    
    # 获取财务数据
    print("\n" + "=" * 80)
    print("获取财务数据 (ROE/营收增长)...")
    print("=" * 80)
    
    financial_data = []
    # 只获取有估值数据的股票的财务数据
    if valuation_data:
        codes_with_valuation = [v['code'] for v in valuation_data]
    else:
        codes_with_valuation = stock_codes[:1000]  # 限制数量
    
    total_fin = len(codes_with_valuation)
    for i, code in enumerate(codes_with_valuation):
        try:
            bs_code = convert_code(code)
            
            # 获取盈利能力
            rs_profit = bs.query_profit_data(code=bs_code, year=2024, quarter=3)
            roe = None
            if rs_profit.error_code == '0' and rs_profit.next():
                row = rs_profit.get_row_data()
                if len(row) > 4 and row[4]:
                    try:
                        roe = float(row[4])
                    except:
                        pass
            
            # 获取成长能力
            rs_growth = bs.query_growth_data(code=bs_code, year=2024, quarter=3)
            revenue_growth = None
            profit_growth = None
            if rs_growth.error_code == '0' and rs_growth.next():
                row = rs_growth.get_row_data()
                if len(row) > 3 and row[3]:
                    try:
                        revenue_growth = float(row[3])
                    except:
                        pass
                if len(row) > 4 and row[4]:
                    try:
                        profit_growth = float(row[4])
                    except:
                        pass
            
            # 只保存有效数据
            if roe is not None or revenue_growth is not None:
                financial_data.append({
                    'code': code,
                    'roe': roe,
                    'revenue_growth': revenue_growth,
                    'profit_growth': profit_growth
                })
            
            if (i + 1) % 100 == 0:
                print(f"  已处理 {i + 1}/{total_fin}, 获取 {len(financial_data)} 条有效数据")
            time.sleep(0.05)
        except Exception as e:
            continue
    
    # 保存财务数据
    if financial_data:
        df_fin = pd.DataFrame(financial_data)
        output_file = PROJECT_ROOT / "data" / "fundamental" / "financial_real.parquet"
        pl.from_pandas(df_fin).write_parquet(output_file)
        print(f"\n财务数据已保存: {output_file}")
        print(f"共 {len(df_fin)} 条记录")
        print(f"数据预览:")
        print(df_fin.describe())
    
    bs.logout()
    print("\n" + "=" * 80)
    print("数据获取完成!")
    print("=" * 80)


def merge_real_data():
    """合并真实数据到股票列表"""
    print("\n" + "=" * 80)
    print("合并真实数据到股票列表")
    print("=" * 80)
    
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    valuation_file = PROJECT_ROOT / "data" / "fundamental" / "valuation_real.parquet"
    financial_file = PROJECT_ROOT / "data" / "fundamental" / "financial_real.parquet"
    industry_file = PROJECT_ROOT / "data" / "fundamental" / "industry_baostock.parquet"
    
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")
    
    # 合并估值数据
    if valuation_file.exists():
        df_val = pl.read_parquet(valuation_file)
        df_stocks = df_stocks.join(df_val, on='code', how='left')
        non_null = df_stocks.filter(pl.col('pe_ttm').is_not_null()).shape[0]
        print(f"合并估值数据: {non_null}/{len(df_stocks)} 只 ({non_null/len(df_stocks)*100:.1f}%)")
    
    # 合并财务数据
    if financial_file.exists():
        df_fin = pl.read_parquet(financial_file)
        df_stocks = df_stocks.join(df_fin, on='code', how='left')
        non_null = df_stocks.filter(pl.col('roe').is_not_null()).shape[0]
        print(f"合并财务数据: {non_null}/{len(df_stocks)} 只 ({non_null/len(df_stocks)*100:.1f}%)")
    
    # 合并行业数据
    if industry_file.exists():
        df_ind = pl.read_parquet(industry_file)
        # 清理空行业
        df_ind = df_ind.with_columns([
            pl.when(pl.col('industry') == '')
            .then(pl.lit('未知'))
            .otherwise(pl.col('industry'))
            .alias('industry')
        ])
        df_ind = df_ind.unique(subset=['code'], keep='first')
        df_stocks = df_stocks.join(df_ind, on='code', how='left')
        df_stocks = df_stocks.with_columns([
            pl.col('industry').fill_null('未知').alias('industry')
        ])
        print(f"合并行业数据完成")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表: {stock_list_file}")
    
    # 显示统计
    print("\n数据覆盖情况:")
    for col in ['pe_ttm', 'pb', 'roe', 'revenue_growth', 'industry']:
        if col in df_stocks.columns:
            null_count = df_stocks.filter(pl.col(col).is_null()).shape[0]
            coverage = (len(df_stocks) - null_count) / len(df_stocks) * 100
            print(f"  {col}: {len(df_stocks) - null_count}/{len(df_stocks)} ({coverage:.1f}%)")
    
    # 显示数据示例
    print("\n数据示例 (有完整基本面数据的股票):")
    sample = df_stocks.filter(
        pl.col('pe_ttm').is_not_null() & 
        pl.col('roe').is_not_null()
    ).head(5)
    print(sample.select(['code', 'name', 'industry', 'pe_ttm', 'pb', 'roe', 'revenue_growth']))
    
    return df_stocks


if __name__ == "__main__":
    fetch_all_data()
    merge_real_data()
