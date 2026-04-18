#!/usr/bin/env python3
"""
使用微服务获取股票基本面数据

改造说明:
1. 从直接调用Baostock改为调用微服务UnifiedFetcher
2. 利用微服务的主备数据源自动切换
3. 统一数据验证和错误处理
4. 支持批量获取和增量更新
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime
import time
import asyncio
from typing import List, Dict, Optional, Tuple
from multiprocessing import Pool, cpu_count
from dataclasses import dataclass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.data_service.fetchers.unified_fetcher import UnifiedFetcher, get_unified_fetcher
from core.logger import setup_logger

logger = setup_logger("fetch_fundamental", log_file="system/fetch_fundamental.log")


@dataclass
class Config:
    """配置类"""
    # 进程配置
    max_workers: int = min(4, cpu_count())
    batch_size: int = 100
    
    # 请求频率控制
    request_delay: float = 0.05
    
    # 数据质量阈值
    max_pe: float = 10000.0
    max_pb: float = 1000.0


config = Config()


# ==================== 微服务调用函数 ====================

async def fetch_fundamental_via_service(code: str) -> Optional[Dict]:
    """通过微服务获取基本面数据"""
    try:
        fetcher = await get_unified_fetcher()
        fundamental = await fetcher.fetch_fundamental(code)
        if fundamental:
            return {
                'code': fundamental.code,
                'pe_ttm': fundamental.pe_ttm,
                'pb': fundamental.pb,
                'ps_ttm': fundamental.ps_ttm,
                'pcf': fundamental.pcf,
                'total_mv': fundamental.total_mv,
                'float_mv': fundamental.float_mv,
                'turnover': fundamental.turnover,
                'date': fundamental.date
            }
        return None
    except Exception as e:
        logger.warning(f"{code} 微服务获取基本面失败: {e}")
        return None


def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ==================== 数据获取函数 ====================

def fetch_valuation_data(codes: List[str]) -> pd.DataFrame:
    """
    获取估值数据
    
    Args:
        codes: 股票代码列表
    Returns:
        估值数据DataFrame
    """
    print(f"获取 {len(codes)} 只股票的估值数据...")
    
    valuation_data = []
    failed_codes = []
    
    # 初始化微服务获取器
    run_async(get_unified_fetcher())
    
    for i, code in enumerate(codes, 1):
        try:
            data = run_async(fetch_fundamental_via_service(code))
            
            if data:
                # 过滤异常值
                pe_ttm = data.get('pe_ttm')
                pb = data.get('pb')
                
                if pe_ttm and (pe_ttm < 0 or pe_ttm > config.max_pe):
                    pe_ttm = None
                if pb and (pb < 0 or pb > config.max_pb):
                    pb = None
                
                valuation_data.append({
                    'code': code,
                    'pe_ttm': pe_ttm,
                    'pb': pb,
                    'ps_ttm': data.get('ps_ttm'),
                    'pcf': data.get('pcf'),
                    'turnover': data.get('turnover'),
                    'total_mv': data.get('total_mv'),
                    'float_mv': data.get('float_mv'),
                })
            else:
                failed_codes.append(code)
            
            if i % 100 == 0:
                print(f"  已处理 {i}/{len(codes)} 只股票，成功: {len(valuation_data)}")
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.warning(f"获取 {code} 估值数据失败: {e}")
            failed_codes.append(code)
            continue
    
    if failed_codes:
        print(f"  失败: {len(failed_codes)} 只股票")
    
    if valuation_data:
        df = pd.DataFrame(valuation_data)
        print(f"\n估值数据获取完成: {len(df)} 条记录")
        return df
    
    return pd.DataFrame()


def fetch_fundamental_batch(args: Tuple[List[str], int]) -> List[Dict]:
    """
    批量获取基本面数据（用于多进程）
    
    Args:
        args: (codes_batch, batch_index)
    Returns:
        基本面数据列表
    """
    codes_batch, batch_index = args
    results = []
    
    # 初始化微服务获取器
    run_async(get_unified_fetcher())
    
    for code in codes_batch:
        try:
            data = run_async(fetch_fundamental_via_service(code))
            if data:
                # 过滤异常值
                pe_ttm = data.get('pe_ttm')
                pb = data.get('pb')
                
                if pe_ttm and (pe_ttm < 0 or pe_ttm > config.max_pe):
                    pe_ttm = None
                if pb and (pb < 0 or pb > config.max_pb):
                    pb = None
                
                results.append({
                    'code': code,
                    'pe_ttm': pe_ttm,
                    'pb': pb,
                    'ps_ttm': data.get('ps_ttm'),
                    'pcf': data.get('pcf'),
                    'turnover': data.get('turnover'),
                    'total_mv': data.get('total_mv'),
                    'float_mv': data.get('float_mv'),
                })
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.warning(f"批量获取 {code} 失败: {e}")
            continue
    
    return results


def fetch_valuation_data_parallel(codes: List[str]) -> pd.DataFrame:
    """
    并行获取估值数据
    
    Args:
        codes: 股票代码列表
    Returns:
        估值数据DataFrame
    """
    print(f"并行获取 {len(codes)} 只股票的估值数据...")
    print(f"使用 {config.max_workers} 个进程")
    
    # 分批处理
    batches = [
        (codes[i:i + config.batch_size], i // config.batch_size)
        for i in range(0, len(codes), config.batch_size)
    ]
    
    all_results = []
    
    with Pool(processes=config.max_workers) as pool:
        results = pool.map(fetch_fundamental_batch, batches)
        for batch_results in results:
            all_results.extend(batch_results)
    
    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\n估值数据获取完成: {len(df)} 条记录")
        return df
    
    return pd.DataFrame()


def fetch_industry_data() -> pd.DataFrame:
    """
    获取行业数据
    
    Returns:
        行业数据DataFrame
    """
    print("\n获取行业数据...")
    
    try:
        # 通过微服务获取股票列表（包含行业信息）
        async def fetch_stock_list():
            fetcher = await get_unified_fetcher()
            return await fetcher.fetch_stock_list()
        
        df_stocks = run_async(fetch_stock_list())
        
        if not df_stocks.empty and 'industry' in df_stocks.columns:
            df_industry = df_stocks[['code', 'industry']].copy()
            df_industry['industry'] = df_industry['industry'].fillna('未知')
            print(f"行业数据获取完成: {len(df_industry)} 条记录")
            return df_industry
        
        print("警告: 无法获取行业数据")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"获取行业数据失败: {e}")
        return pd.DataFrame()


def save_fundamental_data(df_valuation: pd.DataFrame, df_industry: pd.DataFrame):
    """
    保存基本面数据
    
    Args:
        df_valuation: 估值数据
        df_industry: 行业数据
    """
    output_dir = PROJECT_ROOT / "data" / "fundamental"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存估值数据
    if not df_valuation.empty:
        output_file = output_dir / "valuation.parquet"
        pl.from_pandas(df_valuation).write_parquet(output_file)
        print(f"\n估值数据已保存: {output_file}")
        print(f"共 {len(df_valuation)} 条记录")
        print(df_valuation.head())
    
    # 保存行业数据
    if not df_industry.empty:
        output_file = output_dir / "industry.parquet"
        pl.from_pandas(df_industry).write_parquet(output_file)
        print(f"\n行业数据已保存: {output_file}")
        print(f"共 {len(df_industry)} 条记录")


def merge_fundamental_data():
    """合并基本面数据到股票列表"""
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    valuation_file = PROJECT_ROOT / "data" / "fundamental" / "valuation.parquet"
    industry_file = PROJECT_ROOT / "data" / "fundamental" / "industry.parquet"
    
    if not stock_list_file.exists():
        print("股票列表不存在")
        return
    
    df_stocks = pl.read_parquet(stock_list_file)
    print(f"原始股票列表: {len(df_stocks)} 只")
    
    # 合并估值数据
    if valuation_file.exists():
        df_val = pl.read_parquet(valuation_file)
        df_stocks = df_stocks.join(df_val, on='code', how='left')
        print(f"合并估值数据: {len(df_val)} 条")
    
    # 合并行业数据
    if industry_file.exists():
        df_ind = pl.read_parquet(industry_file)
        df_stocks = df_stocks.join(df_ind, on='code', how='left')
        df_stocks = df_stocks.with_columns([
            pl.col('industry').fill_null('未知').alias('industry')
        ])
        print(f"合并行业数据: {len(df_ind)} 条")
    
    # 保存
    df_stocks.write_parquet(stock_list_file)
    print(f"\n已更新股票列表")
    
    # 显示统计
    print("\n数据覆盖情况:")
    for col in ['pe_ttm', 'pb', 'ps_ttm', 'industry']:
        if col in df_stocks.columns:
            null_count = df_stocks.filter(pl.col(col).is_null()).shape[0]
            coverage = (len(df_stocks) - null_count) / len(df_stocks) * 100
            print(f"  {col}: {len(df_stocks) - null_count}/{len(df_stocks)} ({coverage:.1f}%)")
    
    return df_stocks


def main():
    """主函数"""
    print("=" * 80)
    print("使用微服务获取股票基本面数据")
    print("=" * 80)
    
    # 读取股票列表
    stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
    if not stock_list_file.exists():
        print("错误: 股票列表不存在，请先运行 fetch_stock_list.py")
        sys.exit(1)
    
    df_stocks = pl.read_parquet(stock_list_file)
    stock_codes = df_stocks['code'].to_list()
    print(f"股票列表: {len(stock_codes)} 只")
    
    # 获取估值数据（使用并行处理）
    print("\n" + "-" * 80)
    print("获取估值数据")
    print("-" * 80)
    
    if len(stock_codes) > 500:
        # 大量股票使用并行处理
        df_valuation = fetch_valuation_data_parallel(stock_codes)
    else:
        # 少量股票使用串行处理
        df_valuation = fetch_valuation_data(stock_codes)
    
    # 获取行业数据
    print("\n" + "-" * 80)
    print("获取行业数据")
    print("-" * 80)
    df_industry = fetch_industry_data()
    
    # 保存数据
    print("\n" + "-" * 80)
    print("保存数据")
    print("-" * 80)
    save_fundamental_data(df_valuation, df_industry)
    
    # 合并数据
    print("\n" + "-" * 80)
    print("合并数据到股票列表")
    print("-" * 80)
    merge_fundamental_data()
    
    print("\n" + "=" * 80)
    print("完成!")
    print("=" * 80)


if __name__ == "__main__":
    main()
