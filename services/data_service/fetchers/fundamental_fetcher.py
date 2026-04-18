#!/usr/bin/env python3
"""
基本面数据获取器 - 微服务内部使用

功能:
1. 通过UnifiedFetcher获取基本面数据
2. 支持批量获取和并行处理
3. 数据验证和质量检查
4. PE/PB异常值过滤
"""
import pandas as pd
import polars as pl
from datetime import datetime
import time
import asyncio
from typing import List, Dict, Optional, Tuple
from multiprocessing import Pool, cpu_count
from dataclasses import dataclass

# 微服务内部导入
from .unified_fetcher import UnifiedFetcher, get_unified_fetcher
from core.logger import setup_logger

logger = setup_logger("fundamental_fetcher", log_file="system/fundamental_fetcher.log")


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
    logger.info(f"获取 {len(codes)} 只股票的估值数据...")
    
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
                logger.info(f"进度: {i}/{len(codes)}, 成功: {len(valuation_data)}")
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.warning(f"获取 {code} 估值数据失败: {e}")
            failed_codes.append(code)
            continue
    
    if failed_codes:
        logger.warning(f"失败: {len(failed_codes)} 只股票")
    
    if valuation_data:
        df = pd.DataFrame(valuation_data)
        logger.info(f"估值数据获取完成: {len(df)} 条记录")
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
    logger.info(f"并行获取 {len(codes)} 只股票的估值数据，使用 {config.max_workers} 个进程")
    
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
        logger.info(f"估值数据获取完成: {len(df)} 条记录")
        return df
    
    return pd.DataFrame()


def fetch_industry_data() -> pd.DataFrame:
    """
    获取行业数据
    
    Returns:
        行业数据DataFrame
    """
    logger.info("获取行业数据...")
    
    try:
        # 通过微服务获取股票列表（包含行业信息）
        async def fetch_stock_list():
            fetcher = await get_unified_fetcher()
            return await fetcher.fetch_stock_list()
        
        df_stocks = run_async(fetch_stock_list())
        
        if not df_stocks.empty and 'industry' in df_stocks.columns:
            df_industry = df_stocks[['code', 'industry']].copy()
            df_industry['industry'] = df_industry['industry'].fillna('未知')
            logger.info(f"行业数据获取完成: {len(df_industry)} 条记录")
            return df_industry
        
        logger.warning("无法获取行业数据")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"获取行业数据失败: {e}")
        return pd.DataFrame()


def fetch_fundamental_for_stock(code: str) -> Optional[Dict]:
    """
    获取单只股票的基本面数据
    
    Args:
        code: 股票代码
    Returns:
        基本面数据字典或None
    """
    return run_async(fetch_fundamental_via_service(code))


# 向后兼容的别名
fetch_fundamental_batch_microservice = fetch_fundamental_batch
fetch_fundamental_parallel_microservice = fetch_valuation_data_parallel
