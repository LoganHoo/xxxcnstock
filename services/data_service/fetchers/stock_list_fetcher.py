#!/usr/bin/env python3
"""
股票列表获取器 - 微服务内部使用

功能:
1. 通过UnifiedFetcher获取股票列表
2. 统一数据格式和错误处理
3. 保存到本地parquet文件
"""
import pandas as pd
import polars as pl
from datetime import datetime
import asyncio
from typing import List, Dict

# 微服务内部导入
from .unified_fetcher import UnifiedFetcher, get_unified_fetcher
from core.logger import setup_logger

logger = setup_logger("stock_list_fetcher", log_file="system/stock_list_fetcher.log")


def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


async def fetch_stock_list_via_service() -> pd.DataFrame:
    """通过微服务获取股票列表"""
    try:
        fetcher = await get_unified_fetcher()
        df = await fetcher.fetch_stock_list()
        return df
    except Exception as e:
        logger.error(f"微服务获取股票列表失败: {e}")
        raise


def fetch_stock_list() -> List[Dict]:
    """
    获取股票列表
    
    Returns:
        股票列表字典列表
    """
    logger.info("从微服务获取股票列表...")
    
    try:
        df = run_async(fetch_stock_list_via_service())
        
        if df.empty:
            logger.error("获取股票列表失败: 数据为空")
            return []
        
        # 转换为标准格式
        stock_list = []
        for _, row in df.iterrows():
            stock_list.append({
                'code': str(row.get('code', '')),
                'name': str(row.get('name', '')),
                'industry': str(row.get('industry', '')),
                'exchange': str(row.get('exchange', '')),
            })
        
        logger.info(f"获取到 {len(stock_list)} 只股票")
        return stock_list
        
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return []


def save_stock_list_to_parquet(stock_list: List[Dict], output_file) -> bool:
    """
    保存股票列表到parquet文件
    
    Args:
        stock_list: 股票列表数据
        output_file: 输出文件路径
    Returns:
        是否成功
    """
    if not stock_list:
        logger.warning("没有数据需要保存")
        return False

    try:
        df = pd.DataFrame(stock_list)
        pl.from_pandas(df).write_parquet(output_file)
        logger.info(f"股票列表已保存: {output_file}, 共 {len(stock_list)} 只")
        return True
    except Exception as e:
        logger.error(f"保存股票列表失败: {e}")
        return False


def get_exchange_statistics(stock_list: List[Dict]) -> Dict[str, int]:
    """
    获取交易所统计信息
    
    Args:
        stock_list: 股票列表
    Returns:
        交易所统计字典
    """
    exchanges = {}
    for stock in stock_list:
        exchange = stock.get('exchange', 'unknown')
        exchanges[exchange] = exchanges.get(exchange, 0) + 1
    return exchanges


# 向后兼容的别名
fetch_stock_list_via_service = fetch_stock_list
