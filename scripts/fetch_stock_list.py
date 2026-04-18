#!/usr/bin/env python3
"""
从微服务获取所有股票列表并保存到本地parquet文件

改造说明:
1. 从直接调用Baostock改为调用微服务UnifiedFetcher
2. 利用微服务的主备数据源自动切换
3. 统一数据格式和错误处理
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime
import asyncio

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.data_service.fetchers.unified_fetcher import UnifiedFetcher, get_unified_fetcher
from core.logger import setup_logger

logger = setup_logger("fetch_stock_list", log_file="system/fetch_stock_list.log")


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


def save_stock_list(stock_list: list, filename: str = "stock_list.parquet"):
    """
    保存股票列表到parquet文件
    
    参数:
        stock_list: 股票列表数据
        filename: 文件名
    """
    if not stock_list:
        print("没有数据需要保存")
        return

    output_dir = PROJECT_ROOT / "data"
    output_dir.mkdir(exist_ok=True)

    df = pd.DataFrame(stock_list)
    output_file = output_dir / filename
    
    try:
        pl.from_pandas(df).write_parquet(output_file)
        print(f"✅ 股票列表已保存: {output_file}")
        print(f"   共 {len(stock_list)} 只股票")
    except Exception as e:
        logger.error(f"保存股票列表失败: {e}")
        raise


def main():
    """主函数"""
    print("=" * 80)
    print("从微服务获取股票列表")
    print("=" * 80)
    
    try:
        # 通过微服务获取股票列表
        print("\n正在从微服务获取股票列表...")
        df = run_async(fetch_stock_list_via_service())
        
        if df.empty:
            print("❌ 获取股票列表失败: 数据为空")
            return
        
        # 转换为标准格式
        stock_list = []
        for _, row in df.iterrows():
            stock_list.append({
                'code': str(row.get('code', '')),
                'name': str(row.get('name', '')),
                'industry': str(row.get('industry', '')),
                'exchange': str(row.get('exchange', '')),
            })
        
        # 保存
        save_stock_list(stock_list)
        
        # 显示统计
        print(f"\n📊 统计信息:")
        print(f"   总股票数: {len(stock_list)}")
        
        # 按交易所统计
        exchanges = {}
        for stock in stock_list:
            exchange = stock.get('exchange', 'unknown')
            exchanges[exchange] = exchanges.get(exchange, 0) + 1
        
        for exchange, count in exchanges.items():
            print(f"   {exchange}: {count} 只")
        
        print("\n✅ 完成!")
        
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        print(f"\n❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
