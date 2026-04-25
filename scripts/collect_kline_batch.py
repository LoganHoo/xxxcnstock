#!/usr/bin/env python3
"""
分批采集K线数据 - 优化版本

特性：
1. 降低请求频率（0.5秒间隔）
2. 优先使用Baostock数据源
3. 分批采集，批次间暂停
4. 单进程模式避免连接池耗尽
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple
import logging

from core.paths import DATA_DIR
from core.logger import setup_logger
from core.delisting_guard import get_delisting_guard
from services.data_service.fetchers.kline_fetcher import (
    get_incremental_date_range, 
    validate_kline_data,
    save_with_verification,
    fetch_kline_via_service,
    config
)
from services.data_service.fetchers.unified_fetcher import get_unified_fetcher

logger = setup_logger("kline_batch", log_file="system/kline_batch.log")


async def fetch_single_stock(code: str, days: int, kline_path: Path) -> Tuple[str, bool, int, str]:
    """
    获取单只股票的K线数据
    
    Returns:
        (code, success, rows, status)
    """
    try:
        # 获取增量日期范围
        start_date, end_date, is_incremental = get_incremental_date_range(
            code, days, kline_path
        )
        
        # 如果已经是最新数据，跳过
        if start_date > end_date:
            return (code, True, 0, 'skipped')
        
        # 通过微服务获取K线数据
        df_new = await fetch_kline_via_service(code, start_date, end_date)
        
        if df_new is None or df_new.empty:
            return (code, False, 0, 'no_data')
        
        # 标准化列名
        column_mapping = {
            'date': 'trade_date',
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'volume': 'volume',
            'amount': 'amount'
        }
        df_new = df_new.rename(columns=column_mapping)
        
        # 确保有trade_date列
        if 'trade_date' not in df_new.columns and 'date' in df_new.columns:
            df_new = df_new.rename(columns={'date': 'trade_date'})
        
        # 验证数据完整性
        is_valid, msg = validate_kline_data(df_new, code)
        if not is_valid:
            return (code, False, 0, f'validation_failed: {msg}')
        
        # 合并现有数据（增量模式）
        import polars as pl
        output_file = kline_path / f"{code}.parquet"
        if is_incremental and output_file.exists():
            try:
                df_existing = pl.read_parquet(output_file).to_pandas()
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=['trade_date'], keep='last')
                df_combined = df_combined.sort_values('trade_date')
                df_new = df_combined
            except Exception as e:
                logger.warning(f"合并{code}数据失败，使用新数据: {e}")
        
        # 保存并验证
        if save_with_verification(df_new, output_file):
            return (code, True, len(df_new), 'success')
        else:
            return (code, False, 0, 'save_failed')
            
    except Exception as e:
        logger.exception(f"处理{code}时错误")
        return (code, False, 0, f'error: {str(e)}')


async def fetch_batch(codes: List[str], days: int, kline_path: Path, 
                      request_delay: float = 0.5) -> List[Tuple]:
    """
    采集一批股票数据
    
    Args:
        codes: 股票代码列表
        days: 获取天数
        kline_path: 数据保存路径
        request_delay: 请求间隔（秒）
    
    Returns:
        结果列表
    """
    results = []
    
    # 初始化获取器
    await get_unified_fetcher()
    
    for i, code in enumerate(codes):
        result = await fetch_single_stock(code, days, kline_path)
        results.append(result)
        
        # 请求间隔
        if i < len(codes) - 1:
            time.sleep(request_delay)
        
        # 每10只记录进度
        if (i + 1) % 10 == 0:
            success_count = sum(1 for r in results if r[1] and r[3] != 'skipped')
            logger.info(f"批次进度: {i + 1}/{len(codes)}, 成功 {success_count}")
    
    return results


def fetch_kline_data_batched(
    codes: List[str], 
    kline_dir: Path, 
    days: int = 300,
    batch_size: int = 100,
    batch_pause: float = 5.0,
    filter_delisted: bool = True
) -> Dict:
    """
    分批获取K线历史行情数据
    
    Args:
        codes: 股票代码列表
        kline_dir: K线数据保存目录
        days: 获取天数
        batch_size: 每批股票数量
        batch_pause: 批次间暂停时间（秒）
        filter_delisted: 是否过滤退市股票
    
    Returns:
        统计结果字典
    """
    import asyncio
    
    # 过滤退市股票
    if filter_delisted:
        delisting_guard = get_delisting_guard()
        original_count = len(codes)
        codes = [code for code in codes if not delisting_guard.is_delisted_by_code(code)]
        filtered_count = original_count - len(codes)
        if filtered_count > 0:
            logger.info(f"已过滤 {filtered_count} 只退市股票，剩余 {len(codes)} 只")
    
    logger.info(f"开始分批采集K线数据 (最近{days}天, 共{len(codes)}只股票)")
    logger.info(f"批次大小: {batch_size}, 批次间隔: {batch_pause}秒")
    
    kline_dir.mkdir(exist_ok=True)
    kline_path = Path(kline_dir)
    
    # 分批处理
    total_batches = (len(codes) + batch_size - 1) // batch_size
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_rows = 0
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(codes))
        batch_codes = codes[start_idx:end_idx]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"处理第 {batch_idx + 1}/{total_batches} 批 ({len(batch_codes)} 只股票)")
        logger.info(f"{'='*60}")
        
        # 异步采集本批次
        batch_start = time.time()
        results = asyncio.run(fetch_batch(batch_codes, days, kline_path))
        batch_duration = time.time() - batch_start
        
        # 统计结果
        batch_success = sum(1 for r in results if r[1] and r[3] != 'skipped')
        batch_skipped = sum(1 for r in results if r[3] == 'skipped')
        batch_failed = len(results) - batch_success - batch_skipped
        batch_rows = sum(r[2] for r in results)
        
        success_count += batch_success
        skipped_count += batch_skipped
        failed_count += batch_failed
        total_rows += batch_rows
        
        logger.info(f"批次完成: 成功 {batch_success}, 跳过 {batch_skipped}, "
                   f"失败 {batch_failed}, 新增 {batch_rows} 行, 耗时 {batch_duration:.1f}秒")
        
        # 批次间暂停（除了最后一批）
        if batch_idx < total_batches - 1:
            logger.info(f"暂停 {batch_pause} 秒后继续...")
            time.sleep(batch_pause)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"采集完成!")
    logger.info(f"总计: 成功 {success_count}, 跳过 {skipped_count}, 失败 {failed_count}")
    logger.info(f"总行数: {total_rows}")
    logger.info(f"{'='*60}")
    
    return {
        'success_count': success_count,
        'skipped_count': skipped_count,
        'failed_count': failed_count,
        'total_rows': total_rows,
        'total_codes': len(codes)
    }


def main():
    """主函数"""
    print("=" * 70)
    print("K线数据分批采集 - 优化版本")
    print("=" * 70)
    
    # 计算300天前的日期
    end_date = datetime.now()
    start_date = end_date - timedelta(days=300)
    
    print(f"\n时间范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    print(f"天数: 300天")
    print(f"请求间隔: {config.request_delay}秒")
    print(f"批次大小: {config.stocks_per_batch}只")
    print(f"批次间隔: {config.batch_pause_seconds}秒")
    
    # 获取股票列表
    print("\n加载股票列表...")
    stock_list = pd.read_parquet(DATA_DIR / 'stock_list.parquet')
    codes = stock_list['code'].tolist()
    print(f"股票数量: {len(codes)} 只")
    
    # 分批采集
    print("\n开始分批采集...")
    print("-" * 70)
    
    result = fetch_kline_data_batched(
        codes=codes,
        kline_dir=DATA_DIR / 'kline',
        days=300,
        batch_size=config.stocks_per_batch,
        batch_pause=config.batch_pause_seconds,
        filter_delisted=True
    )
    
    # 验证结果
    print("\n验证数据...")
    kline_dir = DATA_DIR / 'kline'
    kline_files = list(kline_dir.glob('*.parquet'))
    print(f"K线数据文件总数: {len(kline_files)} 个")
    
    if kline_files:
        sample_file = kline_files[0]
        df = pd.read_parquet(sample_file)
        print(f"\n示例文件: {sample_file.name}")
        print(f"  数据行数: {len(df)}")
        if 'trade_date' in df.columns:
            print(f"  日期范围: {df['trade_date'].min()} 至 {df['trade_date'].max()}")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
