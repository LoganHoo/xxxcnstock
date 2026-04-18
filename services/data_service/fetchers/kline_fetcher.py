#!/usr/bin/env python3
"""
K线数据获取器 - 微服务内部使用
多进程加速版本

功能:
1. 通过UnifiedFetcher获取K线数据
2. 支持增量更新和断点续传
3. 数据验证和质量检查
4. 多进程并行处理
"""
import sys
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time
import logging
from multiprocessing import Pool, cpu_count
from functools import wraps
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import asyncio

# 微服务内部导入
from .unified_fetcher import UnifiedFetcher, get_unified_fetcher
from core.logger import setup_logger

logger = setup_logger("kline_fetcher", log_file="system/kline_fetcher.log")


@dataclass
class Config:
    """配置类"""
    # 进程配置
    max_workers: int = min(4, cpu_count())
    batch_size: int = 50
    
    # 重试配置
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    
    # 请求频率控制
    request_delay: float = 0.01
    
    # 数据配置
    kline_days: int = 365 * 3
    min_kline_rows: int = 50  # 最少K线数据行数
    
    # 数据质量阈值
    max_pe: float = 1000.0
    max_pb: float = 100.0


config = Config()


def retry_on_error(max_retries: int = None, base_delay: float = None):
    """指数退避重试装饰器"""
    max_retries = max_retries or config.max_retries
    base_delay = base_delay or config.retry_base_delay
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), config.retry_max_delay)
                        logger.warning(f"{func.__name__} 第{attempt + 1}次尝试失败: {e}, {delay}s后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} 所有{max_retries + 1}次尝试均失败")
            raise last_exception
        return wrapper
    return decorator


def validate_kline_data(df: pd.DataFrame, code: str) -> Tuple[bool, str]:
    """验证K线数据完整性"""
    if len(df) < config.min_kline_rows:
        return False, f"数据行数不足: {len(df)} < {config.min_kline_rows}"
    
    required_cols = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"缺少字段: {missing_cols}"
    
    if (df['close'] <= 0).any():
        return False, "存在无效收盘价"
    
    # 检查OHLC逻辑
    invalid_ohlc = (
        (df['high'] < df['low']) |
        (df['high'] < df['open']) |
        (df['high'] < df['close']) |
        (df['low'] > df['open']) |
        (df['low'] > df['close'])
    )
    if invalid_ohlc.any():
        n_invalid = invalid_ohlc.sum()
        logger.warning(f"{code} 存在{n_invalid}条OHLC逻辑异常数据")
    
    return True, "OK"


def save_with_verification(df: pd.DataFrame, output_file: Path) -> bool:
    """保存数据并验证完整性"""
    try:
        pl.from_pandas(df).write_parquet(output_file)
        # 验证
        df_verify = pl.read_parquet(output_file)
        if len(df_verify) != len(df):
            logger.error(f"数据验证失败: 保存{len(df)}行, 读取{len(df_verify)}行")
            return False
        return True
    except Exception as e:
        logger.error(f"保存或验证失败: {e}")
        return False


def get_incremental_date_range(code: str, days: int, data_dir: Path) -> Tuple[str, str, bool]:
    """
    获取增量更新的日期范围
    
    Returns:
        (start_date, end_date, is_incremental)
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    kline_file = data_dir / f"{code}.parquet"
    
    if kline_file.exists():
        try:
            df_existing = pl.read_parquet(kline_file)
            if len(df_existing) > 0:
                last_date = df_existing['trade_date'].max()
                last_date = datetime.strptime(last_date, '%Y-%m-%d')
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                
                if start_date > end_date:
                    return start_date, end_date, True  # 已是最新
                return start_date, end_date, True
        except Exception as e:
            logger.warning(f"读取{code}历史数据失败: {e}")
    
    # 全量更新
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    return start_date, end_date, False


# ==================== 微服务调用函数 ====================

async def fetch_kline_via_service(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """通过微服务获取K线数据"""
    try:
        fetcher = await get_unified_fetcher()
        df = await fetcher.fetch_kline(code, start_date, end_date)
        return df if not df.empty else None
    except Exception as e:
        logger.warning(f"{code} 微服务获取K线失败: {e}")
        return None


def run_async(coro):
    """运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def fetch_kline_batch(args):
    """
    批量获取K线数据
    
    Args:
        args: (codes_batch, kline_dir, days)
    """
    codes_batch, kline_dir, days = args
    kline_path = Path(kline_dir)
    results = []
    
    # 初始化微服务获取器
    run_async(get_unified_fetcher())
    
    for code in codes_batch:
        try:
            # 获取增量日期范围
            start_date, end_date, is_incremental = get_incremental_date_range(
                code, days, kline_path
            )
            
            # 如果已经是最新数据，跳过
            if start_date > end_date:
                results.append((code, True, 0, 'skipped'))
                continue
            
            # 通过微服务获取K线数据
            df_new = run_async(fetch_kline_via_service(code, start_date, end_date))
            
            if df_new is None or df_new.empty:
                results.append((code, False, 0, 'no_data'))
                continue
            
            # 验证数据完整性
            is_valid, msg = validate_kline_data(df_new, code)
            if not is_valid:
                results.append((code, False, 0, f'validation_failed: {msg}'))
                continue
            
            # 合并现有数据（增量模式）
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
                results.append((code, True, len(df_new), 'success'))
            else:
                results.append((code, False, 0, 'save_failed'))
            
            time.sleep(config.request_delay)
            
        except Exception as e:
            logger.exception(f"处理{code}时错误")
            results.append((code, False, 0, f'error: {str(e)}'))
    
    return results


# ==================== 主控函数 ====================

def fetch_kline_data_parallel(codes: List[str], kline_dir: Path, days: int = None) -> Dict:
    """
    并行获取K线历史行情数据
    
    Args:
        codes: 股票代码列表
        kline_dir: K线数据保存目录
        days: 获取天数
    Returns:
        统计结果字典
    """
    days = days or config.kline_days
    
    logger.info(f"并行获取K线数据 (最近{days}天, 进程数: {config.max_workers})")
    
    kline_dir.mkdir(exist_ok=True)
    
    # 将股票代码分批
    batches = [codes[i:i+config.batch_size] for i in range(0, len(codes), config.batch_size)]
    batch_args = [(batch, str(kline_dir), days) for batch in batches]
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_rows = 0
    
    with Pool(processes=config.max_workers) as pool:
        for i, results in enumerate(pool.imap_unordered(fetch_kline_batch, batch_args)):
            for code, success, rows, status in results:
                if success:
                    if status == 'skipped':
                        skipped_count += 1
                    else:
                        success_count += 1
                        total_rows += rows
                else:
                    failed_count += 1
                    logger.warning(f"{code} 失败: {status}")
            
            processed = min((i + 1) * config.batch_size, len(codes))
            if (i + 1) % 10 == 0 or processed == len(codes):
                logger.info(f"进度: {processed}/{len(codes)}, "
                           f"成功 {success_count}, 跳过 {skipped_count}, "
                           f"失败 {failed_count}, 累计 {total_rows} 行")
    
    logger.info(f"K线数据获取完成: {success_count}/{len(codes)} 只成功, "
                f"{skipped_count} 只跳过, {failed_count} 只失败")
    
    return {
        'success': success_count,
        'skipped': skipped_count,
        'failed': failed_count,
        'total_rows': total_rows
    }


def fetch_kline_for_stock(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    获取单只股票的K线数据
    
    Args:
        code: 股票代码
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
    Returns:
        K线数据DataFrame或None
    """
    return run_async(fetch_kline_via_service(code, start_date, end_date))


# 向后兼容的别名
fetch_kline_batch_microservice = fetch_kline_batch
fetch_kline_data_parallel_microservice = fetch_kline_data_parallel
