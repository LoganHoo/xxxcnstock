"""
Parquet文件操作工具

提供标准化的Parquet文件读写、更新功能
"""
import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Union, List
from datetime import datetime

from core.logger import setup_logger

logger = setup_logger("parquet_utils")


# 标准列名映射
COLUMN_MAPPING = {
    # 日期
    'date': 'trade_date',
    # 价格
    'open': 'open',
    'close': 'close',
    'high': 'high',
    'low': 'low',
    # 成交量和成交额
    'volume': 'volume',
    'amount': 'amount',
    # 昨收 (多种可能的字段名)
    'preclose': 'preclose',
    'pre_close': 'preclose',
    'preClose': 'preclose',
    # 涨跌幅 (多种可能的字段名)
    'pct_chg': 'pct_chg',
    'pctChg': 'pct_chg',
    'change_pct': 'pct_chg',
    'pct_change': 'pct_chg',
    # 换手率 (多种可能的字段名)
    'turn': 'turnover',
    'turnover': 'turnover',
    'turnover_rate': 'turnover',
}

# 标准列顺序
STANDARD_COLUMNS = [
    'code', 'trade_date', 'open', 'high', 'low', 'close',
    'volume', 'amount', 'preclose', 'pct_chg', 'turnover'
]


def normalize_columns(df: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
    """
    标准化DataFrame列名
    
    Args:
        df: 输入DataFrame (polars或pandas)
        
    Returns:
        标准化后的polars DataFrame
    """
    # 转换为polars
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df)
    
    # 重命名列
    rename_map = {}
    for old_col, new_col in COLUMN_MAPPING.items():
        if old_col in df.columns and new_col not in df.columns:
            rename_map[old_col] = new_col
    
    if rename_map:
        df = df.rename(rename_map)
    
    return df


def ensure_code_column(df: pl.DataFrame, code: str) -> pl.DataFrame:
    """
    确保DataFrame包含code列
    
    Args:
        df: 输入DataFrame
        code: 股票代码
        
    Returns:
        包含code列的DataFrame
    """
    if 'code' not in df.columns:
        df = df.with_columns(pl.lit(code).alias('code'))
    return df


def calculate_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    计算派生列 (preclose, pct_chg)
    
    Args:
        df: 输入DataFrame
        
    Returns:
        包含派生列的DataFrame
    """
    # 计算preclose（如果不存在但有pct_chg和close）
    if 'preclose' not in df.columns and 'pct_chg' in df.columns and 'close' in df.columns:
        df = df.with_columns(
            (pl.col('close') / (1 + pl.col('pct_chg') / 100)).alias('preclose')
        )
    
    # 计算pct_chg（如果不存在但有preclose和close）
    if 'pct_chg' not in df.columns and 'preclose' in df.columns and 'close' in df.columns:
        df = df.with_columns(
            ((pl.col('close') - pl.col('preclose')) / pl.col('preclose') * 100).alias('pct_chg')
        )
    
    return df


def reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    按标准顺序排列列
    
    Args:
        df: 输入DataFrame
        
    Returns:
        列按标准顺序排列的DataFrame
    """
    # 只保留存在的列
    existing_cols = [col for col in STANDARD_COLUMNS if col in df.columns]
    # 添加其他列
    other_cols = [col for col in df.columns if col not in STANDARD_COLUMNS]
    
    return df.select(existing_cols + other_cols)


def update_parquet_file(
    code: str,
    new_data: Union[pl.DataFrame, pd.DataFrame, Dict],
    kline_dir: Union[str, Path],
    date_field: str = 'trade_date'
) -> bool:
    """
    更新Parquet文件，支持增量更新
    
    Args:
        code: 股票代码
        new_data: 新数据 (DataFrame或字典)
        kline_dir: K线数据目录
        date_field: 日期字段名
        
    Returns:
        是否成功更新
    """
    kline_dir = Path(kline_dir)
    parquet_file = kline_dir / f"{code}.parquet"
    
    try:
        # 统一转换为polars DataFrame
        if isinstance(new_data, dict):
            new_df = pl.DataFrame([new_data])
        elif isinstance(new_data, pd.DataFrame):
            new_df = pl.from_pandas(new_data)
        else:
            new_df = new_data
        
        # 标准化列名
        new_df = normalize_columns(new_df)
        
        # 确保code列存在
        new_df = ensure_code_column(new_df, code)
        
        # 计算派生列
        new_df = calculate_derived_columns(new_df)
        
        if parquet_file.exists():
            # 读取现有数据
            existing = pl.read_parquet(parquet_file)
            
            # 标准化现有数据列名
            existing = normalize_columns(existing)
            
            # 获取新数据中的日期
            new_dates = set(new_df[date_field].cast(str).to_list())
            
            # 过滤掉已存在的日期
            existing_filtered = existing.filter(
                ~pl.col(date_field).cast(str).is_in(new_dates)
            )
            
            # 合并数据
            merged = pl.concat([existing_filtered, new_df], how='diagonal')
            
            # 去重并排序
            merged = merged.unique(subset=[date_field], keep='last')
            merged = merged.sort(date_field)
            
            # 重新排列列
            merged = reorder_columns(merged)
            
            # 保存
            merged.write_parquet(parquet_file)
        else:
            # 新文件
            new_df = reorder_columns(new_df)
            new_df.write_parquet(parquet_file)
        
        return True
        
    except Exception as e:
        logger.error(f"更新 {code} 的Parquet文件失败: {e}")
        return False


class ParquetManager:
    """
    Parquet文件管理器
    
    提供高级的Parquet文件操作功能
    """
    
    def __init__(self, kline_dir: Union[str, Path]):
        """
        初始化
        
        Args:
            kline_dir: K线数据目录
        """
        self.kline_dir = Path(kline_dir)
        self.kline_dir.mkdir(parents=True, exist_ok=True)
    
    def get_stock_codes(self) -> List[str]:
        """
        获取所有股票代码
        
        Returns:
            股票代码列表
        """
        return [f.stem for f in self.kline_dir.glob("*.parquet")]
    
    def read_stock_data(self, code: str) -> Optional[pl.DataFrame]:
        """
        读取股票数据
        
        Args:
            code: 股票代码
            
        Returns:
            DataFrame或None
        """
        parquet_file = self.kline_dir / f"{code}.parquet"
        if not parquet_file.exists():
            return None
        
        try:
            df = pl.read_parquet(parquet_file)
            return normalize_columns(df)
        except Exception as e:
            logger.error(f"读取 {code} 数据失败: {e}")
            return None
    
    def get_latest_date(self, code: str) -> Optional[str]:
        """
        获取股票最新数据日期
        
        Args:
            code: 股票代码
            
        Returns:
            最新日期字符串或None
        """
        df = self.read_stock_data(code)
        if df is None or df.is_empty():
            return None
        
        return df['trade_date'].cast(str).to_list()[-1]
    
    def update_stock_data(
        self,
        code: str,
        new_data: Union[pl.DataFrame, pd.DataFrame, Dict]
    ) -> bool:
        """
        更新股票数据
        
        Args:
            code: 股票代码
            new_data: 新数据
            
        Returns:
            是否成功
        """
        return update_parquet_file(code, new_data, self.kline_dir)
