"""
数据加载器
负责从Parquet文件加载K线数据
"""
import polars as pl
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def load_data(data_path: str = None) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    加载K线数据
    
    Args:
        data_path: 数据文件路径（Parquet格式）
    
    Returns:
        (数据DataFrame, 元数据字典)
    """
    logger.info("[LOAD] 开始加载数据")
    
    if data_path is None:
        data_dir = Path('data/kline')
        if not data_dir.exists():
            data_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
        
        parquet_files = list(data_dir.glob("*.parquet"))
        
        if not parquet_files:
            raise FileNotFoundError(f"未找到Parquet文件: {data_dir}")
        
        logger.info(f"[LOAD] 发现 {len(parquet_files)} 个Parquet文件")
        
        target_columns = {"code", "trade_date", "open", "close", "high", "low", "volume"}
        
        dfs = []
        loaded_count = 0
        
        for parquet_file in parquet_files:
            try:
                df = pl.read_parquet(parquet_file, columns=list(target_columns))
                df = df.with_columns([
                    pl.col("code").cast(pl.Utf8),
                    pl.col("trade_date").cast(pl.Utf8)
                ])
                dfs.append(df)
                loaded_count += 1
                
                if loaded_count % 500 == 0:
                    logger.info(f"[LOAD] 已加载 {loaded_count} 个文件...")
                    
            except Exception as e:
                logger.warning(f"[LOAD] 跳过文件 {parquet_file.name}: {e}")
        
        if not dfs:
            raise ValueError("没有成功加载任何数据文件")
        
        data = pl.concat(dfs, rechunk=True)
        
        logger.info(f"[LOAD] ✅ 数据加载完成: {len(data)} 条记录")
        
        meta = {
            'total_files': loaded_count,
            'total_records': len(data),
            'date_range': {
                'start': data['trade_date'].min() if 'trade_date' in data.columns else None,
                'end': data['trade_date'].max() if 'trade_date' in data.columns else None
            },
            'unique_stocks': data['code'].n_unique() if 'code' in data.columns else 0
        }
        
        return data, meta
    
    else:
        data_file = Path(data_path)
        if not data_file.exists():
            raise FileNotFoundError(f"数据文件不存在: {data_path}")
        
        data = pl.read_parquet(data_file)
        
        logger.info(f"[LOAD] ✅ 数据加载完成: {len(data)} 条记录")
        
        meta = {
            'total_files': 1,
            'total_records': len(data),
            'date_range': {
                'start': data['trade_date'].min() if 'trade_date' in data.columns else None,
                'end': data['trade_date'].max() if 'trade_date' in data.columns else None
            },
            'unique_stocks': data['code'].n_unique() if 'code' in data.columns else 0
        }
        
        return data, meta
