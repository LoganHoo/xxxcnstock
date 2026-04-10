"""
数据验证器
负责验证数据的完整性和质量
"""
import polars as pl
from typing import Tuple, Dict, Any
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def validate_data(data: pl.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """
    验证数据质量
    
    Args:
        data: 数据DataFrame
    
    Returns:
        (是否有效, 元数据字典)
    """
    logger.info("[VALIDATE] 开始验证数据")
    
    meta = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    if len(data) == 0:
        meta['valid'] = False
        meta['errors'].append("数据为空")
        logger.error("[VALIDATE] ❌ 数据为空")
        return False, meta
    
    required_columns = {"code", "trade_date", "open", "close", "high", "low", "volume"}
    missing_columns = required_columns - set(data.columns)
    
    if missing_columns:
        meta['valid'] = False
        meta['errors'].append(f"缺少必要字段: {missing_columns}")
        logger.error(f"[VALIDATE] ❌ 缺少必要字段: {missing_columns}")
        return False, meta
    
    # 检查重复
    duplicate_count = data.select(pl.col("code", "trade_date")).distinct().height - len(data)
    
    if duplicate_count > 0:
        meta['warnings'].append(f"存在 {duplicate_count} 条重复记录")
        logger.warning(f"[VALIDATE] ⚠️ 存在 {duplicate_count} 条重复记录")
    
    # 检查价格合理性
    price_check = data.filter(
        (pl.col("high") < pl.col("low")) |
        (pl.col("high") < pl.col("open")) |
        (pl.col("high") < pl.col("close")) |
        (pl.col("low") > pl.col("open")) |
        (pl.col("low") > pl.col("close"))
    )
    
    if len(price_check) > 0:
        meta['warnings'].append(f"存在 {len(price_check)} 条价格异常记录")
        logger.warning(f"[VALIDATE] ⚠️ 存在 {len(price_check)} 条价格异常记录")
    
    # 检查日期连续性
    if 'trade_date' in data.columns:
        dates = data.select(pl.col("trade_date")).unique().sort("trade_date")
        
        if len(dates) > 1:
            date_diffs = dates.select(
                pl.col("trade_date").diff().drop_null()
            )
            
            max_gap = date_diffs.max().item() if not date_diffs.is_empty() else None
            
            meta['stats']['date_gap'] = str(max_gap) if max_gap else None
    
    # 检查数据新鲜度
    if 'trade_date' in data.columns:
        latest_date = data['trade_date'].max()
        latest_date_obj = datetime.strptime(latest_date, "%Y-%m-%d") if isinstance(latest_date, str) else latest_date
        days_since_latest = (datetime.now() - latest_date_obj).days
        
        meta['stats']['days_since_latest'] = days_since_latest
        
        if days_since_latest > 30:
            meta['warnings'].append(f"最新数据已过期 {days_since_latest} 天")
            logger.warning(f"[VALIDATE] ⚠️ 最新数据已过期 {days_since_latest} 天")
    
    if meta['valid']:
        logger.info(f"[VALIDATE] ✅ 数据验证通过: {len(data)} 条记录")
    
    return meta['valid'], meta
