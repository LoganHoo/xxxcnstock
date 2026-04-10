"""
因子计算器
负责计算策略所需的因子
"""
import polars as pl
from typing import Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)


def calculate_factors(data: pl.DataFrame, config: dict) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    计算因子
    
    Args:
        data: K线数据
        config: 配置
    
    Returns:
        (包含因子的数据, 元数据)
    """
    logger.info("[TRANSFORM] 开始计算因子")
    
    meta = {
        'factors_calculated': [],
        'records_before': len(data),
        'records_after': 0
    }
    
    try:
        result = data.clone()
        
        # 计算MA5
        if 'close' in result.columns:
            result = result.with_columns([
                pl.col("close").rolling_mean(window_size=5).alias("factor_ma5")
            ])
            meta['factors_calculated'].append('ma5')
        
        # 计算MA20
        if 'close' in result.columns:
            result = result.with_columns([
                pl.col("close").rolling_mean(window_size=20).alias("factor_ma20")
            ])
            meta['factors_calculated'].append('ma20')
        
        # 计算MA5偏差（相对于MA20）
        if 'factor_ma5' in result.columns and 'factor_ma20' in result.columns:
            result = result.with_columns([
                ((pl.col("factor_ma5") - pl.col("factor_ma20")) / pl.col("factor_ma20") * 100).alias("factor_ma5_bias")
            ])
            meta['factors_calculated'].append('ma5_bias')
        
        # 计算成交量因子
        if 'volume' in result.columns:
            result = result.with_columns([
                pl.col("volume").rolling_mean(window_size=5).alias("factor_vol_ma5")
            ])
            meta['factors_calculated'].append('vol_ma5')
        
        # 计算价格动量
        if 'close' in result.columns:
            result = result.with_columns([
                (pl.col("close") / pl.col("close").shift(20) - 1).alias("factor_momentum_20d")
            ])
            meta['factors_calculated'].append('momentum_20d')
        
        meta['records_after'] = len(result)
        
        logger.info(f"[TRANSFORM] ✅ 因子计算完成: {meta['factors_calculated']}")
        
        return result, meta
        
    except Exception as e:
        logger.error(f"[TRANSFORM] ❌ 因子计算失败: {e}")
        meta['error'] = str(e)
        return data, meta
