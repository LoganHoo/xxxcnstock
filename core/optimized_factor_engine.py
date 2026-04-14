"""
优化版因子计算引擎
支持：延迟计算、因子缓存、增量计算
"""
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import polars as pl
import numpy as np
from core.logger import get_logger
from core.factor_library import FactorRegistry

# 导入因子模块以触发注册
import factors  # noqa: F401

logger = get_logger(__name__)


class OptimizedFactorEngine:
    """优化版因子计算引擎"""
    
    def __init__(self, cache_dir: str = None):
        self.project_root = Path(__file__).parent.parent
        self.cache_dir = Path(cache_dir) if cache_dir else self.project_root / "data" / "cache" / "factors"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 因子缓存索引
        self.factor_cache_index_file = self.cache_dir / "factor_cache_index.json"
        self.factor_cache_index = self._load_factor_cache_index()
    
    def _load_factor_cache_index(self) -> dict:
        """加载因子缓存索引"""
        if self.factor_cache_index_file.exists():
            try:
                with open(self.factor_cache_index_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载因子缓存索引失败: {e}")
        return {}
    
    def _save_factor_cache_index(self):
        """保存因子缓存索引"""
        try:
            with open(self.factor_cache_index_file, 'w', encoding='utf-8') as f:
                json.dump(self.factor_cache_index, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存因子缓存索引失败: {e}")
    
    def _get_factor_cache_key(self, factor_name: str, data_hash: str) -> str:
        """生成因子缓存键"""
        return hashlib.md5(f"{factor_name}_{data_hash}".encode()).hexdigest()
    
    def _get_factor_cache_path(self, cache_key: str) -> Path:
        """获取因子缓存路径"""
        return self.cache_dir / f"factor_{cache_key}.parquet"
    
    def _compute_data_hash(self, data: pl.DataFrame, date_range: str) -> str:
        """计算数据哈希（用于缓存验证）"""
        # 使用数据的元信息生成哈希
        hash_input = f"{date_range}_{len(data)}_{data['code'].n_unique()}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]
    
    def calculate_factors_lazy(
        self, 
        data: pl.DataFrame, 
        factor_names: List[str],
        use_cache: bool = True
    ) -> pl.DataFrame:
        """
        延迟计算因子（优化版）
        
        Args:
            data: K线数据
            factor_names: 需要计算的因子列表
            use_cache: 是否使用缓存
            
        Returns:
            包含因子的DataFrame
        """
        date_range = f"{data['trade_date'].min()}_{data['trade_date'].max()}"
        data_hash = self._compute_data_hash(data, date_range)
        
        logger.info(f"[FACTOR] 计算 {len(factor_names)} 个因子")
        
        # 分离需要计算的因子和可从缓存加载的因子
        factors_to_compute = []
        cached_factors = []
        
        for factor_name in factor_names:
            cache_key = self._get_factor_cache_key(factor_name, data_hash)
            cache_path = self._get_factor_cache_path(cache_key)
            
            if use_cache and cache_path.exists():
                cache_info = self.factor_cache_index.get(cache_key, {})
                if cache_info.get('data_hash') == data_hash:
                    cached_factors.append((factor_name, cache_path))
                    continue
            
            factors_to_compute.append(factor_name)
        
        # 从缓存加载已有因子
        result = data.clone()
        for factor_name, cache_path in cached_factors:
            logger.debug(f"[FACTOR] 从缓存加载: {factor_name}")
            factor_df = pl.read_parquet(cache_path)
            # P1优化：确保join键类型一致（处理Categorical类型）
            if result['code'].dtype != factor_df['code'].dtype:
                factor_df = factor_df.with_columns([
                    pl.col('code').cast(result['code'].dtype)
                ])
            result = result.join(factor_df, on=["code", "trade_date"], how="left")
        
        logger.info(f"[FACTOR] 缓存命中: {len(cached_factors)}/{len(factor_names)}")
        
        # 计算新因子
        if factors_to_compute:
            result = self._compute_factors(result, factors_to_compute, data_hash)
        
        return result
    
    def _compute_factors(
        self, 
        data: pl.DataFrame, 
        factor_names: List[str],
        data_hash: str
    ) -> pl.DataFrame:
        """计算因子并缓存（P0优化：批量计算）"""
        result = data.clone()
        
        # P0优化：批量计算相关因子，减少中间DataFrame创建
        # 1. 批量计算MA系列因子
        ma_factors = [f for f in factor_names if f in ['ma5', 'ma10', 'ma20']]
        if ma_factors:
            ma_exprs = []
            for window in [5, 10, 20]:
                factor_name = f"ma{window}"
                if factor_name in ma_factors:
                    ma_exprs.append(
                        pl.col("close").rolling_mean(window_size=window).over("code").alias(f"factor_{factor_name}")
                    )
            # 一次性计算所有MA因子
            result = result.with_columns(ma_exprs)
            logger.debug(f"[FACTOR] 批量计算MA因子: {ma_factors}")
        
        # 2. 计算依赖MA的派生因子
        if "ma5_bias" in factor_names:
            # 确保MA5和MA20已计算
            if "factor_ma5" not in result.columns:
                result = result.with_columns([
                    pl.col("close").rolling_mean(window_size=5).over("code").alias("factor_ma5")
                ])
            if "factor_ma20" not in result.columns:
                result = result.with_columns([
                    pl.col("close").rolling_mean(window_size=20).over("code").alias("factor_ma20")
                ])
            result = result.with_columns([
                ((pl.col("factor_ma5") - pl.col("factor_ma20")) / pl.col("factor_ma20") * 100)
                .alias("factor_ma5_bias")
            ])
            # 缓存ma5_bias
            self._cache_factor(result, "ma5_bias", data_hash)
        
        # 3. 批量计算其他独立因子
        other_factors = [f for f in factor_names if f not in ['ma5', 'ma10', 'ma20', 'ma5_bias']]
        
        for factor_name in other_factors:
            try:
                factor_col = f"factor_{factor_name}"
                
                if factor_name == "v_ratio10":
                    # 10日成交量比 - 优化为单次with_columns
                    result = result.with_columns([
                        pl.col("volume").shift(1).over("code").alias("prev_volume")
                    ])
                    result = result.with_columns([
                        pl.when(pl.col("prev_volume").is_null() | (pl.col("prev_volume") == 0))
                        .then(1.0)
                        .otherwise(pl.col("volume") / pl.col("prev_volume"))
                        .alias(factor_col)
                    ])
                    result = result.drop("prev_volume")  # 清理临时列
                
                elif factor_name == "momentum_20d":
                    result = result.with_columns([
                        (pl.col("close") / pl.col("close").shift(20).over("code") - 1).alias(factor_col)
                    ])
                
                elif factor_name == "volatility_20d":
                    result = result.with_columns([
                        (pl.col("close").rolling_std(window_size=20).over("code") / 
                         pl.col("close").rolling_mean(window_size=20).over("code")).alias(factor_col)
                    ])
                
                elif factor_name == "rsi":
                    result = self._calculate_rsi(result, factor_col)
                
                elif factor_name == "macd":
                    result = self._calculate_macd(result, factor_col)
                
                else:
                    # 尝试从FactorRegistry加载
                    factor_class = FactorRegistry.get(factor_name)
                    if factor_class:
                        factor_instance = factor_class()
                        result = factor_instance.calculate(result)
                    else:
                        logger.warning(f"[FACTOR] 未知因子: {factor_name}")
                        continue
                
                # 缓存计算结果
                self._cache_factor(result, factor_name, data_hash)
                
            except Exception as e:
                logger.error(f"[FACTOR] 计算因子 {factor_name} 失败: {e}")
        
        # 缓存MA因子结果
        for ma_factor in ma_factors:
            self._cache_factor(result, ma_factor, data_hash)
        
        self._save_factor_cache_index()
        return result
    
    def _cache_factor(self, data: pl.DataFrame, factor_name: str, data_hash: str):
        """缓存单个因子结果（提取公共逻辑）
        
        Args:
            data: 包含因子的DataFrame
            factor_name: 因子名称
            data_hash: 数据哈希
        """
        try:
            factor_col = f"factor_{factor_name}"
            if factor_col not in data.columns:
                return
            
            cache_key = self._get_factor_cache_key(factor_name, data_hash)
            cache_path = self._get_factor_cache_path(cache_key)
            
            factor_df = data.select(["code", "trade_date", factor_col])
            factor_df.write_parquet(cache_path)
            
            self.factor_cache_index[cache_key] = {
                'factor_name': factor_name,
                'data_hash': data_hash,
                'created_at': datetime.now().isoformat(),
                'row_count': len(factor_df)
            }
            
            logger.debug(f"[FACTOR] 已缓存: {factor_name}")
        except Exception as e:
            logger.warning(f"[FACTOR] 缓存因子 {factor_name} 失败: {e}")
    
    def _calculate_rsi(self, data: pl.DataFrame, output_col: str, period: int = 14) -> pl.DataFrame:
        """计算RSI指标"""
        delta = pl.col("close").diff().over("code")
        gain = pl.when(delta > 0).then(delta).otherwise(0)
        loss = pl.when(delta < 0).then(-delta).otherwise(0)
        
        avg_gain = gain.rolling_mean(window_size=period).over("code")
        avg_loss = loss.rolling_mean(window_size=period).over("code")
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return data.with_columns([rsi.alias(output_col)])
    
    def _calculate_macd(self, data: pl.DataFrame, output_col: str) -> pl.DataFrame:
        """计算MACD指标"""
        ema12 = pl.col("close").ewm_mean(span=12).over("code")
        ema26 = pl.col("close").ewm_mean(span=26).over("code")
        macd = ema12 - ema26
        
        return data.with_columns([macd.alias(output_col)])
    
    def clear_cache(self, older_than_days: int = 7):
        """清理过期因子缓存"""
        cutoff = datetime.now() - timedelta(days=older_than_days)
        removed = 0
        
        for cache_key, info in list(self.factor_cache_index.items()):
            created = datetime.fromisoformat(info.get('created_at', '2000-01-01'))
            if created < cutoff:
                cache_path = self._get_factor_cache_path(cache_key)
                if cache_path.exists():
                    cache_path.unlink()
                del self.factor_cache_index[cache_key]
                removed += 1
        
        self._save_factor_cache_index()
        logger.info(f"[FACTOR CACHE] 清理了 {removed} 个过期缓存文件")


# 全局单例
factor_engine = OptimizedFactorEngine()


def calculate_factors_optimized(
    data: pl.DataFrame, 
    factor_names: List[str],
    use_cache: bool = True
) -> pl.DataFrame:
    """优化的因子计算函数（便捷接口）"""
    return factor_engine.calculate_factors_lazy(data, factor_names, use_cache)
