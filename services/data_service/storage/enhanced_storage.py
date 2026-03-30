"""
增强型存储模块 - 使用 Parquet + Polars + DuckDB + Redis
支持高效数据分析、查询和缓存
"""

import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import json
import logging

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

from core.config import get_settings
from core.logger import setup_logger

logger = setup_logger("enhanced_storage", log_file="system/storage.log")


class EnhancedStorage:
    """增强型存储引擎
    
    存储架构:
    - Parquet: 持久化存储，列式存储高效压缩
    - Polars: 内存DataFrame操作，比pandas快10-100倍
    - DuckDB: 分析型SQL查询，支持直接查询Parquet
    - Redis: 热数据缓存，毫秒级响应
    """
    
    def __init__(
        self,
        data_dir: str = None,
        redis_host: str = None,
        redis_port: int = None,
        redis_db: int = None,
        redis_password: str = None
    ):
        settings = get_settings()
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Redis连接 - 优先使用参数，否则从配置读取
        redis_host = redis_host or settings.REDIS_HOST
        redis_port = redis_port or settings.REDIS_PORT
        redis_db = redis_db if redis_db is not None else settings.REDIS_DB
        redis_password = redis_password or settings.REDIS_PASSWORD
        
        self.redis_client = None
        if HAS_REDIS:
            try:
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=redis_db,
                    password=redis_password,
                    decode_responses=True,
                    socket_connect_timeout=5
                )
                self.redis_client.ping()
                logger.info(f"Redis连接成功: {redis_host}:{redis_port}")
            except Exception as e:
                logger.warning(f"Redis连接失败，将仅使用本地存储: {e}")
                self.redis_client = None
        
        # DuckDB连接
        self.duck_conn = None
        if HAS_DUCKDB:
            try:
                # 使用内存数据库，可查询Parquet文件
                self.duck_conn = duckdb.connect(":memory:")
                logger.info("DuckDB初始化成功")
            except Exception as e:
                logger.warning(f"DuckDB初始化失败: {e}")
        
        logger.info(f"EnhancedStorage初始化完成 - Polars: {HAS_POLARS}, DuckDB: {HAS_DUCKDB}, Redis: {self.redis_client is not None}")
    
    # ==================== Parquet 操作 ====================
    
    def save_parquet(
        self,
        df: Any,
        relative_path: str,
        compression: str = "zstd"
    ) -> bool:
        """保存数据到Parquet文件
        
        Args:
            df: DataFrame (支持pandas/polars)
            relative_path: 相对路径
            compression: 压缩算法 (zstd, snappy, gzip)
        """
        try:
            file_path = self.data_dir / relative_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 统一转换为polars处理
            if HAS_POLARS:
                if hasattr(df, 'to_pandas'):  # polars
                    pl_df = df
                else:  # pandas
                    pl_df = pl.from_pandas(df)
                pl_df.write_parquet(str(file_path), compression=compression)
            else:
                import pandas as pd
                if hasattr(df, 'to_pandas'):
                    df = df.to_pandas()
                df.to_parquet(file_path, engine='pyarrow', compression=compression, index=False)
            
            file_size = file_path.stat().st_size / 1024 / 1024
            logger.info(f"保存Parquet成功: {file_path}, 大小: {file_size:.2f}MB")
            return True
            
        except Exception as e:
            logger.error(f"保存Parquet失败: {relative_path}, {e}")
            return False
    
    def read_parquet(self, relative_path: str) -> Optional[Any]:
        """读取Parquet文件，返回Polars DataFrame"""
        try:
            file_path = self.data_dir / relative_path
            
            if not file_path.exists():
                logger.warning(f"文件不存在: {file_path}")
                return None
            
            if HAS_POLARS:
                df = pl.read_parquet(str(file_path))
            else:
                import pandas as pd
                df = pd.read_parquet(file_path)
            
            logger.info(f"读取Parquet成功: {file_path}, 行数: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"读取Parquet失败: {relative_path}, {e}")
            return None
    
    def list_parquet_files(self, subdir: str = "") -> List[Path]:
        """列出目录下的所有Parquet文件"""
        dir_path = self.data_dir / subdir
        if not dir_path.exists():
            return []
        return sorted(dir_path.glob("**/*.parquet"))
    
    # ==================== DuckDB SQL查询 ====================
    
    def query_sql(self, sql: str) -> Optional[Any]:
        """使用DuckDB执行SQL查询
        
        示例:
            SELECT * FROM 'data/enhanced_scores_*.parquet' 
            WHERE grade = 'S' 
            ORDER BY enhanced_score DESC
        """
        if not HAS_DUCKDB or not self.duck_conn:
            logger.error("DuckDB不可用")
            return None
        
        try:
            result = self.duck_conn.execute(sql).pl()
            logger.info(f"SQL查询成功，返回 {len(result)} 行")
            return result
        except Exception as e:
            logger.error(f"SQL查询失败: {e}")
            return None
    
    def query_parquet(
        self,
        file_pattern: str,
        where: str = None,
        order_by: str = None,
        limit: int = None
    ) -> Optional[Any]:
        """查询Parquet文件
        
        Args:
            file_pattern: 文件模式，如 'enhanced_scores_*.parquet'
            where: WHERE条件
            order_by: 排序字段
            limit: 返回行数
        """
        if not HAS_DUCKDB:
            logger.error("DuckDB不可用")
            return None
        
        # 构建SQL
        sql = f"SELECT * FROM '{self.data_dir}/{file_pattern}'"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.query_sql(sql)
    
    # ==================== Redis 缓存操作 ====================
    
    def cache_set(
        self,
        key: str,
        value: Any,
        ttl: int = 3600,
        prefix: str = "xcnstock:"
    ) -> bool:
        """设置缓存
        
        Args:
            key: 缓存键
            value: 缓存值 (自动JSON序列化)
            ttl: 过期时间(秒)，默认1小时
            prefix: 键前缀
        """
        if not self.redis_client:
            return False
        
        try:
            full_key = f"{prefix}{key}"
            
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False, default=str)
            elif not isinstance(value, str):
                value = str(value)
            
            self.redis_client.setex(full_key, ttl, value)
            logger.debug(f"缓存设置成功: {full_key}, TTL: {ttl}s")
            return True
            
        except Exception as e:
            logger.error(f"缓存设置失败: {key}, {e}")
            return False
    
    def cache_get(self, key: str, prefix: str = "xcnstock:") -> Optional[Any]:
        """获取缓存"""
        if not self.redis_client:
            return None
        
        try:
            full_key = f"{prefix}{key}"
            value = self.redis_client.get(full_key)
            
            if value:
                try:
                    return json.loads(value)
                except:
                    return value
            return None
            
        except Exception as e:
            logger.error(f"缓存获取失败: {key}, {e}")
            return None
    
    def cache_delete(self, key: str, prefix: str = "xcnstock:") -> bool:
        """删除缓存"""
        if not self.redis_client:
            return False
        
        try:
            full_key = f"{prefix}{key}"
            self.redis_client.delete(full_key)
            return True
        except Exception as e:
            logger.error(f"缓存删除失败: {key}, {e}")
            return False
    
    def cache_get_or_set(
        self,
        key: str,
        fetch_func,
        ttl: int = 3600,
        prefix: str = "xcnstock:"
    ) -> Any:
        """获取缓存，不存在则执行fetch_func并缓存结果"""
        cached = self.cache_get(key, prefix)
        if cached is not None:
            return cached
        
        # 执行获取函数
        result = fetch_func()
        
        if result is not None:
            self.cache_set(key, result, ttl, prefix)
        
        return result
    
    # ==================== 高级分析功能 ====================
    
    def get_top_stocks(
        self,
        grade: str = None,
        limit: int = 20,
        use_cache: bool = True
    ) -> Optional[Any]:
        """获取评分最高的股票
        
        Args:
            grade: 等级筛选 (S, A, B, C)
            limit: 返回数量
            use_cache: 是否使用缓存
        """
        cache_key = f"top_stocks:{grade}:{limit}"
        
        if use_cache:
            cached = self.cache_get(cache_key)
            if cached:
                if HAS_POLARS:
                    return pl.from_dicts(cached)
                return cached
        
        # 从Parquet查询
        where = f"grade = '{grade}'" if grade else None
        result = self.query_parquet(
            "enhanced_scores_full.parquet",
            where=where,
            order_by="enhanced_score DESC",
            limit=limit
        )
        
        if result is not None and use_cache:
            # 缓存结果
            if HAS_POLARS:
                self.cache_set(cache_key, result.to_dicts(), ttl=300)
            else:
                self.cache_set(cache_key, result, ttl=300)
        
        return result
    
    def get_statistics(self, use_cache: bool = True) -> Dict[str, Any]:
        """获取统计数据"""
        cache_key = "statistics:all"
        
        if use_cache:
            cached = self.cache_get(cache_key)
            if cached:
                return cached
        
        # 使用DuckDB统计
        stats_sql = f"""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN grade = 'S' THEN 1 END) as s_count,
            COUNT(CASE WHEN grade = 'A' THEN 1 END) as a_count,
            COUNT(CASE WHEN grade = 'B' THEN 1 END) as b_count,
            COUNT(CASE WHEN grade = 'C' THEN 1 END) as c_count,
            AVG(enhanced_score) as avg_score,
            MAX(enhanced_score) as max_score,
            AVG(rsi) as avg_rsi,
            AVG(momentum_10d) as avg_momentum
        FROM '{self.data_dir}/enhanced_scores_full.parquet'
        """
        
        result = self.query_sql(stats_sql)
        
        if result is not None:
            if HAS_POLARS:
                stats = result.to_dicts()[0]
            else:
                stats = result.iloc[0].to_dict()
            
            if use_cache:
                self.cache_set(cache_key, stats, ttl=60)
            
            return stats
        
        return {}
    
    def get_stocks_by_trend(
        self,
        trend_type: str = "多头排列",
        limit: int = 50
    ) -> Optional[Any]:
        """按趋势类型筛选股票"""
        # 使用参数化查询避免SQL注入
        if HAS_DUCKDB and self.duck_conn:
            try:
                # 准备参数
                pattern = f"%{trend_type}%"
                # 执行参数化查询
                result = self.duck_conn.execute(
                    """
                    SELECT * FROM ?
                    WHERE reasons LIKE ?
                    ORDER BY enhanced_score DESC
                    LIMIT ?
                    """,
                    [str(self.data_dir / "enhanced_scores_full.parquet"), pattern, limit]
                ).pl()
                logger.info(f"按趋势筛选成功，返回 {len(result)} 行")
                return result
            except Exception as e:
                logger.error(f"按趋势筛选失败: {e}")
                return None
        return None
    
    def export_results(
        self,
        output_path: str = None,
        format: str = "parquet"
    ) -> bool:
        """导出分析结果
        
        Args:
            output_path: 输出路径
            format: 格式 (parquet, csv, json)
        """
        if not output_path:
            output_path = f"results/export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            df = self.read_parquet("enhanced_scores_full.parquet")
            if df is None:
                logger.error("没有数据可导出")
                return False
            
            output_file = Path(self.data_dir) / output_path
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            if format == "parquet":
                if HAS_POLARS:
                    df.write_parquet(str(output_file.with_suffix('.parquet')))
                else:
                    df.to_parquet(output_file.with_suffix('.parquet'), index=False)
            elif format == "csv":
                if HAS_POLARS:
                    df.write_csv(str(output_file.with_suffix('.csv')))
                else:
                    df.to_csv(output_file.with_suffix('.csv'), index=False)
            elif format == "json":
                if HAS_POLARS:
                    df.write_json(str(output_file.with_suffix('.json')))
                else:
                    df.to_json(output_file.with_suffix('.json'), orient='records', force_ascii=False)
            
            logger.info(f"导出成功: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"导出失败: {e}")
            return False
    
    def close(self):
        """关闭所有连接"""
        if self.duck_conn:
            self.duck_conn.close()
        if self.redis_client:
            self.redis_client.close()
        logger.info("存储引擎已关闭")


# 单例模式
_storage_instance: Optional[EnhancedStorage] = None

def get_storage() -> EnhancedStorage:
    """获取存储引擎单例"""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = EnhancedStorage()
    return _storage_instance
