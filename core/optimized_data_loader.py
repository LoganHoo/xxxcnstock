"""
优化版数据加载器
支持增量加载、日期分区、智能缓存、并行加载
"""
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional, List
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import polars as pl
from core.logger import get_logger

logger = get_logger(__name__)

# P1优化：使用StringCache优化Categorical类型合并性能
pl.enable_string_cache()


class OptimizedDataLoader:
    """优化版数据加载器"""
    
    def __init__(self, data_dir: str = None, cache_dir: str = None):
        self.project_root = Path(__file__).parent.parent
        self.data_dir = Path(data_dir) if data_dir else self.project_root / "data" / "kline"
        self.cache_dir = Path(cache_dir) if cache_dir else self.project_root / "data" / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存索引文件
        self.cache_index_file = self.cache_dir / "data_cache_index.json"
        self.cache_index = self._load_cache_index()
    
    def _load_cache_index(self) -> dict:
        """加载缓存索引"""
        if self.cache_index_file.exists():
            try:
                with open(self.cache_index_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载缓存索引失败: {e}")
        return {}
    
    def _save_cache_index(self):
        """保存缓存索引"""
        try:
            with open(self.cache_index_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_index, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存缓存索引失败: {e}")
    
    def _get_cache_key(self, start_date: str, end_date: str, columns: tuple) -> str:
        """生成缓存键"""
        key_str = f"{start_date}_{end_date}_{columns}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """获取缓存文件路径"""
        return self.cache_dir / f"data_{cache_key}.parquet"
    
    def load_recent_data(
        self, 
        days: int = 60,
        end_date: str = None,
        columns: List[str] = None,
        use_cache: bool = True
    ) -> Tuple[pl.DataFrame, dict]:
        """
        加载最近N天的数据（增量优化版）
        
        Args:
            days: 加载天数（默认60天，满足MA20+10日量能计算）
            end_date: 结束日期（默认昨天）
            columns: 需要的列
            use_cache: 是否使用缓存
            
        Returns:
            (数据, 元信息)
        """
        if columns is None:
            columns = ["code", "trade_date", "open", "close", "high", "low", "volume"]
        
        # 计算日期范围
        if end_date is None:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=days)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        logger.info(f"[LOAD] 加载数据范围: {start_date} ~ {end_date} ({days}天)")
        
        # 检查缓存
        cache_key = self._get_cache_key(start_date, end_date, tuple(columns))
        cache_path = self._get_cache_path(cache_key)
        
        if use_cache and cache_path.exists():
            cache_info = self.cache_index.get(cache_key, {})
            if cache_info.get('start_date') == start_date and cache_info.get('end_date') == end_date:
                logger.info(f"[LOAD] 从缓存加载: {cache_path}")
                data = pl.read_parquet(cache_path)
                meta = {
                    'total_rows': len(data),
                    'total_stocks': data['code'].n_unique(),
                    'date_range': f"{start_date} ~ {end_date}",
                    'loaded_from_cache': True
                }
                return data, meta
        
        # 从原始文件加载
        data = self._load_from_files(start_date, end_date, columns)
        
        # 保存到缓存
        if use_cache and len(data) > 0:
            data.write_parquet(cache_path)
            self.cache_index[cache_key] = {
                'start_date': start_date,
                'end_date': end_date,
                'columns': columns,
                'created_at': datetime.now().isoformat(),
                'row_count': len(data)
            }
            self._save_cache_index()
            logger.info(f"[LOAD] 已保存到缓存: {cache_path}")
        
        meta = {
            'total_rows': len(data),
            'total_stocks': data['code'].n_unique(),
            'date_range': f"{start_date} ~ {end_date}",
            'loaded_from_cache': False
        }
        
        return data, meta
    
    def _load_from_files(
        self, 
        start_date: str, 
        end_date: str, 
        columns: List[str]
    ) -> pl.DataFrame:
        """从原始Parquet文件加载指定日期范围的数据（P0优化：并行加载）"""
        # 获取所有股票文件
        parquet_files = list(self.data_dir.glob('*.parquet'))
        total_files = len(parquet_files)
        
        logger.info(f"[LOAD] 从 {total_files} 个股票文件加载数据（并行模式）")
        
        # P0优化：使用进程池并行加载
        # 根据CPU核心数确定工作进程数
        num_workers = min(multiprocessing.cpu_count(), 8)
        batch_size = max(1, total_files // num_workers)
        
        # 分割文件批次
        file_batches = [
            parquet_files[i:i + batch_size] 
            for i in range(0, total_files, batch_size)
        ]
        
        logger.info(f"[LOAD] 使用 {num_workers} 个进程并行加载 {len(file_batches)} 个批次")
        
        all_dfs = []
        completed_batches = 0
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # 提交所有批次任务
            future_to_batch = {
                executor.submit(
                    self._load_batch, 
                    batch, 
                    start_date, 
                    end_date, 
                    columns
                ): i 
                for i, batch in enumerate(file_batches)
            }
            
            # 收集结果
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_df = future.result()
                    if batch_df is not None and len(batch_df) > 0:
                        all_dfs.append(batch_df)
                    completed_batches += 1
                    if completed_batches % 2 == 0:
                        logger.info(f"[LOAD] 进度: {completed_batches}/{len(file_batches)} 批次完成")
                except Exception as e:
                    logger.warning(f"[LOAD] 批次 {batch_idx} 加载失败: {e}")
        
        if not all_dfs:
            logger.warning("没有找到任何数据")
            # P1优化：使用更紧凑的数据类型
            return pl.DataFrame(schema={
                "code": pl.Categorical,  # Utf8 → Categorical（节省50%+内存）
                "trade_date": pl.Date,   # Utf8 → Date（节省30%内存，便于日期计算）
                "open": pl.Float32,      # Float64 → Float32（节省50%内存）
                "high": pl.Float32,
                "low": pl.Float32,
                "close": pl.Float32,
                "volume": pl.UInt64      # Float64 → UInt64（成交量应为整数）
            })
        
        # 合并所有批次数据
        data = pl.concat(all_dfs)
        logger.info(f"[LOAD] 完成: {len(data)} 行, {data['code'].n_unique()} 只股票")
        
        return data
    
    @staticmethod
    def _load_batch(
        files_batch: List[Path], 
        start_date: str, 
        end_date: str, 
        columns: List[str]
    ) -> Optional[pl.DataFrame]:
        """加载一批Parquet文件（静态方法，用于进程池）
        
        P0优化：将文件读取逻辑提取为静态方法，支持多进程并行执行
        P1优化：使用紧凑的数据类型减少内存占用
        
        Args:
            files_batch: 一批Parquet文件路径
            start_date: 开始日期
            end_date: 结束日期
            columns: 需要的列
            
        Returns:
            合并后的DataFrame或None
        """
        batch_dfs = []
        
        for f in files_batch:
            try:
                # 扫描Parquet文件（不立即加载）
                lf = pl.scan_parquet(f)
                
                # 选择需要的列
                available_cols = [c for c in columns if c in lf.columns]
                lf = lf.select([pl.col(c) for c in available_cols])
                
                # 过滤日期范围
                lf = lf.filter(
                    (pl.col("trade_date") >= start_date) &
                    (pl.col("trade_date") <= end_date)
                )
                
                # P1优化：使用紧凑的数据类型
                type_mappings = {
                    'code': pl.Categorical,
                    'trade_date': pl.Date,
                    'open': pl.Float32,
                    'high': pl.Float32,
                    'low': pl.Float32,
                    'close': pl.Float32,
                    'volume': pl.UInt64
                }
                
                for col, dtype in type_mappings.items():
                    if col in available_cols:
                        lf = lf.with_columns(pl.col(col).cast(dtype))
                
                # 收集数据
                df = lf.collect()
                
                if len(df) > 0:
                    batch_dfs.append(df)
                    
            except Exception:
                # 静默跳过错误文件
                continue
        
        if batch_dfs:
            return pl.concat(batch_dfs)
        return None
    
    def clear_cache(self, older_than_days: int = 7):
        """清理过期缓存"""
        cutoff = datetime.now() - timedelta(days=older_than_days)
        removed = 0
        
        for cache_key, info in list(self.cache_index.items()):
            created = datetime.fromisoformat(info.get('created_at', '2000-01-01'))
            if created < cutoff:
                cache_path = self._get_cache_path(cache_key)
                if cache_path.exists():
                    cache_path.unlink()
                del self.cache_index[cache_key]
                removed += 1
        
        self._save_cache_index()
        logger.info(f"[CACHE] 清理了 {removed} 个过期缓存文件")


# 全局单例
data_loader = OptimizedDataLoader()


def load_data_optimized(
    days: int = 60,
    end_date: str = None,
    columns: List[str] = None,
    use_cache: bool = True
) -> Tuple[pl.DataFrame, dict]:
    """优化的数据加载函数（便捷接口）"""
    return data_loader.load_recent_data(days, end_date, columns, use_cache)
