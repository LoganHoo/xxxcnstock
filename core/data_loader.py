"""
数据加载器
负责从Parquet文件加载K线数据
支持增量加载和多级缓存
"""
import polars as pl
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
import logging
from datetime import datetime, timedelta

from core.incremental_processor import IncrementalDetector, IncrementalDataLoader
from core.cache.multi_level_cache import MultiLevelCache

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


class DataLoader:
    """
    增强型数据加载器

    支持增量加载、多级缓存和批量处理。
    """

    def __init__(
        self,
        data_dir: str = None,
        use_cache: bool = True,
        use_incremental: bool = True
    ):
        """
        初始化数据加载器

        Args:
            data_dir: K线数据目录
            use_cache: 是否启用缓存
            use_incremental: 是否启用增量加载
        """
        if data_dir is None:
            self.data_dir = Path('data/kline')
            if not self.data_dir.exists():
                self.data_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')
        else:
            self.data_dir = Path(data_dir)

        self.use_cache = use_cache
        self.use_incremental = use_incremental

        # 初始化缓存
        self.cache = None
        if use_cache:
            try:
                self.cache = MultiLevelCache(
                    l1_maxsize=1000,
                    l1_ttl=3600,
                    redis_host='localhost',
                    redis_port=6379,
                    l2_ttl=86400
                )
                logger.info("[DataLoader] 多级缓存已启用")
            except Exception as e:
                logger.warning(f"[DataLoader] 缓存初始化失败: {e}")

        # 初始化增量检测器
        self.incremental_detector = None
        if use_incremental:
            self.incremental_detector = IncrementalDetector(self.data_dir)
            logger.info("[DataLoader] 增量加载已启用")

    def load_stock(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        columns: List[str] = None
    ) -> Optional[pl.DataFrame]:
        """
        加载单只股票数据

        Args:
            code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            columns: 指定列名

        Returns:
            DataFrame 或 None
        """
        cache_key = f"stock:{code}:{start_date}:{end_date}"

        # 尝试从缓存获取
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug(f"[DataLoader] 缓存命中: {code}")
                return cached

        # 从文件加载
        kline_file = self.data_dir / f"{code}.parquet"
        if not kline_file.exists():
            logger.warning(f"[DataLoader] 文件不存在: {kline_file}")
            return None

        try:
            # 使用懒加载优化内存
            lf = pl.scan_parquet(kline_file)

            # 选择列
            if columns:
                lf = lf.select(columns)

            # 日期过滤
            if start_date:
                lf = lf.filter(pl.col('trade_date') >= start_date)
            if end_date:
                lf = lf.filter(pl.col('trade_date') <= end_date)

            # 收集结果
            df = lf.collect()

            # 写入缓存
            if self.cache and len(df) > 0:
                self.cache.set(cache_key, df, level='both')

            return df

        except Exception as e:
            logger.error(f"[DataLoader] 加载失败 {code}: {e}")
            return None

    def load_stocks(
        self,
        codes: List[str],
        start_date: str = None,
        end_date: str = None,
        columns: List[str] = None
    ) -> pl.DataFrame:
        """
        批量加载多只股票数据

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            columns: 指定列名

        Returns:
            合并后的 DataFrame
        """
        dfs = []

        for code in codes:
            df = self.load_stock(code, start_date, end_date, columns)
            if df is not None and len(df) > 0:
                dfs.append(df)

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs, how='diagonal')

    def check_data_freshness(
        self,
        codes: List[str],
        target_date: str
    ) -> Dict[str, Any]:
        """
        检查数据新鲜度

        Args:
            codes: 股票代码列表
            target_date: 目标日期

        Returns:
            检查结果
        """
        if not self.incremental_detector:
            return {'error': '增量检测未启用'}

        results = []
        needs_update = []

        for code in codes:
            result = self.incremental_detector.check_stock(code, target_date, target_date)
            results.append(result)
            if result.needs_update:
                needs_update.append(code)

        return {
            'total': len(codes),
            'up_to_date': len(codes) - len(needs_update),
            'needs_update': len(needs_update),
            'update_list': needs_update
        }

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        if self.cache:
            return {
                'l1_hits': self.cache.hit_stats['l1'],
                'l2_hits': self.cache.hit_stats['l2'],
                'misses': self.cache.hit_stats['miss']
            }
        return {}


def load_data_incremental(
    data_path: str = None,
    start_date: str = None,
    end_date: str = None,
    use_cache: bool = True
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    增量加载数据（便捷函数）

    Args:
        data_path: 数据目录
        start_date: 开始日期
        end_date: 结束日期
        use_cache: 是否使用缓存

    Returns:
        (数据DataFrame, 元数据字典)
    """
    loader = DataLoader(data_path, use_cache=use_cache)

    # 获取所有股票代码
    data_dir = Path(data_path) if data_path else Path('data/kline')
    if not data_dir.exists():
        data_dir = Path('/Volumes/Xdata/workstation/xxxcnstock/data/kline')

    parquet_files = list(data_dir.glob("*.parquet"))
    codes = [f.stem for f in parquet_files]

    logger.info(f"[LOAD] 发现 {len(codes)} 只股票")

    # 批量加载
    data = loader.load_stocks(codes, start_date, end_date)

    meta = {
        'total_stocks': len(codes),
        'total_records': len(data),
        'date_range': {
            'start': data['trade_date'].min() if len(data) > 0 else None,
            'end': data['trade_date'].max() if len(data) > 0 else None
        },
        'cache_stats': loader.get_cache_stats()
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
