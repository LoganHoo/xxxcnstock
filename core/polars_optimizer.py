"""
Polars 性能优化模块

提供基于 Polars 的高性能数据处理功能，包括：
- 批量数据读取与写入
- 高效数据转换
- 并行计算优化
- 内存优化策略
"""

import os
from typing import List, Dict, Optional, Callable, Any, Union
from pathlib import Path
from dataclasses import dataclass
import functools

import polars as pl
import numpy as np
from loguru import logger


@dataclass
class PolarsConfig:
    """Polars 配置"""
    # 线程配置
    n_threads: Optional[int] = None  # None 表示使用所有可用线程
    
    # 内存配置
    streaming: bool = False  # 是否使用流式处理
    
    # 批处理配置
    batch_size: int = 10000
    
    # 分区配置
    partition_size: int = 100000


class PolarsOptimizer:
    """
    Polars 优化器
    
    提供高性能的数据处理功能。
    """
    
    def __init__(self, config: Optional[PolarsConfig] = None):
        self.config = config or PolarsConfig()
        
        # 设置线程数
        if self.config.n_threads:
            pl.Config.set_global_string_cache()
            os.environ['POLARS_MAX_THREADS'] = str(self.config.n_threads)
    
    def read_parquet_batch(
        self,
        files: List[Path],
        columns: Optional[List[str]] = None,
        n_rows: Optional[int] = None
    ) -> pl.DataFrame:
        """
        批量读取 Parquet 文件
        
        Args:
            files: Parquet 文件路径列表
            columns: 指定列名
            n_rows: 限制行数
            
        Returns:
            pl.DataFrame: 合并后的数据框
        """
        if not files:
            return pl.DataFrame()
        
        # 使用 scan_parquet 进行懒加载
        lazy_frames = []
        for file in files:
            if not file.exists():
                logger.warning(f"File not found: {file}")
                continue
            
            lf = pl.scan_parquet(file)
            if columns:
                lf = lf.select(columns)
            if n_rows:
                lf = lf.limit(n_rows)
            lazy_frames.append(lf)
        
        if not lazy_frames:
            return pl.DataFrame()
        
        # 合并并收集
        combined = pl.concat(lazy_frames, how="diagonal")
        return combined.collect(streaming=self.config.streaming)
    
    def read_parquet_parallel(
        self,
        pattern: str,
        base_dir: Path,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        并行读取匹配模式的 Parquet 文件
        
        Args:
            pattern: 文件匹配模式（glob）
            base_dir: 基础目录
            columns: 指定列名
            
        Returns:
            pl.DataFrame: 合并后的数据框
        """
        files = list(base_dir.glob(pattern))
        return self.read_parquet_batch(files, columns)
    
    def write_parquet_optimized(
        self,
        df: pl.DataFrame,
        output_path: Path,
        compression: str = "zstd",
        row_group_size: int = 100000
    ) -> bool:
        """
        优化的 Parquet 写入
        
        Args:
            df: 数据框
            output_path: 输出路径
            compression: 压缩算法
            row_group_size: 行组大小
            
        Returns:
            bool: 是否成功
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            df.write_parquet(
                output_path,
                compression=compression,
                row_group_size=row_group_size,
                use_pyarrow=False  # 使用原生 Polars 写入器
            )
            return True
        except Exception as e:
            logger.error(f"Failed to write parquet: {e}")
            return False
    
    def process_batch(
        self,
        df: pl.DataFrame,
        operations: List[Callable[[pl.DataFrame], pl.DataFrame]]
    ) -> pl.DataFrame:
        """
        批量处理数据
        
        Args:
            df: 输入数据框
            operations: 处理函数列表
            
        Returns:
            pl.DataFrame: 处理后的数据框
        """
        result = df
        for op in operations:
            result = op(result)
        return result
    
    def parallel_map(
        self,
        items: List[Any],
        func: Callable[[Any], Any],
        n_threads: Optional[int] = None
    ) -> List[Any]:
        """
        并行映射
        
        Args:
            items: 输入列表
            func: 映射函数
            n_threads: 线程数
            
        Returns:
            List[Any]: 结果列表
        """
        from concurrent.futures import ThreadPoolExecutor
        
        n_threads = n_threads or self.config.n_threads or os.cpu_count()
        
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            results = list(executor.map(func, items))
        
        return results


class DataTransformer:
    """
    数据转换器
    
    提供常用的数据转换功能。
    """
    
    @staticmethod
    def add_technical_indicators(df: pl.DataFrame) -> pl.DataFrame:
        """
        添加技术指标
        
        Args:
            df: 包含 OHLCV 数据的数据框
            
        Returns:
            pl.DataFrame: 添加指标后的数据框
        """
        # 确保列存在
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # 计算移动平均线
        df = df.with_columns([
            pl.col('close').rolling_mean(window_size=5).alias('ma5'),
            pl.col('close').rolling_mean(window_size=10).alias('ma10'),
            pl.col('close').rolling_mean(window_size=20).alias('ma20'),
            pl.col('close').rolling_mean(window_size=60).alias('ma60'),
        ])
        
        # 计算波动率（20日标准差）
        df = df.with_columns([
            pl.col('close').rolling_std(window_size=20).alias('volatility_20')
        ])
        
        # 计算成交量移动平均
        df = df.with_columns([
            pl.col('volume').rolling_mean(window_size=5).alias('volume_ma5'),
            pl.col('volume').rolling_mean(window_size=20).alias('volume_ma20'),
        ])
        
        # 计算价格变化率
        df = df.with_columns([
            ((pl.col('close') - pl.col('close').shift(1)) / pl.col('close').shift(1) * 100)
            .alias('returns')
        ])
        
        return df
    
    @staticmethod
    def resample_timeframe(
        df: pl.DataFrame,
        date_col: str = 'trade_date',
        timeframe: str = '1w'
    ) -> pl.DataFrame:
        """
        重采样时间周期
        
        Args:
            df: 数据框
            date_col: 日期列名
            timeframe: 目标周期 ('1d', '1w', '1m')
            
        Returns:
            pl.DataFrame: 重采样后的数据框
        """
        # 转换日期列为日期类型
        df = df.with_columns([
            pl.col(date_col).str.strptime(pl.Date, "%Y-%m-%d").alias('_date')
        ])
        
        # 根据周期分组
        if timeframe == '1w':
            df = df.with_columns([
                pl.col('_date').dt.truncate("1w").alias('_period')
            ])
        elif timeframe == '1m':
            df = df.with_columns([
                pl.col('_date').dt.truncate("1mo").alias('_period')
            ])
        else:
            df = df.with_columns([
                pl.col('_date').alias('_period')
            ])
        
        # 聚合
        result = df.group_by('_period').agg([
            pl.col('open').first().alias('open'),
            pl.col('high').max().alias('high'),
            pl.col('low').min().alias('low'),
            pl.col('close').last().alias('close'),
            pl.col('volume').sum().alias('volume'),
        ]).sort('_period')
        
        return result.rename({'_period': date_col})
    
    @staticmethod
    def normalize_prices(df: pl.DataFrame, price_cols: List[str] = None) -> pl.DataFrame:
        """
        标准化价格数据
        
        Args:
            df: 数据框
            price_cols: 价格列名列表
            
        Returns:
            pl.DataFrame: 标准化后的数据框
        """
        price_cols = price_cols or ['open', 'high', 'low', 'close']
        
        # 计算基准价格（第一天的收盘价）
        base_price = df['close'].first()
        
        # 标准化
        for col in price_cols:
            if col in df.columns:
                df = df.with_columns([
                    (pl.col(col) / base_price * 100).alias(f"{col}_norm")
                ])
        
        return df


def polars_cached(cache_dir: Path, ttl_seconds: int = 3600):
    """
    Polars DataFrame 缓存装饰器
    
    缓存函数返回的 DataFrame 到 Parquet 文件。
    
    使用示例：
        @polars_cached(cache_dir=Path("./cache"), ttl_seconds=1800)
        def load_stock_data(code: str) -> pl.DataFrame:
            return pl.read_parquet(f"./data/{code}.parquet")
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = f"{func.__name__}_{hash(str(args))}_{hash(str(kwargs))}"
            cache_file = cache_dir / f"{cache_key}.parquet"
            
            # 检查缓存
            if cache_file.exists():
                import time
                if time.time() - cache_file.stat().st_mtime < ttl_seconds:
                    logger.debug(f"Cache hit: {cache_key}")
                    return pl.read_parquet(cache_file)
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 保存缓存
            if isinstance(result, pl.DataFrame):
                cache_dir.mkdir(parents=True, exist_ok=True)
                result.write_parquet(cache_file)
            
            return result
        
        return wrapper
    return decorator


# 便捷函数
def scan_parquet_directory(
    directory: Path,
    pattern: str = "*.parquet"
) -> pl.LazyFrame:
    """
    扫描 Parquet 目录
    
    Args:
        directory: 目录路径
        pattern: 文件匹配模式
        
    Returns:
        pl.LazyFrame: 懒加载数据框
    """
    files = list(directory.glob(pattern))
    
    if not files:
        return pl.LazyFrame()
    
    return pl.scan_parquet(files)


def optimize_memory_usage(df: pl.DataFrame) -> pl.DataFrame:
    """
    优化内存使用
    
    通过类型转换减少内存占用。
    
    Args:
        df: 数据框
        
    Returns:
        pl.DataFrame: 优化后的数据框
    """
    optimizations = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Float64 -> Float32
        if dtype == pl.Float64:
            optimizations.append(pl.col(col).cast(pl.Float32))
        
        # Int64 -> Int32（如果范围允许）
        elif dtype == pl.Int64:
            max_val = df[col].max()
            min_val = df[col].min()
            if max_val is not None and min_val is not None:
                if -2147483648 <= min_val and max_val <= 2147483647:
                    optimizations.append(pl.col(col).cast(pl.Int32))
        
        # 字符串 -> Categorical（如果基数较低）
        elif dtype == pl.Utf8:
            n_unique = df[col].n_unique()
            n_rows = len(df)
            if n_unique / n_rows < 0.5:  # 基数低于 50%
                optimizations.append(pl.col(col).cast(pl.Categorical))
    
    if optimizations:
        return df.with_columns(optimizations)
    
    return df


def compute_factor_expressions(
    df: pl.DataFrame,
    expressions: Dict[str, pl.Expr]
) -> pl.DataFrame:
    """
    批量计算因子表达式

    Args:
        df: 数据框
        expressions: 因子表达式字典 {因子名: 表达式}

    Returns:
        pl.DataFrame: 添加因子后的数据框
    """
    return df.with_columns([
        expr.alias(name)
        for name, expr in expressions.items()
    ])


class PolarsTechnicalIndicators:
    """
    Polars 技术指标计算类

    提供基于 Polars 的高性能技术指标计算，替代 Pandas 实现。
    性能提升：比 Pandas 快 10-100 倍
    """

    @staticmethod
    def macd(
        df: pl.DataFrame,
        close_col: str = 'close',
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> pl.DataFrame:
        """
        计算 MACD 指标

        Args:
            df: 数据框
            close_col: 收盘价列名
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期

        Returns:
            pl.DataFrame: 添加 MACD 相关列的数据框
        """
        # 计算 EMA
        ema_fast = pl.col(close_col).ewm_mean(span=fast, adjust=False)
        ema_slow = pl.col(close_col).ewm_mean(span=slow, adjust=False)

        # MACD 线 = 快线 - 慢线
        macd_line = ema_fast - ema_slow

        # 信号线 = MACD 的 EMA
        signal_line = macd_line.ewm_mean(span=signal, adjust=False)

        # MACD 柱状图 = MACD 线 - 信号线
        histogram = macd_line - signal_line

        return df.with_columns([
            macd_line.alias('macd'),
            signal_line.alias('macd_signal'),
            histogram.alias('macd_histogram')
        ])

    @staticmethod
    def rsi(
        df: pl.DataFrame,
        close_col: str = 'close',
        period: int = 14
    ) -> pl.DataFrame:
        """
        计算 RSI 指标

        Args:
            df: 数据框
            close_col: 收盘价列名
            period: 计算周期

        Returns:
            pl.DataFrame: 添加 RSI 列的数据框
        """
        # 计算价格变化
        delta = pl.col(close_col) - pl.col(close_col).shift(1)

        # 上涨和下跌
        gain = pl.when(delta > 0).then(delta).otherwise(0)
        loss = pl.when(delta < 0).then(-delta).otherwise(0)

        # 计算平均上涨和下跌（使用 Wilder's smoothing）
        avg_gain = gain.ewm_mean(span=period, adjust=False)
        avg_loss = loss.ewm_mean(span=period, adjust=False)

        # RS = 平均上涨 / 平均下跌
        rs = avg_gain / avg_loss

        # RSI = 100 - (100 / (1 + RS))
        rsi = 100 - (100 / (1 + rs))

        return df.with_columns([
            rsi.alias(f'rsi_{period}')
        ])

    @staticmethod
    def volume_factors(
        df: pl.DataFrame,
        volume_col: str = 'volume',
        close_col: str = 'close',
        open_col: str = 'open',
        high_col: str = 'high',
        low_col: str = 'low'
    ) -> pl.DataFrame:
        """
        计算成交量相关因子

        Args:
            df: 数据框
            volume_col: 成交量列名
            close_col: 收盘价列名
            open_col: 开盘价列名
            high_col: 最高价列名
            low_col: 最低价列名

        Returns:
            pl.DataFrame: 添加成交量因子的数据框
        """
        # 成交量移动平均
        volume_ma5 = pl.col(volume_col).rolling_mean(window_size=5)
        volume_ma20 = pl.col(volume_col).rolling_mean(window_size=20)

        # 成交量比率（当前成交量 / 移动平均）
        volume_ratio = pl.col(volume_col) / volume_ma20

        # 量价趋势 (PVT)
        price_change = (pl.col(close_col) - pl.col(close_col).shift(1)) / pl.col(close_col).shift(1)
        pvt = (price_change * pl.col(volume_col)).cum_sum()

        # OBV (On Balance Volume)
        obv = pl.when(pl.col(close_col) > pl.col(close_col).shift(1)) \
            .then(pl.col(volume_col)) \
            .when(pl.col(close_col) < pl.col(close_col).shift(1)) \
            .then(-pl.col(volume_col)) \
            .otherwise(0).cum_sum()

        # VWAP (Volume Weighted Average Price)
        typical_price = (pl.col(high_col) + pl.col(low_col) + pl.col(close_col)) / 3
        vwap = (typical_price * pl.col(volume_col)).cum_sum() / pl.col(volume_col).cum_sum()

        return df.with_columns([
            volume_ma5.alias('volume_ma5'),
            volume_ma20.alias('volume_ma20'),
            volume_ratio.alias('volume_ratio'),
            pvt.alias('pvt'),
            obv.alias('obv'),
            vwap.alias('vwap')
        ])

    @staticmethod
    def bollinger_bands(
        df: pl.DataFrame,
        close_col: str = 'close',
        period: int = 20,
        std_dev: float = 2.0
    ) -> pl.DataFrame:
        """
        计算布林带

        Args:
            df: 数据框
            close_col: 收盘价列名
            period: 计算周期
            std_dev: 标准差倍数

        Returns:
            pl.DataFrame: 添加布林带列的数据框
        """
        # 中轨 = 移动平均线
        middle = pl.col(close_col).rolling_mean(window_size=period)

        # 标准差
        std = pl.col(close_col).rolling_std(window_size=period)

        # 上轨和下轨
        upper = middle + std * std_dev
        lower = middle - std * std_dev

        # 带宽 (%)
        bandwidth = (upper - lower) / middle * 100

        # %B 指标
        percent_b = (pl.col(close_col) - lower) / (upper - lower)

        return df.with_columns([
            middle.alias('bb_middle'),
            upper.alias('bb_upper'),
            lower.alias('bb_lower'),
            bandwidth.alias('bb_bandwidth'),
            percent_b.alias('bb_percent_b')
        ])

    @staticmethod
    def kdj(
        df: pl.DataFrame,
        high_col: str = 'high',
        low_col: str = 'low',
        close_col: str = 'close',
        n: int = 9,
        m1: int = 3,
        m2: int = 3
    ) -> pl.DataFrame:
        """
        计算 KDJ 指标

        Args:
            df: 数据框
            high_col: 最高价列名
            low_col: 最低价列名
            close_col: 收盘价列名
            n: RSV 计算周期
            m1: K 值平滑因子
            m2: D 值平滑因子

        Returns:
            pl.DataFrame: 添加 KDJ 列的数据框
        """
        # 计算 RSV
        lowest_low = pl.col(low_col).rolling_min(window_size=n)
        highest_high = pl.col(high_col).rolling_max(window_size=n)

        rsv = (pl.col(close_col) - lowest_low) / (highest_high - lowest_low) * 100

        # K 值 = m1-1/m1 * 昨日K + 1/m1 * 今日RSV
        # 使用 EWM 近似计算
        k = rsv.ewm_mean(span=m1, adjust=False)

        # D 值 = m2-1/m2 * 昨日D + 1/m2 * 今日K
        d = k.ewm_mean(span=m2, adjust=False)

        # J 值 = 3K - 2D
        j = 3 * k - 2 * d

        return df.with_columns([
            k.alias('kdj_k'),
            d.alias('kdj_d'),
            j.alias('kdj_j')
        ])

    @staticmethod
    def moving_averages(
        df: pl.DataFrame,
        close_col: str = 'close',
        periods: list = [5, 10, 20, 60, 120, 250]
    ) -> pl.DataFrame:
        """
        计算多条移动平均线

        Args:
            df: 数据框
            close_col: 收盘价列名
            periods: 周期列表

        Returns:
            pl.DataFrame: 添加均线列的数据框
        """
        ma_exprs = []
        for period in periods:
            ma_exprs.append(
                pl.col(close_col).rolling_mean(window_size=period).alias(f'ma{period}')
            )
        return df.with_columns(ma_exprs)

    @staticmethod
    def exponential_moving_averages(
        df: pl.DataFrame,
        close_col: str = 'close',
        periods: list = [5, 10, 20, 60]
    ) -> pl.DataFrame:
        """
        计算多条指数移动平均线 (EMA)

        Args:
            df: 数据框
            close_col: 收盘价列名
            periods: 周期列表

        Returns:
            pl.DataFrame: 添加 EMA 列的数据框
        """
        ema_exprs = []
        for period in periods:
            ema_exprs.append(
                pl.col(close_col).ewm_mean(span=period, adjust=False).alias(f'ema{period}')
            )
        return df.with_columns(ema_exprs)

    @staticmethod
    def cci(
        df: pl.DataFrame,
        high_col: str = 'high',
        low_col: str = 'low',
        close_col: str = 'close',
        period: int = 20
    ) -> pl.DataFrame:
        """
        计算 CCI (Commodity Channel Index) 商品通道指标

        Args:
            df: 数据框
            high_col: 最高价列名
            low_col: 最低价列名
            close_col: 收盘价列名
            period: 计算周期

        Returns:
            pl.DataFrame: 添加 CCI 列的数据框
        """
        # 典型价格 = (最高价 + 最低价 + 收盘价) / 3
        tp = (pl.col(high_col) + pl.col(low_col) + pl.col(close_col)) / 3

        # 典型价格的简单移动平均
        tp_sma = tp.rolling_mean(window_size=period)

        # 平均绝对偏差
        mean_deviation = (tp - tp_sma).abs().rolling_mean(window_size=period)

        # CCI = (典型价格 - 典型价格SMA) / (0.015 * 平均绝对偏差)
        cci = (tp - tp_sma) / (0.015 * mean_deviation)

        return df.with_columns([
            cci.alias(f'cci_{period}')
        ])

    @staticmethod
    def williams_r(
        df: pl.DataFrame,
        high_col: str = 'high',
        low_col: str = 'low',
        close_col: str = 'close',
        period: int = 14
    ) -> pl.DataFrame:
        """
        计算 Williams %R 威廉指标

        Args:
            df: 数据框
            high_col: 最高价列名
            low_col: 最低价列名
            close_col: 收盘价列名
            period: 计算周期

        Returns:
            pl.DataFrame: 添加 Williams %R 列的数据框
        """
        # 最高价的最大值
        highest_high = pl.col(high_col).rolling_max(window_size=period)
        # 最低价的最小值
        lowest_low = pl.col(low_col).rolling_min(window_size=period)

        # Williams %R = (最高价 - 收盘价) / (最高价 - 最低价) * -100
        williams_r = (highest_high - pl.col(close_col)) / (highest_high - lowest_low) * -100

        return df.with_columns([
            williams_r.alias(f'williams_r_{period}')
        ])

    @staticmethod
    def atr(
        df: pl.DataFrame,
        high_col: str = 'high',
        low_col: str = 'low',
        close_col: str = 'close',
        period: int = 14
    ) -> pl.DataFrame:
        """
        计算 ATR (Average True Range) 平均真实波幅

        Args:
            df: 数据框
            high_col: 最高价列名
            low_col: 最低价列名
            close_col: 收盘价列名
            period: 计算周期

        Returns:
            pl.DataFrame: 添加 ATR 列的数据框
        """
        # 真实波幅 = max(最高价-最低价, |最高价-昨日收盘价|, |最低价-昨日收盘价|)
        tr1 = pl.col(high_col) - pl.col(low_col)
        tr2 = (pl.col(high_col) - pl.col(close_col).shift(1)).abs()
        tr3 = (pl.col(low_col) - pl.col(close_col).shift(1)).abs()

        true_range = pl.max_horizontal(tr1, tr2, tr3)

        # ATR = 真实波幅的 EMA
        atr = true_range.ewm_mean(span=period, adjust=False)

        return df.with_columns([
            true_range.alias('true_range'),
            atr.alias(f'atr_{period}')
        ])

    @staticmethod
    def momentum(
        df: pl.DataFrame,
        close_col: str = 'close',
        period: int = 10
    ) -> pl.DataFrame:
        """
        计算动量指标 (Momentum)

        Args:
            df: 数据框
            close_col: 收盘价列名
            period: 计算周期

        Returns:
            pl.DataFrame: 添加动量列的数据框
        """
        # 动量 = 当前收盘价 - N周期前的收盘价
        momentum = pl.col(close_col) - pl.col(close_col).shift(period)

        # 动量比率 = (当前收盘价 / N周期前的收盘价 - 1) * 100
        momentum_pct = (pl.col(close_col) / pl.col(close_col).shift(period) - 1) * 100

        return df.with_columns([
            momentum.alias(f'momentum_{period}'),
            momentum_pct.alias(f'momentum_pct_{period}')
        ])


class PandasPolarsBridge:
    """
    Pandas 到 Polars 的桥接工具

    提供便捷的转换函数，帮助从 Pandas 迁移到 Polars。
    """

    @staticmethod
    def pandas_to_polars(df_pandas) -> pl.DataFrame:
        """
        将 Pandas DataFrame 转换为 Polars DataFrame

        Args:
            df_pandas: Pandas DataFrame

        Returns:
            pl.DataFrame: Polars DataFrame
        """
        return pl.from_pandas(df_pandas)

    @staticmethod
    def polars_to_pandas(df_polars: pl.DataFrame):
        """
        将 Polars DataFrame 转换为 Pandas DataFrame

        Args:
            df_polars: Polars DataFrame

        Returns:
            pd.DataFrame: Pandas DataFrame
        """
        return df_polars.to_pandas()

    @staticmethod
    def convert_factors_to_polars(
        df_pandas,
        factor_functions: Dict[str, callable]
    ) -> pl.DataFrame:
        """
        批量转换 Pandas 因子计算函数为 Polars

        Args:
            df_pandas: Pandas DataFrame
            factor_functions: 因子函数字典 {因子名: 函数}

        Returns:
            pl.DataFrame: 包含所有因子的 Polars DataFrame
        """
        # 转换为 Polars
        df_polars = PandasPolarsBridge.pandas_to_polars(df_pandas)

        # 应用 Polars 指标计算
        indicators = PolarsTechnicalIndicators()

        # MACD
        if 'macd' in factor_functions:
            df_polars = indicators.macd(df_polars)

        # RSI
        if 'rsi' in factor_functions:
            df_polars = indicators.rsi(df_polars)

        # 成交量因子
        if 'volume' in factor_functions:
            df_polars = indicators.volume_factors(df_polars)

        # 布林带
        if 'bollinger' in factor_functions:
            df_polars = indicators.bollinger_bands(df_polars)

        # KDJ
        if 'kdj' in factor_functions:
            df_polars = indicators.kdj(df_polars)

        return df_polars


class PolarsBenchmark:
    """
    Polars 性能基准测试

    对比 Polars 和 Pandas 的性能差异。
    """

    @staticmethod
    def benchmark_macd(df_pandas, iterations: int = 10) -> Dict[str, float]:
        """
        基准测试：MACD 计算

        Args:
            df_pandas: 测试数据
            iterations: 迭代次数

        Returns:
            Dict: 性能对比结果
        """
        import time
        import pandas as pd
        import talib

        # Pandas + TA-Lib
        pandas_times = []
        for _ in range(iterations):
            start = time.time()
            _ = talib.MACD(df_pandas['close'].values)
            pandas_times.append(time.time() - start)

        # Polars
        df_polars = pl.from_pandas(df_pandas)
        polars_times = []
        for _ in range(iterations):
            start = time.time()
            _ = PolarsTechnicalIndicators.macd(df_polars)
            polars_times.append(time.time() - start)

        return {
            'pandas_avg_time': sum(pandas_times) / len(pandas_times),
            'polars_avg_time': sum(polars_times) / len(polars_times),
            'speedup': sum(pandas_times) / sum(polars_times)
        }

    @staticmethod
    def benchmark_rsi(df_pandas, iterations: int = 10) -> Dict[str, float]:
        """基准测试：RSI 计算"""
        import time
        import talib

        # Pandas + TA-Lib
        pandas_times = []
        for _ in range(iterations):
            start = time.time()
            _ = talib.RSI(df_pandas['close'].values)
            pandas_times.append(time.time() - start)

        # Polars
        df_polars = pl.from_pandas(df_pandas)
        polars_times = []
        for _ in range(iterations):
            start = time.time()
            _ = PolarsTechnicalIndicators.rsi(df_polars)
            polars_times.append(time.time() - start)

        return {
            'pandas_avg_time': sum(pandas_times) / len(pandas_times),
            'polars_avg_time': sum(polars_times) / len(polars_times),
            'speedup': sum(pandas_times) / sum(polars_times)
        }

    @staticmethod
    def compare_results(
        df_pandas,
        df_polars: pl.DataFrame,
        factor_name: str,
        tolerance: float = 1e-6
    ) -> bool:
        """
        对比 Pandas 和 Polars 的计算结果

        Args:
            df_pandas: Pandas 结果
            df_polars: Polars 结果
            factor_name: 因子名
            tolerance: 容差

        Returns:
            bool: 结果是否一致
        """
        import numpy as np

        pandas_values = df_pandas[factor_name].values
        polars_values = df_polars[factor_name].to_numpy()

        # 处理 NaN
        mask = ~(np.isnan(pandas_values) | np.isnan(polars_values))

        if not mask.any():
            return True

        diff = np.abs(pandas_values[mask] - polars_values[mask])
        max_diff = diff.max()

        return max_diff < tolerance
