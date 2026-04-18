#!/usr/bin/env python3
"""
XGBoost选股策略 - V4最终修复版
基于因子分析V3的XGBoost模型进行每日选股

V4修复内容：
1. 修复数据泄漏 - 使用shift确保只用过去数据
2. 添加停牌和涨跌停处理
3. 添加交易成本模型
4. 实现行业分散控制
5. 添加文件锁防止并发冲突
6. 添加IC/IR评估指标
7. 实现特征选择和VIF检测
8. 添加模型监控和自动重训练
9. 修复涨跌停处理问题（V4新增）
10. 添加资金容量限制（V4新增）
11. 添加日志轮转机制（V4新增）
12. 添加选股稳定性监控（V4新增）
13. 完善回测指标（V4新增）
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import polars as pl
import numpy as np
import logging
import json
import hashlib
import pickle
import fcntl
import portalocker
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict
from logging.handlers import RotatingFileHandler
import warnings
warnings.filterwarnings('ignore')

from core.factor_library import FactorRegistry
import factors.technical
import factors.volume_price
import factors.market

# 配置日志 - V4: 添加轮转
log_file = PROJECT_ROOT / 'logs' / 'xgboost_picker_v4.log'
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=5, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class RiskControlConfig:
    """风险控制配置 - V5优化版"""
    max_position_per_industry: float = 0.5  # V5: 放宽行业限制 30% -> 50%
    min_daily_volume: float = 10_000_000
    max_stocks: int = 20
    exclude_st: bool = True
    exclude_new_stocks_days: int = 60
    max_correlation: float = 0.8
    exclude_limit_up: bool = True
    exclude_limit_down: bool = True
    stop_loss_pct: float = 0.05  # V5: 新增止损线 5%
    stop_profit_pct: float = 0.10  # V5: 新增止盈线 10%


@dataclass
class TradingCostConfig:
    """交易成本配置"""
    commission_rate: float = 0.0003
    stamp_duty_rate: float = 0.001
    slippage_rate: float = 0.001
    min_commission: float = 5.0


@dataclass
class CapitalConfig:
    """V4: 资金容量配置"""
    initial_capital: float = 1_000_000  # 初始资金100万
    max_position_per_stock: float = 0.1  # 单票最大10%
    lot_size: int = 100  # 最小交易单位100股
    reserve_ratio: float = 0.05  # 保留5%现金


@dataclass
class ModelMetadata:
    """模型元数据"""
    version: str
    train_date: str
    train_samples: int
    feature_cols: List[str]
    feature_importance: List[Tuple[str, float]]
    train_params: Dict[str, Any]
    cv_scores: Optional[List[float]] = None
    validation_score: Optional[float] = None
    ic_score: Optional[float] = None
    ir_score: Optional[float] = None


@dataclass
class PickStabilityMetrics:
    """V4: 选股稳定性指标"""
    turnover_rate: float  # 换手率
    industry_concentration: float  # 行业集中度
    score_stability: float  # 得分稳定性
    new_picks_ratio: float  # 新入选比例


class DataCache:
    """数据缓存管理器 - 带文件锁"""
    
    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or (PROJECT_ROOT / "cache" / "xgboost_v4")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache = {}
        self._max_memory_size = 100
    
    def _get_cache_key(self, prefix: str, params: Dict) -> str:
        param_str = json.dumps(params, sort_keys=True)
        return f"{prefix}_{hashlib.md5(param_str.encode()).hexdigest()[:12]}"
    
    def get(self, prefix: str, params: Dict) -> Optional[Any]:
        cache_key = self._get_cache_key(prefix, params)
        
        if cache_key in self._memory_cache:
            logger.debug(f"内存缓存命中: {cache_key}")
            return self._memory_cache[cache_key]
        
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        lock_file = self.cache_dir / f"{cache_key}.lock"
        
        if cache_file.exists():
            try:
                with open(lock_file, 'w') as f:
                    portalocker.lock(f, portalocker.LOCK_SH)
                    data = pl.read_parquet(cache_file)
                
                self._memory_cache[cache_key] = data
                self._cleanup_memory_cache()
                logger.debug(f"文件缓存命中: {cache_key}")
                return data
            except Exception as e:
                logger.warning(f"读取缓存失败: {e}")
        
        return None
    
    def set(self, prefix: str, params: Dict, data: pl.DataFrame):
        cache_key = self._get_cache_key(prefix, params)
        
        self._memory_cache[cache_key] = data
        self._cleanup_memory_cache()
        
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        lock_file = self.cache_dir / f"{cache_key}.lock"
        
        try:
            with open(lock_file, 'w') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                data.write_parquet(cache_file)
            
            logger.debug(f"缓存已保存: {cache_key}")
        except Exception as e:
            logger.warning(f"保存缓存失败: {e}")
    
    def _cleanup_memory_cache(self):
        if len(self._memory_cache) > self._max_memory_size:
            oldest_keys = list(self._memory_cache.keys())[:len(self._memory_cache) - self._max_memory_size]
            for key in oldest_keys:
                del self._memory_cache[key]
    
    def clear(self):
        self._memory_cache.clear()
        for f in self.cache_dir.glob("*.parquet"):
            f.unlink()
        for f in self.cache_dir.glob("*.lock"):
            f.unlink()
        logger.info("缓存已清空")


class TradeCalendar:
    """交易日历管理 - 带备用方案"""
    
    def __init__(self, data_path: Path = None):
        self.data_path = data_path or (PROJECT_ROOT / "data" / "kline")
        self._dates = None
        self._load_calendar()
    
    def _load_calendar(self):
        sh_index_file = self.data_path / "000001.parquet"
        sz_index_file = self.data_path / "399001.parquet"
        
        for index_file in [sh_index_file, sz_index_file]:
            if index_file.exists():
                try:
                    df = pl.read_parquet(index_file)
                    self._dates = sorted(df["trade_date"].unique().to_list())
                    logger.info(f"使用 {index_file.name} 作为交易日历")
                    return
                except Exception as e:
                    logger.warning(f"加载 {index_file.name} 失败: {e}")
        
        logger.error("无法加载交易日历")
        self._dates = []
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        return [d for d in self._dates if start_date <= d <= end_date]
    
    def get_next_n_trade_dates(self, date: str, n: int) -> List[str]:
        dates = self.get_trade_dates(date, "20991231")
        try:
            idx = dates.index(date)
            return dates[idx:idx+n+1]
        except ValueError:
            logger.error(f"日期 {date} 不在交易日历中")
            return []
    
    def is_trade_date(self, date: str) -> bool:
        return date in self._dates


class XGBoostStockPicker:
    """XGBoost选股器 - V4最终修复版"""
    
    def __init__(self, risk_config: RiskControlConfig = None, cost_config: TradingCostConfig = None, capital_config: CapitalConfig = None):
        self.model = None
        self.feature_cols = []
        self.model_trained = False
        self.risk_config = risk_config or RiskControlConfig()
        self.cost_config = cost_config or TradingCostConfig()
        self.capital_config = capital_config or CapitalConfig()
        self.cache = DataCache()
        self.calendar = TradeCalendar()
        self.metadata: Optional[ModelMetadata] = None
        self.stock_info: Dict[str, Dict] = {}
        self._load_stock_info()
        self._previous_picks: List[str] = []  # V4: 记录上次选股
    
    def _load_stock_info(self):
        """加载股票信息（行业、名称等）"""
        stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
        if stock_list_file.exists():
            try:
                df = pl.read_parquet(stock_list_file)
                for row in df.to_dicts():
                    code = row.get('code') or row.get('ts_code', '').split('.')[0]
                    if code:
                        self.stock_info[code] = {
                            'name': row.get('name', ''),
                            'industry': row.get('industry', '未知'),
                            'list_date': row.get('list_date', '')
                        }
                logger.info(f"加载 {len(self.stock_info)} 只股票信息")
            except Exception as e:
                logger.warning(f"加载股票信息失败: {e}")
    
    def load_training_data(self, start_date: str, end_date: str, max_stocks: Optional[int] = None, batch_size: int = 500) -> pl.DataFrame:
        """加载训练数据"""
        cache_params = {'start': start_date, 'end': end_date, 'max_stocks': max_stocks}
        cached_data = self.cache.get('training_data', cache_params)
        if cached_data is not None:
            logger.info(f"使用缓存数据: {len(cached_data)} 条")
            return cached_data
        
        data_path = PROJECT_ROOT / "data" / "kline"
        if not data_path.exists():
            logger.error(f"数据目录不存在: {data_path}")
            return pl.DataFrame()
        
        parquet_files = list(data_path.glob("*.parquet"))
        if max_stocks:
            parquet_files = parquet_files[:max_stocks]
        
        logger.info(f"加载 {len(parquet_files)} 只股票数据...")
        
        all_data = []
        total_rows = 0
        failed_stocks = []
        
        for i, parquet_file in enumerate(parquet_files):
            try:
                df = pl.read_parquet(parquet_file)
                df = df.with_columns([
                    pl.col("code").cast(pl.Utf8),
                    pl.col("trade_date").cast(pl.Utf8),
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Float64),
                ])
                
                df = df.filter((pl.col("trade_date") >= start_date) & (pl.col("trade_date") <= end_date))
                
                if len(df) > 0:
                    all_data.append(df)
                    total_rows += len(df)
                
                if len(all_data) >= batch_size:
                    logger.info(f"处理进度: {i+1}/{len(parquet_files)}, 已加载 {total_rows} 行")
                    temp_data = pl.concat(all_data, how="diagonal_relaxed")
                    all_data = [temp_data]
                    
            except Exception as e:
                failed_stocks.append((parquet_file.stem, str(e)))
        
        if failed_stocks:
            logger.warning(f"加载失败 {len(failed_stocks)} 只股票")
        
        if not all_data:
            logger.warning("未加载到任何数据")
            return pl.DataFrame()
        
        data = pl.concat(all_data, how="diagonal_relaxed")
        logger.info(f"数据加载完成: {len(data)} 行, {data['code'].n_unique()} 只股票")
        
        # V8: 合并基本面数据到K线数据
        data = self._merge_fundamental_data(data)
        
        self.cache.set('training_data', cache_params, data)
        return data
    
    def _merge_fundamental_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """V8: 合并基本面数据（PE/PB/ROE等）到K线数据"""
        stock_list_file = PROJECT_ROOT / "data" / "stock_list.parquet"
        if not stock_list_file.exists():
            logger.warning("股票列表不存在，跳过基本面数据合并")
            return data
        
        try:
            df_stocks = pl.read_parquet(stock_list_file)
            
            # 检查是否有基本面数据列
            fundamental_cols = ['pe_ttm', 'pb', 'roe', 'revenue_growth']
            available_cols = [c for c in fundamental_cols if c in df_stocks.columns]
            
            if not available_cols:
                logger.info("股票列表中没有基本面数据")
                return data
            
            logger.info(f"V8: 合并基本面数据: {available_cols}")
            
            # 选择需要的列
            cols_to_merge = ['code'] + available_cols
            df_fund = df_stocks.select(cols_to_merge)
            
            # 合并到K线数据
            data = data.join(df_fund, on='code', how='left')
            
            # 统计覆盖情况
            for col in available_cols:
                non_null = data.filter(pl.col(col).is_not_null()).shape[0]
                coverage = non_null / len(data) * 100
                logger.info(f"  {col}: {coverage:.1f}% 覆盖率")
            
            return data
            
        except Exception as e:
            logger.warning(f"合并基本面数据失败: {e}")
            return data
    
    def calculate_factors(self, data: pl.DataFrame, use_cache: bool = True) -> pl.DataFrame:
        """计算所有因子 - 修复数据泄漏"""
        if use_cache:
            sample = data.head(100)
            cache_params = {
                'codes': sorted(sample['code'].unique().to_list())[:10],
                'dates': sorted(sample['trade_date'].unique().to_list())[:5],
                'shape': f"{len(data)}x{len(data.columns)}"
            }
            cached = self.cache.get('factors', cache_params)
            if cached is not None:
                logger.info("使用缓存因子")
                return cached
        
        logger.info("计算因子...")
        original_len = len(data)
        
        registry = FactorRegistry()
        factor_classes = registry.list_all()
        
        success_factors = 0
        failed_factors = []
        
        for name, factor_class in factor_classes.items():
            try:
                factor = factor_class()
                data = factor.calculate(data)
                success_factors += 1
            except Exception as e:
                failed_factors.append((name, str(e)))
        
        if failed_factors:
            logger.warning(f"{len(failed_factors)} 个因子计算失败")
        logger.info(f"成功计算 {success_factors} 个因子")
        
        data = self._calculate_proxy_factors_v4(data)
        data = self._clean_factors(data)
        
        logger.info(f"因子计算完成: {len(data)} 行")
        
        if use_cache:
            self.cache.set('factors', cache_params, data)
        
        return data
    
    def _calculate_proxy_factors_v4(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算基本面因子 - V8使用真实PE/PB/ROE数据"""
        try:
            # V4: 使用shift(1)确保只用过去数据
            data = data.with_columns([
                pl.col("close").shift(1).rolling_mean(window_size=20).over("code").alias("ma20_lag1"),
                pl.col("close").shift(1).rolling_std(window_size=20).over("code").alias("vol20_lag1"),
                pl.col("volume").shift(1).rolling_mean(window_size=20).over("code").alias("vol_ma20_lag1"),
                pl.col("high").shift(1).rolling_max(window_size=20).over("code").alias("high20_lag1"),
                pl.col("low").shift(1).rolling_min(window_size=20).over("code").alias("low20_lag1")
            ])
            
            # V8: 优先使用真实估值数据，缺失时使用代理因子
            # 检查是否有真实PE/PB数据
            has_real_pe = 'pe_ttm' in data.columns
            has_real_pb = 'pb' in data.columns
            has_real_roe = 'roe' in data.columns
            
            if has_real_pe:
                # V8: 使用真实PE，并做异常值处理
                data = data.with_columns([
                    pl.when((pl.col("pe_ttm") > 0) & (pl.col("pe_ttm") < 200))
                    .then(pl.col("pe_ttm"))
                    .otherwise(pl.col("ma20_lag1") / (pl.col("vol20_lag1") + 0.001))
                    .alias("factor_pe_real")
                ])
                logger.info("V8: 使用真实PE数据")
            else:
                data = data.with_columns([
                    (pl.col("close").shift(1) / (pl.col("ma20_lag1") * pl.col("vol_ma20_lag1") / 1e6 + 0.001))
                    .alias("factor_pe_real")
                ])
            
            if has_real_pb:
                # V8: 使用真实PB
                data = data.with_columns([
                    pl.when((pl.col("pb") > 0) & (pl.col("pb") < 20))
                    .then(pl.col("pb"))
                    .otherwise(pl.col("close").shift(1) / pl.col("ma20_lag1"))
                    .alias("factor_pb_real")
                ])
                logger.info("V8: 使用真实PB数据")
            else:
                data = data.with_columns([
                    (pl.col("close").shift(1) / pl.col("ma20_lag1")).alias("factor_pb_real")
                ])
            
            # V8: 估值因子 (综合真实和代理数据)
            data = data.with_columns([
                (1.0 / (pl.col("factor_pe_real") + 0.1)).alias("factor_ep"),  # 盈利收益率
                (1.0 / (pl.col("factor_pb_real") + 0.1)).alias("factor_bp"),  # 账面市值比
                # V7: PS代理因子 (价格/成交量活跃度)
                (pl.col("close").shift(1) / (pl.col("vol_ma20_lag1") / 1e6 + 0.001)).alias("factor_ps_proxy")
            ])
            
            # V7: 收益率相关因子
            data = data.with_columns([
                pl.col("close").pct_change().over("code").shift(1).alias("ret_lag1")
            ])
            
            data = data.with_columns([
                pl.col("ret_lag1").rolling_mean(window_size=60).over("code").alias("ret_mean_lag1"),
                pl.col("ret_lag1").rolling_std(window_size=60).over("code").alias("ret_std_lag1"),
                # V7: 不同周期的收益稳定性
                pl.col("ret_lag1").rolling_mean(window_size=20).over("code").alias("ret_mean_20d"),
                pl.col("ret_lag1").rolling_mean(window_size=120).over("code").alias("ret_mean_120d")
            ])
            
            # V8: ROE和盈利能力因子 (优先使用真实数据)
            if has_real_roe:
                data = data.with_columns([
                    pl.when(pl.col("roe").is_not_null() & (pl.col("roe").abs() < 100))
                    .then(pl.col("roe"))
                    .otherwise(pl.col("ret_mean_lag1") / (pl.col("ret_std_lag1") + 0.001) * 10)
                    .alias("factor_roe_real")
                ])
                logger.info("V8: 使用真实ROE数据")
            else:
                data = data.with_columns([
                    (pl.col("ret_mean_lag1") / (pl.col("ret_std_lag1") + 0.001) * 10).alias("factor_roe_real")
                ])
            
            # V8: 营收增长率 (优先使用真实数据)
            has_real_revenue_growth = 'revenue_growth' in data.columns
            if has_real_revenue_growth:
                data = data.with_columns([
                    pl.when(pl.col("revenue_growth").is_not_null() & (pl.col("revenue_growth").abs() < 200))
                    .then(pl.col("revenue_growth"))
                    .otherwise(pl.col("ret_mean_20d") / (pl.col("ret_mean_120d").abs() + 0.001) * 10)
                    .alias("factor_revenue_growth_real")
                ])
                logger.info("V8: 使用真实营收增长率数据")
            else:
                data = data.with_columns([
                    (pl.col("ret_mean_20d") / (pl.col("ret_mean_120d").abs() + 0.001) * 10)
                    .alias("factor_revenue_growth_real")
                ])
            
            # V8: 综合财务因子
            data = data.with_columns([
                # ROE因子
                pl.col("factor_roe_real").alias("factor_roe"),
                # 营收增长因子
                pl.col("factor_revenue_growth_real").alias("factor_revenue_growth"),
                # V7: 盈利稳定性 (代理)
                (pl.col("ret_mean_lag1") / (pl.col("close").shift(1) + 0.001)).alias("factor_profitability"),
                # V7: 夏普比率代理
                (pl.col("ret_mean_20d") / (pl.col("ret_std_lag1") + 0.001)).alias("factor_sharpe_proxy"),
                # V8: 价值质量综合评分
                (pl.col("factor_ep") + pl.col("factor_bp") + pl.col("factor_roe_real") / 100).alias("factor_value_quality")
            ])
            
            # V7: 财务健康度因子
            data = data.with_columns([
                # V7: 波动率健康度 (低波动 = 健康)
                (1.0 / (pl.col("vol20_lag1") / pl.col("ma20_lag1") + 0.001)).alias("factor_vol_health"),
                # V7: 流动性健康度
                (pl.col("vol_ma20_lag1") / 1e6).alias("factor_liquidity_health"),
                # V7: 价格位置健康度 (当前价格在20日区间中的位置)
                ((pl.col("close").shift(1) - pl.col("low20_lag1")) / 
                 (pl.col("high20_lag1") - pl.col("low20_lag1") + 0.001)).alias("factor_price_position"),
                # V7: 动量健康度
                ((pl.col("close").shift(1) - pl.col("ma20_lag1")) / pl.col("ma20_lag1")).alias("factor_momentum_health")
            ])
            
            # V7: 价值因子组合
            data = data.with_columns([
                # V7: 综合价值评分 (低PE + 低PB + 高盈利)
                (pl.col("factor_ep_proxy") + pl.col("factor_pb_proxy") + pl.col("factor_roe_proxy")).alias("factor_value_score"),
                # V7: 质量评分 (高盈利 + 低波动 + 高流动性)
                (pl.col("factor_profitability") + pl.col("factor_vol_health") + pl.col("factor_liquidity_health")).alias("factor_quality_score")
            ])
            
            data = data.drop([
                "ma20_lag1", "vol20_lag1", "vol_ma20_lag1", "high20_lag1", "low20_lag1",
                "ret_lag1", "ret_mean_lag1", "ret_std_lag1", "ret_mean_20d", "ret_mean_120d"
            ])
            
        except Exception as e:
            logger.error(f"代理因子计算失败: {e}")
        
        return data
    
    def _clean_factors(self, data: pl.DataFrame) -> pl.DataFrame:
        """清理因子缺失值"""
        factor_cols = [c for c in data.columns if c.startswith("factor_")]
        
        for col in factor_cols:
            null_count = data.filter(pl.col(col).is_null() | pl.col(col).is_nan()).shape[0]
            if null_count > 0:
                logger.debug(f"因子 {col} 有 {null_count} 个缺失值")
            
            data = data.with_columns([
                pl.when(pl.col(col).is_nan() | pl.col(col).is_null())
                .then(0)
                .otherwise(pl.col(col))
                .alias(col)
            ])
        
        return data
    
    def calculate_forward_returns_v4(self, data: pl.DataFrame, days: int = 5) -> pl.DataFrame:
        """计算未来N日收益 - V4修复停牌和涨跌停处理"""
        logger.info(f"计算未来 {days} 日收益...")
        
        data = data.with_columns([
            pl.col("close").shift(-days).over("code").alias(f"future_{days}d"),
            pl.col("volume").shift(-days).over("code").alias(f"future_volume_{days}d"),
            pl.col("high").shift(-days).over("code").alias(f"future_high_{days}d"),
            pl.col("low").shift(-days).over("code").alias(f"future_low_{days}d"),
            pl.col("open").shift(-days).over("code").alias(f"future_open_{days}d")
        ])
        
        # V4: 检测停牌和涨跌停
        data = data.with_columns([
            # 停牌检测
            pl.when(pl.col(f"future_volume_{days}d") == 0).then(True).otherwise(False).alias(f"is_suspended_{days}d"),
            # 涨停检测（未来无法买入）
            pl.when(
                (pl.col(f"future_high_{days}d") == pl.col(f"future_low_{days}d")) &
                (pl.col(f"future_high_{days}d") > pl.col(f"future_open_{days}d") * 1.095)
            ).then(True).otherwise(False).alias(f"is_limit_up_{days}d"),
            # 跌停检测（未来无法卖出）
            pl.when(
                (pl.col(f"future_high_{days}d") == pl.col(f"future_low_{days}d")) &
                (pl.col(f"future_low_{days}d") < pl.col(f"future_open_{days}d") * 0.905)
            ).then(True).otherwise(False).alias(f"is_limit_down_{days}d")
        ])
        
        # V4: 如果停牌或涨跌停，标记为无效
        data = data.with_columns([
            pl.when(
                pl.col(f"future_{days}d").is_null() | 
                pl.col(f"is_suspended_{days}d") |
                pl.col(f"is_limit_up_{days}d") |
                pl.col(f"is_limit_down_{days}d")
            )
            .then(None)
            .otherwise((pl.col(f"future_{days}d") - pl.col("close")) / pl.col("close"))
            .alias(f"forward_return_{days}d")
        ])
        
        data = data.drop([f"future_{days}d", f"future_volume_{days}d", f"future_high_{days}d", 
                         f"future_low_{days}d", f"future_open_{days}d"])
        
        valid_count = data.filter(pl.col(f"forward_return_{days}d").is_not_null()).shape[0]
        suspended_count = data.filter(pl.col(f"is_suspended_{days}d")).shape[0]
        limit_up_count = data.filter(pl.col(f"is_limit_up_{days}d")).shape[0]
        limit_down_count = data.filter(pl.col(f"is_limit_down_{days}d")).shape[0]
        
        logger.info(f"有效未来收益样本: {valid_count}, 停牌: {suspended_count}, 涨停: {limit_up_count}, 跌停: {limit_down_count}")
        
        return data
    
    def select_features_vif(self, data: pl.DataFrame, threshold: float = 10.0) -> List[str]:
        """基于VIF进行特征选择"""
        try:
            from statsmodels.stats.outliers_influence import variance_inflation_factor
            
            factor_cols = [c for c in data.columns if c.startswith("factor_")]
            
            X = data[factor_cols].to_numpy()
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            
            vif_data = pl.DataFrame({
                'feature': factor_cols,
                'vif': [variance_inflation_factor(X, i) for i in range(X.shape[1])]
            })
            
            selected = vif_data.filter(pl.col('vif') < threshold)['feature'].to_list()
            
            logger.info(f"VIF特征选择: {len(factor_cols)} -> {len(selected)}")
            logger.info(f"剔除高VIF特征: {set(factor_cols) - set(selected)}")
            
            return selected
            
        except ImportError:
            logger.warning("statsmodels未安装，跳过VIF检测")
            return [c for c in data.columns if c.startswith("factor_")]
    
    def calculate_ic_ir(self, predictions: np.ndarray, actuals: np.ndarray, dates: np.ndarray) -> Tuple[float, float]:
        """计算IC和IR"""
        unique_dates = np.unique(dates)
        ic_values = []
        
        for date in unique_dates:
            mask = dates == date
            if np.sum(mask) > 10:
                ic = np.corrcoef(predictions[mask], actuals[mask])[0, 1]
                if not np.isnan(ic):
                    ic_values.append(ic)
        
        if len(ic_values) == 0:
            return 0.0, 0.0
        
        ic_mean = np.mean(ic_values)
        ic_std = np.std(ic_values)
        ir = ic_mean / (ic_std + 0.0001)
        
        return ic_mean, ir
    
    def train(self, data: pl.DataFrame, validation_split: float = 0.2, use_cv: bool = True, cv_folds: int = 5, use_vif: bool = True) -> bool:
        """训练XGBoost模型 - V4完整版"""
        try:
            import xgboost as xgb
            from sklearn.model_selection import TimeSeriesSplit
        except ImportError as e:
            logger.error(f"依赖缺失: {e}")
            return False
        
        logger.info("=" * 80)
        logger.info("开始训练XGBoost模型 V4...")
        logger.info("=" * 80)
        
        if use_vif:
            self.feature_cols = self.select_features_vif(data)
        else:
            self.feature_cols = [c for c in data.columns if c.startswith("factor_")]
        
        logger.info(f"特征数量: {len(self.feature_cols)}")
        
        target_col = "forward_return_5d"
        train_data = data.filter(pl.col(target_col).is_not_null())
        
        if len(train_data) < 1000:
            logger.error(f"训练数据不足: {len(train_data)} < 1000")
            return False
        
        train_data = train_data.sort("trade_date")
        
        X = train_data[self.feature_cols].to_numpy()
        y = train_data[target_col].to_numpy()
        dates = train_data["trade_date"].to_numpy()
        
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isinf(X).any(axis=1) | np.isnan(y) | np.isinf(y))
        X = X[valid_mask]
        y = y[valid_mask]
        dates = dates[valid_mask]
        
        logger.info(f"有效训练样本: {len(X)}")
        
        split_idx = int(len(X) * (1 - validation_split))
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        dates_val = dates[split_idx:]
        
        logger.info(f"训练集: {len(X_train)}, 验证集: {len(X_val)}")
        
        y_train_rank = np.argsort(np.argsort(y_train)) / len(y_train)
        
        cv_scores = []
        if use_cv and len(X_train) > cv_folds * 1000:
            logger.info(f"进行 {cv_folds} 折时间序列交叉验证...")
            tscv = TimeSeriesSplit(n_splits=cv_folds)
            
            for fold, (train_idx, val_idx) in enumerate(tscv.split(X_train)):
                X_fold_train, X_fold_val = X_train[train_idx], X_train[val_idx]
                y_fold_train = y_train_rank[train_idx]
                
                # V7: 优化后的模型参数
                model = xgb.XGBRegressor(
                    n_estimators=200, max_depth=5, learning_rate=0.05,
                    subsample=0.7, colsample_bytree=0.7, 
                    reg_alpha=0.5, reg_lambda=2.0,
                    min_child_weight=3, gamma=0.1,
                    random_state=42, n_jobs=-1
                )
                
                model.fit(X_fold_train, y_fold_train)
                pred = model.predict(X_fold_val)
                actual_rank = np.argsort(np.argsort(y_train[val_idx])) / len(val_idx)
                corr = np.corrcoef(pred, actual_rank)[0, 1]
                cv_scores.append(corr)
                logger.info(f"  Fold {fold+1}: 相关性 = {corr:.4f}")
            
            logger.info(f"CV平均相关性: {np.mean(cv_scores):.4f}")
        
        logger.info("训练最终模型...")
        # V7: 优化后的最终模型参数
        self.model = xgb.XGBRegressor(
            n_estimators=500, max_depth=6, learning_rate=0.03,
            subsample=0.7, colsample_bytree=0.7,
            reg_alpha=0.5, reg_lambda=2.0,
            min_child_weight=3, gamma=0.1,
            random_state=42, n_jobs=-1,
            early_stopping_rounds=30, eval_metric="rmse"
        )
        
        self.model.fit(
            X_train, y_train_rank,
            eval_set=[(X_val, np.argsort(np.argsort(y_val)) / len(y_val))],
            verbose=False
        )
        
        self.model_trained = True
        
        val_pred = self.model.predict(X_val)
        val_actual_rank = np.argsort(np.argsort(y_val)) / len(y_val)
        ic_score, ir_score = self.calculate_ic_ir(val_pred, val_actual_rank, dates_val)
        val_corr = np.corrcoef(val_pred, val_actual_rank)[0, 1]
        
        logger.info(f"验证集相关性: {val_corr:.4f}")
        logger.info(f"IC: {ic_score:.4f}, IR: {ir_score:.4f}")
        
        importance = self.model.feature_importances_
        feature_imp = sorted(
            [(self.feature_cols[i], float(importance[i])) for i in range(len(self.feature_cols))],
            key=lambda x: x[1], reverse=True
        )
        
        logger.info("=" * 80)
        logger.info("特征重要性Top15:")
        for factor, imp in feature_imp[:15]:
            logger.info(f"  {factor}: {imp:.4f}")
        logger.info("=" * 80)
        
        self.metadata = ModelMetadata(
            version="4.0",
            train_date=datetime.now().isoformat(),
            train_samples=len(X_train),
            feature_cols=self.feature_cols,
            feature_importance=feature_imp[:20],
            train_params={'n_estimators': 300, 'max_depth': 6, 'learning_rate': 0.03},
            cv_scores=cv_scores if cv_scores else None,
            validation_score=val_corr,
            ic_score=ic_score,
            ir_score=ir_score
        )
        
        return True
    
    def predict(self, data: pl.DataFrame) -> pl.DataFrame:
        """预测选股得分"""
        if not self.model_trained:
            logger.error("模型未训练")
            return data
        
        missing_cols = set(self.feature_cols) - set(data.columns)
        if missing_cols:
            logger.error(f"缺少特征列: {missing_cols}")
            for col in missing_cols:
                data = data.with_columns([pl.lit(0.0).alias(col)])
        
        X = data[self.feature_cols].to_numpy()
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        scores = self.model.predict(X)
        
        return data.with_columns([pl.Series(scores).alias("xgboost_score")])
    
    def apply_risk_control_v4(self, data: pl.DataFrame, date: str) -> pl.DataFrame:
        """应用风险控制 - V4完整版（含涨跌停过滤）"""
        original_count = len(data)
        
        # 1. 流动性过滤
        data = data.with_columns([(pl.col("close") * pl.col("volume")).alias("turnover")])
        data = data.filter(pl.col("turnover") >= self.risk_config.min_daily_volume)
        logger.info(f"流动性过滤: {original_count} -> {len(data)}")
        
        # 2. ST和退市排除
        if self.risk_config.exclude_st:
            st_codes = [code for code, info in self.stock_info.items() 
                       if 'ST' in info.get('name', '') or '退市' in info.get('name', '')]
            if st_codes:
                data = data.filter(~pl.col("code").is_in(st_codes))
                logger.info(f"ST过滤后: {len(data)}")
        
        # 3. 新股排除
        if self.risk_config.exclude_new_stocks_days > 0:
            from datetime import datetime
            current_date = datetime.strptime(date, '%Y-%m-%d')
            new_stock_codes = [
                code for code, info in self.stock_info.items()
                if info.get('list_date')
                and (current_date - datetime.strptime(info['list_date'], '%Y%m%d')).days < self.risk_config.exclude_new_stocks_days
            ]
            if new_stock_codes:
                data = data.filter(~pl.col("code").is_in(new_stock_codes))
                logger.info(f"新股过滤后: {len(data)}")
        
        # V4: 4. 涨停排除（无法买入）
        if self.risk_config.exclude_limit_up:
            data = data.with_columns([
                pl.when(
                    (pl.col("high") == pl.col("low")) &
                    (pl.col("high") > pl.col("open") * 1.095))
                .then(True).otherwise(False).alias("is_limit_up_today")
            ])
            limit_up_count = data.filter(pl.col("is_limit_up_today")).shape[0]
            data = data.filter(~pl.col("is_limit_up_today"))
            data = data.drop("is_limit_up_today")
            if limit_up_count > 0:
                logger.info(f"涨停过滤后: {len(data)} (排除{limit_up_count}只)")
        
        # V4: 5. 跌停排除（无法卖出）
        if self.risk_config.exclude_limit_down:
            data = data.with_columns([
                pl.when(
                    (pl.col("high") == pl.col("low")) &
                    (pl.col("low") < pl.col("open") * 0.905))
                .then(True).otherwise(False).alias("is_limit_down_today")
            ])
            limit_down_count = data.filter(pl.col("is_limit_down_today")).shape[0]
            data = data.filter(~pl.col("is_limit_down_today"))
            data = data.drop("is_limit_down_today")
            if limit_down_count > 0:
                logger.info(f"跌停过滤后: {len(data)} (排除{limit_down_count}只)")
        
        return data
    
    def apply_industry_diversification(self, selected: pl.DataFrame, top_n: int) -> pl.DataFrame:
        """应用行业分散控制"""
        industries = []
        for code in selected['code'].to_list():
            industries.append(self.stock_info.get(code, {}).get('industry', '未知'))
        selected = selected.with_columns([pl.Series(industries).alias("industry")])
        
        max_per_industry = max(1, int(top_n * self.risk_config.max_position_per_industry))
        
        result = []
        industry_counts = defaultdict(int)
        
        for row in selected.to_dicts():
            industry = row.get('industry', '未知')
            if industry_counts[industry] < max_per_industry:
                result.append(row)
                industry_counts[industry] += 1
            if len(result) >= top_n:
                break
        
        logger.info(f"行业分散后: {len(result)} 只, 行业分布: {dict(industry_counts)}")
        
        return pl.DataFrame(result)
    
    def calculate_stability_metrics(self, current_picks: List[str]) -> PickStabilityMetrics:
        """V4: 计算选股稳定性指标"""
        if not self._previous_picks:
            self._previous_picks = current_picks
            return PickStabilityMetrics(0.0, 0.0, 0.0, 0.0)
        
        # 换手率
        common_picks = set(current_picks) & set(self._previous_picks)
        turnover_rate = 1 - len(common_picks) / len(current_picks) if current_picks else 0.0
        
        # 新入选比例
        new_picks_ratio = len(set(current_picks) - set(self._previous_picks)) / len(current_picks) if current_picks else 0.0
        
        # 行业集中度
        industries = [self.stock_info.get(code, {}).get('industry', '未知') for code in current_picks]
        industry_counts = defaultdict(int)
        for ind in industries:
            industry_counts[ind] += 1
        max_industry_ratio = max(industry_counts.values()) / len(current_picks) if current_picks else 0.0
        
        self._previous_picks = current_picks
        
        return PickStabilityMetrics(
            turnover_rate=turnover_rate,
            industry_concentration=max_industry_ratio,
            score_stability=0.0,  # 需要历史得分数据
            new_picks_ratio=new_picks_ratio
        )
    
    def pick_stocks_v4(self, date: str, top_n: int = None, risk_control: bool = True) -> List[Dict]:
        """选股 - V4完整版（含稳定性监控）"""
        top_n = top_n or self.risk_config.max_stocks
        logger.info(f"选股日期: {date}, 目标数量: {top_n}")
        
        data_path = PROJECT_ROOT / "data" / "kline"
        all_data = []
        error_count = 0
        
        parquet_files = list(data_path.glob("*.parquet"))
        logger.info(f"扫描 {len(parquet_files)} 只股票...")
        
        for parquet_file in parquet_files:
            try:
                df = pl.read_parquet(parquet_file)
                df = df.with_columns([
                    pl.col("code").cast(pl.Utf8),
                    pl.col("trade_date").cast(pl.Utf8),
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Float64),
                ])
                df = df.filter(pl.col("trade_date") == date)
                if len(df) > 0:
                    all_data.append(df)
            except Exception as e:
                error_count += 1
        
        if error_count > 0:
            logger.warning(f"读取失败: {error_count} 只股票")
        
        if not all_data:
            logger.error(f"未找到 {date} 的数据")
            return []
        
        common_cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
        aligned_data = [df.select([c for c in common_cols if c in df.columns]) for df in all_data]
        data = pl.concat(aligned_data, how="diagonal_relaxed")
        logger.info(f"当日有效股票: {len(data)}")
        
        if risk_control:
            data = self.apply_risk_control_v4(data, date)
        
        data = self.calculate_factors(data)
        data = self.predict(data)
        
        selected = data.sort("xgboost_score", descending=True).head(top_n * 2)
        
        if risk_control:
            selected = self.apply_industry_diversification(selected, top_n)
        else:
            selected = selected.head(top_n)
        
        results = selected[["code", "xgboost_score", "close", "volume"]].to_dicts()
        
        # V4: 计算稳定性指标
        pick_codes = [r["code"] for r in results]
        stability = self.calculate_stability_metrics(pick_codes)
        logger.info(f"选股稳定性 - 换手率: {stability.turnover_rate:.2%}, 新入选: {stability.new_picks_ratio:.2%}")
        
        self._log_picks_v4(date, results, stability)
        
        return results
    
    def _log_picks_v4(self, date: str, picks: List[Dict], stability: PickStabilityMetrics):
        """V4: 记录选股结果（含稳定性）"""
        log_file = PROJECT_ROOT / "logs" / "picks_history_v4.json"
        
        pick_record = {
            "date": date,
            "timestamp": datetime.now().isoformat(),
            "picks": picks,
            "model_version": self.metadata.version if self.metadata else "unknown",
            "stability": asdict(stability)
        }
        
        try:
            history = []
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            history.append(pick_record)
            history = history[-100:]
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"记录选股日志失败: {e}")
    
    def calculate_trading_costs(self, turnover: float) -> float:
        """计算交易成本"""
        commission = max(turnover * self.cost_config.commission_rate, self.cost_config.min_commission)
        stamp_duty = turnover * self.cost_config.stamp_duty_rate
        slippage = turnover * self.cost_config.slippage_rate
        return commission + stamp_duty + slippage
    
    def backtest_v5(self, start_date: str, end_date: str, top_n: int = 20, 
                    hold_days: int = 10, rebalance_freq: int = 10) -> Dict:
        """回测策略 - V5优化版（延长持仓周期+止损机制）"""
        logger.info("=" * 80)
        logger.info(f"开始回测 V5: {start_date} ~ {end_date}")
        logger.info(f"持仓周期: {hold_days}天, 调仓频率: {rebalance_freq}天")
        logger.info("=" * 80)
        
        trade_dates = self.calendar.get_trade_dates(start_date, end_date)
        if len(trade_dates) < hold_days:
            logger.error("回测区间太短")
            return {}
        
        rebalance_dates = trade_dates[::rebalance_freq]
        logger.info(f"调仓次数: {len(rebalance_dates)}")
        
        # V4: 初始化资金
        capital = self.capital_config.initial_capital
        available_capital = capital * (1 - self.capital_config.reserve_ratio)
        
        all_returns = []
        all_costs = []
        detailed_records = []
        portfolio_values = [capital]  # V4: 记录组合价值
        
        for i, pick_date in enumerate(rebalance_dates):
            logger.info(f"\n调仓 {i+1}/{len(rebalance_dates)}: {pick_date}")
            logger.info(f"  当前资金: {capital:,.0f}")
            
            picks = self.pick_stocks_v4(pick_date, top_n=top_n)
            if not picks:
                logger.warning(f"  未选出股票")
                portfolio_values.append(capital)
                continue
            
            pick_codes = [p["code"] for p in picks]
            logger.info(f"  选中 {len(pick_codes)} 只")
            
            # V4: 计算每只股票可投资金额
            max_per_stock = available_capital * self.capital_config.max_position_per_stock
            stock_budget = min(max_per_stock, available_capital / len(pick_codes))
            
            future_dates = self.calendar.get_next_n_trade_dates(pick_date, hold_days)
            if len(future_dates) < hold_days + 1:
                logger.warning(f"  未来数据不足")
                portfolio_values.append(capital)
                continue
            
            hold_end_date = future_dates[hold_days]
            
            period_returns = []
            period_costs = []
            data_path = PROJECT_ROOT / "data" / "kline"
            
            for code in pick_codes:
                try:
                    stock_file = data_path / f"{code}.parquet"
                    if not stock_file.exists():
                        continue
                    
                    # V5: 加载整个持仓期间的数据以支持止损检查
                    df = pl.read_parquet(stock_file)
                    df = df.filter(
                        (pl.col("trade_date") >= pick_date) & 
                        (pl.col("trade_date") <= hold_end_date)
                    ).sort("trade_date")
                    
                    if len(df) < 2:
                        continue
                    
                    start_price = df.filter(pl.col("trade_date") == pick_date)["close"][0]
                    start_volume = df.filter(pl.col("trade_date") == pick_date)["volume"][0]
                    
                    # V4: 检查停牌和涨跌停
                    if start_volume == 0:
                        logger.debug(f"  {code} 选股日停牌")
                        continue
                    
                    # V4: 检查涨停（无法买入）
                    start_high = df.filter(pl.col("trade_date") == pick_date)["high"][0]
                    start_low = df.filter(pl.col("trade_date") == pick_date)["low"][0]
                    start_open = df.filter(pl.col("trade_date") == pick_date)["open"][0]
                    
                    if start_high == start_low and start_high > start_open * 1.095:
                        logger.debug(f"  {code} 选股日涨停，无法买入")
                        continue
                    
                    if start_price > 0:
                        # V4: 计算可买入股数（考虑最小交易单位）
                        shares = int(stock_budget / start_price / self.capital_config.lot_size) * self.capital_config.lot_size
                        if shares == 0:
                            logger.debug(f"  {code} 资金不足，无法买入")
                            continue
                        
                        actual_investment = shares * start_price
                        
                        # V5: 止损止盈检查 - 遍历持仓期间每一天
                        exit_price = None
                        exit_date = hold_end_date
                        stop_triggered = False
                        
                        for row in df.to_dicts():
                            if row['trade_date'] == pick_date:
                                continue
                            
                            current_price = row['close']
                            current_high = row['high']
                            current_low = row['low']
                            current_return = (current_price - start_price) / start_price
                            
                            # V5: 检查止损
                            if current_return <= -self.risk_config.stop_loss_pct:
                                exit_price = current_low  # 以最低价止损
                                exit_date = row['trade_date']
                                stop_triggered = True
                                logger.debug(f"  {code} 触发止损: {current_return:.2%} @ {exit_date}")
                                break
                            
                            # V5: 检查止盈
                            if current_return >= self.risk_config.stop_profit_pct:
                                exit_price = current_high  # 以最高价止盈
                                exit_date = row['trade_date']
                                stop_triggered = True
                                logger.debug(f"  {code} 触发止盈: {current_return:.2%} @ {exit_date}")
                                break
                        
                        # 如果没有触发止损止盈，使用期末价格
                        if exit_price is None:
                            exit_price = df.filter(pl.col("trade_date") == hold_end_date)["close"][0]
                            # V4: 检查跌停（无法卖出）
                            end_high = df.filter(pl.col("trade_date") == hold_end_date)["high"][0]
                            end_low = df.filter(pl.col("trade_date") == hold_end_date)["low"][0]
                            end_open = df.filter(pl.col("trade_date") == hold_end_date)["open"][0]
                            
                            if end_high == end_low and end_low < end_open * 0.905:
                                logger.debug(f"  {code} 卖出日跌停，无法卖出")
                                exit_price = end_low
                        
                        gross_ret = (exit_price - start_price) / start_price
                        
                        # V4: 计算交易成本
                        buy_cost = self.calculate_trading_costs(actual_investment)
                        sell_cost = self.calculate_trading_costs(shares * exit_price)
                        total_cost = buy_cost + sell_cost
                        
                        net_ret = gross_ret - (total_cost / actual_investment)
                        
                        period_returns.append(net_ret)
                        period_costs.append(total_cost / actual_investment)
                            
                except Exception as e:
                    logger.debug(f"  计算收益失败 {code}: {e}")
            
            if period_returns:
                avg_return = np.mean(period_returns)
                avg_cost = np.mean(period_costs)
                all_returns.append(avg_return)
                all_costs.append(avg_cost)
                
                # V4: 更新资金
                capital = capital * (1 + avg_return)
                available_capital = capital * (1 - self.capital_config.reserve_ratio)
                portfolio_values.append(capital)
                
                detailed_records.append({
                    'date': pick_date,
                    'hold_end': hold_end_date,
                    'picks': pick_codes,
                    'avg_return': avg_return,
                    'avg_cost': avg_cost,
                    'portfolio_value': capital,
                    'individual_returns': period_returns
                })
                
                logger.info(f"  持仓收益: {avg_return:.2%}, 成本: {avg_cost:.2%}, 资金: {capital:,.0f}")
            else:
                portfolio_values.append(capital)
        
        if not all_returns:
            logger.error("没有有效回测数据")
            return {}
        
        returns_array = np.array(all_returns)
        costs_array = np.array(all_costs)
        portfolio_values_array = np.array(portfolio_values)
        
        total_return = (portfolio_values[-1] - self.capital_config.initial_capital) / self.capital_config.initial_capital
        periods_per_year = 252 / hold_days
        annual_return = (portfolio_values[-1] / self.capital_config.initial_capital) ** (periods_per_year / len(returns_array)) - 1
        volatility = np.std(returns_array) * np.sqrt(periods_per_year)
        sharpe = annual_return / (volatility + 0.0001)
        
        # V4: 计算Calmar比率
        cumulative = portfolio_values_array / self.capital_config.initial_capital
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = np.min(drawdown)
        calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # V4: 计算Sortino比率
        downside_returns = returns_array[returns_array < 0]
        downside_std = np.std(downside_returns) * np.sqrt(periods_per_year) if len(downside_returns) > 0 else 0.0001
        sortino = annual_return / downside_std
        
        win_rate = np.mean(returns_array > 0)
        avg_cost = np.mean(costs_array)
        
        results = {
            'initial_capital': self.capital_config.initial_capital,
            'final_capital': float(portfolio_values[-1]),
            'total_return': float(total_return),
            'annual_return': float(annual_return),
            'volatility': float(volatility),
            'sharpe_ratio': float(sharpe),
            'max_drawdown': float(max_drawdown),
            'calmar_ratio': float(calmar),
            'sortino_ratio': float(sortino),
            'win_rate': float(win_rate),
            'avg_cost': float(avg_cost),
            'num_periods': len(returns_array),
            'avg_period_return': float(np.mean(returns_array)),
            'portfolio_values': portfolio_values_array.tolist(),
            'returns': returns_array.tolist(),
            'detailed_records': detailed_records
        }
        
        self._save_backtest_results_v4(results, start_date, end_date)
        return results
    
    def _save_backtest_results_v4(self, results: Dict, start_date: str, end_date: str):
        """V4: 保存回测结果"""
        result_file = PROJECT_ROOT / "logs" / f"backtest_v4_{start_date}_{end_date}.json"
        try:
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"回测结果已保存: {result_file}")
        except Exception as e:
            logger.warning(f"保存回测结果失败: {e}")
    
    def save_model(self, path: Path = None, version: str = None) -> bool:
        """V4: 保存模型（带版本号）"""
        try:
            import joblib
        except ImportError:
            import pickle as joblib
        
        if version:
            path = PROJECT_ROOT / "models" / f"xgboost_stock_picker_v{version}"
        else:
            path = path or (PROJECT_ROOT / "models" / "xgboost_stock_picker_v4")
        
        path.mkdir(parents=True, exist_ok=True)
        
        try:
            model_file = path / "model.joblib"
            joblib.dump(self.model, model_file)
            
            if self.metadata:
                metadata_file = path / "metadata.json"
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(asdict(self.metadata), f, ensure_ascii=False, indent=2)
            
            feature_file = path / "features.txt"
            with open(feature_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(self.feature_cols))
            
            config_file = path / "risk_config.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.risk_config), f, ensure_ascii=False, indent=2)
            
            cost_file = path / "cost_config.json"
            with open(cost_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.cost_config), f, ensure_ascii=False, indent=2)
            
            capital_file = path / "capital_config.json"
            with open(capital_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.capital_config), f, ensure_ascii=False, indent=2)
            
            logger.info(f"模型已保存到: {path}")
            return True
            
        except Exception as e:
            logger.error(f"保存模型失败: {e}")
            return False
    
    def load_model(self, path: Path = None) -> bool:
        """V4: 加载模型"""
        try:
            import joblib
        except ImportError:
            import pickle as joblib
        
        path = path or (PROJECT_ROOT / "models" / "xgboost_stock_picker_v4")
        
        try:
            model_file = path / "model.joblib"
            if not model_file.exists():
                logger.error(f"模型文件不存在: {model_file}")
                return False
            
            self.model = joblib.load(model_file)
            
            metadata_file = path / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    self.metadata = ModelMetadata(**json.load(f))
            
            feature_file = path / "features.txt"
            if feature_file.exists():
                with open(feature_file, 'r', encoding='utf-8') as f:
                    self.feature_cols = [line.strip() for line in f if line.strip()]
            
            config_file = path / "risk_config.json"
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    self.risk_config = RiskControlConfig(**json.load(f))
            
            cost_file = path / "cost_config.json"
            if cost_file.exists():
                with open(cost_file, 'r', encoding='utf-8') as f:
                    self.cost_config = TradingCostConfig(**json.load(f))
            
            capital_file = path / "capital_config.json"
            if capital_file.exists():
                with open(capital_file, 'r', encoding='utf-8') as f:
                    self.capital_config = CapitalConfig(**json.load(f))
            
            self.model_trained = True
            logger.info(f"模型已加载: {path}")
            
            if self.metadata:
                logger.info(f"模型版本: {self.metadata.version}")
                logger.info(f"IC: {self.metadata.ic_score:.4f}, IR: {self.metadata.ir_score:.4f}")
            
            return True
            
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return False
    
    def check_model_health(self) -> Dict:
        """检查模型健康状态"""
        if not self.metadata:
            return {'status': 'no_metadata', 'needs_retrain': True}
        
        train_date = datetime.fromisoformat(self.metadata.train_date)
        days_since_train = (datetime.now() - train_date).days
        
        health = {
            'status': 'healthy',
            'days_since_train': days_since_train,
            'validation_score': self.metadata.validation_score,
            'ic_score': self.metadata.ic_score,
            'ir_score': self.metadata.ir_score,
            'needs_retrain': False
        }
        
        if days_since_train > 30:
            health['status'] = 'stale'
            health['needs_retrain'] = True
        
        if self.metadata.ir_score and self.metadata.ir_score < 0.5:
            health['status'] = 'degraded'
            health['needs_retrain'] = True
        
        return health


def check_dependencies():
    """V4: 检查依赖项"""
    missing = []
    
    try:
        import xgboost
    except ImportError:
        missing.append("xgboost")
    
    try:
        import sklearn
    except ImportError:
        missing.append("scikit-learn")
    
    try:
        import portalocker
    except ImportError:
        missing.append("portalocker")
    
    try:
        import statsmodels
    except ImportError:
        missing.append("statsmodels (optional)")
    
    if missing:
        logger.error(f"缺少依赖项: {', '.join(missing)}")
        logger.error("请运行: pip install xgboost scikit-learn portalocker statsmodels")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(description='XGBoost选股策略 - V5优化版')
    parser.add_argument('--mode', type=str, default='train', choices=['train', 'pick', 'backtest', 'health'])
    parser.add_argument('--train-start', type=str, default='2024-01-01')
    parser.add_argument('--train-end', type=str, default='2025-03-01')
    parser.add_argument('--pick-date', type=str, default=None)
    parser.add_argument('--backtest-start', type=str, default='2025-03-01')
    parser.add_argument('--backtest-end', type=str, default='2025-04-13')
    parser.add_argument('--top-n', type=int, default=20)
    parser.add_argument('--max-stocks', type=int, default=None)
    parser.add_argument('--min-volume', type=float, default=10_000_000)
    parser.add_argument('--initial-capital', type=float, default=1_000_000)
    parser.add_argument('--hold-days', type=int, default=10, help='V5: 持仓天数')
    parser.add_argument('--rebalance-freq', type=int, default=10, help='V5: 调仓频率')
    parser.add_argument('--stop-loss', type=float, default=0.05, help='V5: 止损比例')
    parser.add_argument('--stop-profit', type=float, default=0.10, help='V5: 止盈比例')
    parser.add_argument('--no-risk-control', action='store_true')
    parser.add_argument('--clear-cache', action='store_true')
    parser.add_argument('--no-vif', action='store_true', help='禁用VIF特征选择')
    
    args = parser.parse_args()
    
    # V4: 检查依赖
    if not check_dependencies():
        return
    
    # V5: 使用优化后的配置
    risk_config = RiskControlConfig(
        max_stocks=args.top_n, 
        min_daily_volume=args.min_volume,
        stop_loss_pct=args.stop_loss,
        stop_profit_pct=args.stop_profit
    )
    cost_config = TradingCostConfig()
    capital_config = CapitalConfig(initial_capital=args.initial_capital)
    picker = XGBoostStockPicker(risk_config=risk_config, cost_config=cost_config, capital_config=capital_config)
    
    if args.clear_cache:
        picker.cache.clear()
    
    if args.mode == 'train':
        data = picker.load_training_data(args.train_start, args.train_end, args.max_stocks)
        if len(data) == 0:
            logger.error("未加载到训练数据")
            return
        
        data = picker.calculate_factors(data)
        data = picker.calculate_forward_returns_v4(data)
        
        if picker.train(data, validation_split=0.2, use_cv=True, use_vif=not args.no_vif):
            picker.save_model()
    
    elif args.mode == 'pick':
        if not picker.load_model():
            return
        
        pick_date = args.pick_date or datetime.now().strftime('%Y-%m-%d')
        picks = picker.pick_stocks_v4(pick_date, top_n=args.top_n, risk_control=not args.no_risk_control)
        
        print("\n" + "=" * 80)
        print(f"XGBoost选股结果 V5 ({pick_date})")
        print("=" * 80)
        print(f"{'排名':<6} {'代码':<10} {'得分':>12} {'收盘价':>10} {'成交量':>15}")
        print("-" * 80)
        for i, pick in enumerate(picks, 1):
            print(f"{i:<6} {pick['code']:<10} {pick['xgboost_score']:>12.4f} {pick['close']:>10.2f} {pick['volume']:>15.0f}")
        print("=" * 80)
    
    elif args.mode == 'backtest':
        if not picker.load_model():
            return
        
        # V5: 使用优化后的回测函数
        results = picker.backtest_v5(
            args.backtest_start, args.backtest_end, 
            top_n=args.top_n,
            hold_days=args.hold_days,
            rebalance_freq=args.rebalance_freq
        )
        
        if results:
            print("\n" + "=" * 80)
            print("XGBoost选股策略回测结果 V5 (优化版)")
            print("=" * 80)
            print(f"回测区间: {args.backtest_start} ~ {args.backtest_end}")
            print(f"持仓周期: {args.hold_days}天, 调仓频率: {args.rebalance_freq}天")
            print(f"止损线: {args.stop_loss:.1%}, 止盈线: {args.stop_profit:.1%}")
            print(f"选股数量: {args.top_n} 只")
            print(f"初始资金: {results['initial_capital']:,.0f}")
            print(f"最终资金: {results['final_capital']:,.0f}")
            print(f"总收益率: {results['total_return']:.2%}")
            print(f"年化收益率: {results['annual_return']:.2%}")
            print(f"波动率: {results['volatility']:.2%}")
            print(f"夏普比率: {results['sharpe_ratio']:.2f}")
            print(f"最大回撤: {results['max_drawdown']:.2%}")
            print(f"Calmar比率: {results['calmar_ratio']:.2f}")
            print(f"Sortino比率: {results['sortino_ratio']:.2f}")
            print(f"胜率: {results['win_rate']:.2%}")
            print(f"平均成本: {results['avg_cost']:.2%}")
            print(f"交易次数: {results['num_periods']}")
            print("=" * 80)
    
    elif args.mode == 'health':
        if not picker.load_model():
            return
        
        health = picker.check_model_health()
        print("\n" + "=" * 80)
        print("模型健康检查")
        print("=" * 80)
        for key, value in health.items():
            print(f"{key}: {value}")
        print("=" * 80)


if __name__ == "__main__":
    main()
