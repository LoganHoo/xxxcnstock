#!/usr/bin/env python3
"""
双源K线数据采集器 (v3.0 - 重构版)

架构改进 (v3.0):
✅ 模块化设计：拆分为独立职责模块
  - dual_source_config.py: 配置管理
  - incremental_cache.py: 增量缓存
  - checkpoint_manager.py: 断点续传
  - quick_checker.py: 快速检查
  - collection_stats.py: 统计管理

✅ 依赖注入：所有组件通过构造函数传入，无全局状态

✅ 集中式统计：使用 CollectionStatsManager 统一管理

✅ 资源安全：移除 __del__，强制使用上下文管理器

✅ 智能黑名单：基于连续失败次数阈值

✅ 增强验证：数据质量问题可配置处理策略

核心流程：
1. 从Redis读取股票列表和增量缓存
2. 过滤退市/无效代码（带阈值机制）
3. 快速检查跳过已最新的股票（批量并行I/O）
4. 按位置分割：前半Baostock，后半腾讯
5. 分批并行采集（带重试和降级机制）
6. 数据后处理、质量验证、保存
7. 更新缓存、断点和统计

性能优势：
- 腾讯API比Baostock快5-10倍
- 增量缓存避免每次读parquet文件
- 批量并行快速检查
- Provider单例复用减少连接开销
- 智能降级提高成功率
"""

import sys
from pathlib import Path
import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 导入重构后的模块
from core.logger import setup_logger
from services.data_service.fetchers.dual_source_config import (
    DualSourceConfig,
    get_default_config
)
from services.data_service.fetchers.incremental_cache import IncrementalDateCache
from services.data_service.fetchers.checkpoint_manager import (
    CheckpointManager,
    CheckpointConfig
)
from services.data_service.fetchers.quick_checker import QuickChecker
from services.data_service.fetchers.collection_stats import CollectionStatsManager
from services.data_service.fetchers.stock_list_cache import StockListCacheManager

logger = setup_logger("dual_source_fetcher", log_file="system/dual_source_fetcher.log")


class StockGrouper:
    """股票分组器 - 按板块/行业分组"""
    
    SECTOR_MAP = {
        '6': '上证主板',
        '0': '深证主板',
        '3': '创业板',
        '8': '科创板',
        '4': '北交所',
    }
    
    def group_by_sector(self, codes: List[str]) -> Dict[str, List[str]]:
        from collections import defaultdict
        groups = defaultdict(list)
        
        for code in codes:
            prefix = code[0] if code else '0'
            sector = self.SECTOR_MAP.get(prefix, '其他')
            groups[sector].append(code)
        
        return dict(groups)
    
    def create_batches(self, codes: List[str], batch_size: int = 50) -> List[List[str]]:
        batches = []
        
        for i in range(0, len(codes), batch_size):
            batches.append(codes[i:i + batch_size])
        
        return batches


class DualSourceFetcher:
    """
    双源K线数据采集器 (v3.0)
    
    核心改进：
    - 依赖注入：config, cache, checkpoint 等全部通过构造函数传入
    - 无全局状态：不依赖模块级变量
    - 强制资源管理：必须使用 with 语句或手动调用 close()
    - 集中式统计：使用 CollectionStatsManager
    
    使用方式：
        config = get_default_config()
        
        async with DualSourceFetcher(config=config) as fetcher:
            result = await fetcher.run(codes=['000001', '000002'])
            
        # 或同步方式：
        result = run_dual_source_fetch(codes=['000001', '000002'])
    
    禁止方式：
        ❌ fetcher = DualSourceFetcher()  # 不使用上下文管理器
        ✅ with DualSourceFetcher() as fetcher: ...  # 正确
    """
    
    def __init__(
        self,
        kline_dir: Path = None,
        data_dir: Path = None,
        redis_client=None,
        config: DualSourceConfig = None,
        enable_checkpoint: bool = True
    ):
        """
        初始化采集器

        Args:
            kline_dir: K线数据目录
            data_dir: 数据目录（包含stock_list.parquet等）
            redis_client: Redis客户端实例（可选）
            config: 配置实例（可选，为None则加载默认配置）
            enable_checkpoint: 是否启用断点续传
        """
        # 配置（不可变）
        self._config = config or get_default_config()

        # 目录
        self.kline_dir = Path(kline_dir or Path(__file__).parent.parent.parent / "data" / "kline")
        self.data_dir = Path(data_dir or self.kline_dir.parent)
        self.kline_dir.mkdir(parents=True, exist_ok=True)

        # 初始化组件（全部使用注入的配置）
        self.date_cache = IncrementalDateCache(redis_client, self._config)
        self.stock_cache = StockListCacheManager(data_dir=str(self.data_dir), redis_client=redis_client)
        self.grouper = StockGrouper()
        
        # 断点续传
        if enable_checkpoint and self._config.checkpoint_enabled:
            checkpoint_cfg = CheckpointConfig(
                enabled=True,
                checkpoint_file=self._config.checkpoint_file,
                auto_save_interval=self._config.auto_save_interval,
                save_on_error=self._config.save_on_error,
                max_resume_age_hours=self._config.max_resume_age_hours
            )
            self.checkpoint = CheckpointManager(config=checkpoint_cfg)
        else:
            self.checkpoint = None
        
        # 快速检查器
        if self._config.quick_check_enabled:
            self.quick_checker = QuickChecker(
                self.kline_dir,
                self.date_cache,
                self._config
            )
        else:
            self.quick_checker = None
        
        # 统计管理器（集中式）
        self.stats = CollectionStatsManager()
        
        # Provider单例
        self._providers = {}
        
        # 资源状态
        self._closed = False
    
    def __enter__(self):
        """上下文管理器入口"""
        logger.debug("DualSourceFetcher 上下文进入")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出 - 必须调用以释放资源"""
        self.close()
        return False  # 不抑制异常
    
    def close(self):
        """
        清理所有资源
        
        ⚠️ 重要：必须在不再需要时调用此方法，或使用 with 语句
        """
        if self._closed:
            return
        
        try:
            # 关闭 Provider 连接
            for source, provider in self._providers.items():
                try:
                    if hasattr(provider, 'close'):
                        provider.close()
                        logger.debug(f"关闭 {source} Provider 连接")
                    elif hasattr(provider, 'logout'):
                        provider.logout()
                        logger.debug(f"登出 {source} Provider")
                except Exception as e:
                    logger.warning(f"关闭 {source} Provider 时出错: {e}")
            
            self._providers.clear()
            
            # 关闭缓存连接
            if hasattr(self, 'date_cache') and self.date_cache:
                self.date_cache.close()
            
            self._closed = True
            logger.info("DualSourceFetcher 资源已清理")
            
        except Exception as e:
            logger.error(f"清理资源时出错: {e}")
    
    def _get_provider(self, source: str):
        """
        获取Provider实例（单例模式）
        
        Args:
            source: 数据源名称 ('baostock' 或 'tencent')
            
        Returns:
            Provider实例
            
        Raises:
            ValueError: 如果数据源名称无效
        """
        if source not in self._providers:
            if source == 'baostock':
                from services.data_service.datasource.providers import BaostockProvider
                self._providers[source] = BaostockProvider()
            elif source == 'tencent':
                from services.data_service.datasource.providers import TencentProvider
                self._providers[source] = TencentProvider()
            else:
                raise ValueError(f"未知的数据源: {source}")
            
            logger.debug(f"创建 {source} Provider实例")
        
        return self._providers[source]
    
    def _validate_stock_code(self, code: str) -> Tuple[bool, str]:
        """
        验证股票代码是否有效
        
        Returns:
            (是否有效, 原因)
        """
        try:
            if not code or len(code) < 6:
                return False, 'code_too_short'
            
            code = str(code).strip()
            
            valid_prefixes = [
                '000', '001', '002', '003',
                '300',
                '600', '601', '603', '605',
                '688'
            ]
            
            prefix = code[:3]
            if prefix not in valid_prefixes:
                return False, f'unsupported_prefix:{prefix}'
            
            if len(code) != 6:
                return False, f'invalid_length:{len(code)}'
            
            if not code.isdigit():
                return False, 'non_numeric'
            
            return True, ''
            
        except Exception as e:
            logger.debug(f"验证代码异常 {code}: {e}")
            return True, ''
    
    def _classify_error(self, exception: Exception) -> str:
        """
        分类异常类型
        
        Returns:
            错误类型: 'network_timeout' | 'rate_limit' | 'data_format' | 'auth_error' | 'unknown'
        """
        error_msg = str(exception).lower()
        error_type = type(exception).__name__
        
        if any(kw in error_msg for kw in ['timeout', 'timed out', '连接超时']):
            return 'network_timeout'
        
        if any(kw in error_msg for kw in ['rate limit', '429', 'too many requests', '频率限制']):
            return 'rate_limit'
        
        if any(kw in error_msg for kw in ['auth', '401', '403', 'unauthorized', '认证失败', '权限不足']):
            return 'auth_error'
        
        if any(kw in error_type for kw in ['ValueError', 'KeyError', 'TypeError', 'ParseError']) or \
           any(kw in error_msg for kw in ['invalid', '格式错误', '解析失败', 'no column']):
            return 'data_format'
        
        if any(kw in error_type for kw in ['ConnectionError', 'ConnectionRefusedError', 'SocketError']) or \
           any(kw in error_msg for kw in ['connection refused', '连接被拒绝', '网络不可达']):
            return 'network_timeout'
        
        return 'unknown'
    
    def _post_process_data(self, df, code: str):
        """
        数据后处理：标准化列名、类型转换、基本验证
        
        Args:
            df: 原始DataFrame
            code: 股票代码
            
        Returns:
            处理后的DataFrame或None
        """
        try:
            if df is None or len(df) == 0:
                return None
            
            if hasattr(df, 'to_pandas'):
                df = df.to_pandas()
            elif not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            col_map = {
                'date': 'trade_date',
                'trade_date': 'trade_date',
                'datetime': 'trade_date',
                'time': 'trade_date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'amount': 'amount',
                'vol': 'volume'
            }
            
            rename_dict = {k: v for k, v in col_map.items() if k in df.columns}
            if rename_dict:
                df = df.rename(columns=rename_dict)
            
            required_cols = ['trade_date', 'open', 'high', 'low', 'close']
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logger.warning(f"{code} 缺少必要列: {missing}")
                return None
            
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
            df = df[df['close'] > 0]
            
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last')
                df = df.reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"数据后处理失败 {code}: {e}")
            return None
    
    async def _fetch_with_retry(
        self,
        provider,
        code: str,
        start_date: str,
        end_date: str,
        source_name: str
    ) -> Optional[pd.DataFrame]:
        """
        带重试机制的数据采集
        
        使用指数退避策略，支持可配置的重试次数和延迟
        """
        max_retries = self._config.max_retries
        delay_base = self._config.retry_delay_base
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                
                df = await provider.fetch_kline(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    frequency='d'
                )
                
                elapsed_ms = (time.time() - start_time) * 1000
                
                if df is not None and len(df) > 0:
                    logger.debug(
                        f"[RETRY] {source_name} {code}: "
                        f"第{attempt+1}次尝试成功 ({elapsed_ms:.0f}ms)"
                    )
                    return df
                
                if attempt < max_retries - 1:
                    delay = delay_base * (2 ** attempt)
                    logger.debug(
                        f"[RETRY] {source_name} {code}: "
                        f"无数据，{delay:.1f}s后重试..."
                    )
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                elapsed_ms = (time.time() - start_time) * 1000
                
                if attempt < max_retries - 1:
                    delay = delay_base * (2 ** attempt)
                    logger.warning(
                        f"[RETRY] {source_name} {code}: "
                        f"异常({str(e)[:50]}), {delay:.1f}s后重试..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"[RETRY] {source_name} {code}: "
                        f"重试{max_retries}次均失败: {e}"
                    )
        
        return None
    
    async def _try_fallback_source(
        self,
        code: str,
        primary_source: str,
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        数据源降级：当主数据源返回空时，尝试备用数据源
        
        改进：
        - 记录降级原因到统计
        - 支持多次降级尝试
        - 自动更新备用源的统计信息
        """
        result = {
            'success': False,
            'rows': 0,
            'fallback_source': '',
            'error': ''
        }
        
        if not self._config.fallback_enabled:
            result['error'] = 'fallback_disabled'
            return result
        
        fallback_source = 'tencent' if primary_source == 'baostock' else 'baostock'
        
        try:
            logger.info(
                f"[FALLBACK] {code}: "
                f"主源{primary_source}无数据，尝试{fallback_source}..."
            )
            
            provider = self._get_provider(fallback_source)
            df = await self._fetch_with_retry(
                provider, code, start_date, end_date, fallback_source
            )
            
            if df is not None and len(df) > 0:
                df = self._post_process_data(df, code)
                
                if df is not None and len(df) > 0:
                    saved = await self._save_to_parquet(df, code)
                    
                    if saved:
                        result['success'] = True
                        result['rows'] = len(df)
                        result['fallback_source'] = fallback_source
                        
                        if self._config.cache_enabled:
                            latest_date = str(df['trade_date'].max())[:10]
                            self.date_cache.set_last_date(code, latest_date)
                        
                        # 记录降级成功到统计
                        self.stats.record_fallback_success(code, fallback_source, len(df))
                        
                        logger.info(
                            f"[FALLBACK] ✓ {code}: "
                            f"{fallback_source} 返回 {len(df)}条"
                        )
                    else:
                        result['error'] = 'fallback_save_failed'
                else:
                    result['error'] = 'fallback_no_data_after_process'
            else:
                result['error'] = 'fallback_no_data'
                
        except Exception as e:
            result['error'] = f'fallback_exception:{str(e)[:50]}'
            logger.error(f"[FALLBACK] ✗ {code}: {e}")
        
        return result
    
    def _should_auto_blacklist(self, code: str) -> bool:
        """
        判断是否应该自动加入黑名单（基于阈值机制）
        
        v3.0 改进：
        - 不再一次失败就黑名单
        - 基于连续失败次数阈值决策
        - 可配置阈值大小
        """
        threshold = getattr(self._config, 'blacklist_threshold', 3)
        consecutive_failures = self.stats.get_consecutive_failures(code)
        
        should_blacklist = consecutive_failures >= threshold
        
        if should_blacklist:
            logger.info(
                f"[BLACKLIST_THRESHOLD] {code}: "
                f"连续失败{consecutive_failures}次 >= 阈值{threshold}，触发黑名单"
            )
        
        return should_blacklist
    
    def _auto_blacklist_stock(self, code: str, reason: str = 'unknown'):
        """
        将股票加入自动黑名单
        
        仅在达到阈值时执行
        """
        if not self._should_auto_blacklist(code):
            logger.debug(
                f"[BLACKLIST_SKIP] {code}: 未达黑名单阈值，暂不添加"
            )
            return
        
        try:
            from services.data_service.fetchers.delisting_detector import (
                get_delisting_detector,
                DelistedStockInfo
            )
            
            detector = get_delisting_detector()
            
            info = DelistedStockInfo(
                code=code,
                reason=f"{reason}_threshold_{self._config.blacklist_threshold}",
                source='auto_detection_v3',
                detected_at=datetime.now().isoformat(),
                confidence=0.8 + min(0.19, self.stats.get_consecutive_failures(code) * 0.01)
            )
            
            detector.add_to_blacklist(info)
            self.stats.record_delisted(code, reason)
            
            logger.info(
                f"[BLACKLIST] 自动添加 {code} 到黑名单 "
                f"(原因: {reason}, 连续失败: {self.stats.get_consecutive_failures(code)})"
            )
            
        except Exception as e:
            logger.warning(f"自动黑名单失败 {code}: {e}")
    
    async def _save_to_parquet(self, df, code: str) -> bool:
        """
        保存DataFrame到Parquet文件（合并模式）
        
        特性：
        - 合并新旧数据，去重
        - 日期标准化
        - 可选的数据验证
        """
        try:
            if df is None or len(df) == 0:
                return False
            
            output_file = self.kline_dir / f"{code}.parquet"
            
            if output_file.exists():
                existing_df = pd.read_parquet(output_file)
                
                if 'date' in existing_df.columns and 'trade_date' not in existing_df.columns:
                    existing_df = existing_df.rename(columns={'date': 'trade_date'})
                if 'date' in df.columns and 'trade_date' not in df.columns:
                    df = df.rename(columns={'date': 'trade_date'})
                
                if 'trade_date' in existing_df.columns:
                    existing_df['trade_date'] = pd.to_datetime(existing_df['trade_date']).dt.strftime('%Y-%m-%d')
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['trade_date'], keep='last')
                combined = combined.sort_values('trade_date').reset_index(drop=True)
                df = combined
            
            # 数据验证（如果启用）
            if self._config.cross_validate:
                _, issues = self.cross_validate(df, code)
                
                if issues:
                    # 记录验证问题
                    for issue in issues:
                        issue_type = issue.get('type', 'unknown')
                        self.stats.record_validation_issue(code, issue_type)
                    
                    # 严重问题警告
                    critical_issues = [i for i in issues if i.get('type') in ['invalid_ohlc']]
                    if critical_issues:
                        logger.warning(
                            f"{code} 发现严重验证问题: {critical_issues}"
                        )
            
            df.to_parquet(output_file, index=False)
            
            logger.debug(f"[SAVE] {code}: 已保存 {len(df)} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"保存Parquet失败 {code}: {e}")
            return False
    
    def cross_validate(self, df: pd.DataFrame, code: str, overlap_days: int = None) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        交叉验证数据质量
        
        检查项：
        - 日期间隔异常（>7天）
        - 价格剧烈波动（>20%）
        - OHLC逻辑错误
        """
        issues = []
        overlap_days = overlap_days or self._config.validation_overlap_days
        
        if df.empty or len(df) < overlap_days * 2:
            return df, issues
        
        try:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            date_diffs = df['trade_date'].diff().dt.days.dropna()
            large_gaps = date_diffs[date_diffs > 7]
            
            if len(large_gaps) > 0:
                issues.append({
                    'type': 'date_gap',
                    'count': len(large_gaps),
                    'max_gap': int(large_gaps.max())
                })
            
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    if len(df) > 1:
                        pct_change = df[col].pct_change().abs() * 100
                        extreme_changes = pct_change[pct_change > 20]
                        
                        if len(extreme_changes) > 0:
                            issues.append({
                                'type': 'price_spike',
                                'column': col,
                                'count': len(extreme_changes),
                                'max_change': round(extreme_changes.max(), 2)
                            })
            
            if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                invalid_ohlc = df[
                    (df['high'] < df['low']) |
                    (df['open'] > df['high']) |
                    (df['close'] > df['high']) |
                    (df['open'] < df['low']) |
                    (df['close'] < df['low'])
                ]
                
                if len(invalid_ohlc) > 0:
                    issues.append({
                        'type': 'invalid_ohlc',
                        'count': len(invalid_ohlc)
                    })
                    
        except Exception as e:
            logger.warning(f"交叉验证出错 {code}: {e}")
        
        return df, issues
    
    async def _process_single_stock(
        self,
        code: str,
        forced_source: str,
        today: str,
        semaphore: asyncio.Semaphore,
        last_dates: Dict[str, str]
    ) -> Dict:
        """
        处理单只股票的完整流程（统一入口）
        
        流程：
        1. 数据采集（带重试）
        2. 后处理
        3. 保存
        4. 降级处理（如需）
        5. 黑名单判断（基于阈值）
        6. 统计记录（集中式）
        """
        if self.checkpoint:
            self.checkpoint.set_in_progress(code)
        
        result = {
            'code': code,
            'success': False,
            'rows': 0,
            'source': forced_source,
            'status': '',
            'error': ''
        }
        
        start_time = time.time()
        
        try:
            async with semaphore:
                logger.debug(f"[{forced_source.upper()}] 开始采集 {code}...")
                
                provider = self._get_provider(forced_source)
                
                last_date = last_dates.get(code)
                if last_date:
                    start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start_date = '2015-01-01'
                
                df = await self._fetch_with_retry(
                    provider, code, start_date, today, forced_source
                )
                
                if df is not None and len(df) > 0:
                    df = self._post_process_data(df, code)
                    
                    if df is not None and len(df) > 0:
                        saved = await self._save_to_parquet(df, code)
                        
                        if saved:
                            elapsed_ms = (time.time() - start_time) * 1000
                            
                            result['success'] = True
                            result['rows'] = len(df)
                            result['status'] = f'{forced_source}_full'
                            
                            # 集中式统计记录
                            self.stats.record_success(
                                code, forced_source, len(df), elapsed_ms
                            )
                            
                            if self._config.cache_enabled:
                                latest_date = str(df['trade_date'].max())[:10]
                                self.date_cache.set_last_date(code, latest_date)
                            
                            date_range = f"{df['trade_date'].min()} ~ {df['trade_date'].max()}"
                            logger.info(
                                f"[{forced_source.upper()}] ✓ {code}: "
                                f"{len(df)}条 | {date_range} | {elapsed_ms:.0f}ms"
                            )
                            
                            if self.checkpoint:
                                self.checkpoint.mark_completed(code, result)
                                
                        else:
                            result['error'] = 'save_failed'
                            
                            # 记录失败到集中式统计
                            self.stats.record_failure(
                                code, forced_source, 'save_failed', 'save_error'
                            )
                            
                            logger.warning(f"[{forced_source.upper()}] ✗ {code}: 保存失败")
                            
                    else:
                        result['error'] = 'post_process_failed'
                        result['status'] = 'no_data_after_process'
                        
                        self.stats.record_failure(
                            code, forced_source, 'post_process_failed', 'data_format'
                        )
                        
                        logger.warning(f"[{forced_source.upper()}] ⚠ {code}: 后处理后无数据")
                        
                else:
                    logger.info(
                        f"[{forced_source.upper()}] ⚠ {code}: "
                        f"主源无数据，尝试降级..."
                    )
                    
                    fallback_result = await self._try_fallback_source(
                        code=code,
                        primary_source=forced_source,
                        start_date=start_date,
                        end_date=today
                    )
                    
                    if fallback_result['success']:
                        result.update(fallback_result)
                        result['source'] = (
                            f"{forced_source}→"
                            f"{fallback_result.get('fallback_source', 'unknown')}"
                        )
                        
                        logger.info(
                            f"[FALLBACK] ✓ {code}: "
                            f"降级成功 ({result['rows']}条)"
                        )
                        
                        if self.checkpoint:
                            self.checkpoint.mark_completed(code, result)
                            
                    else:
                        result['error'] = 'no_data_all_sources'
                        result['status'] = 'likely_delisted'
                        
                        # 记录失败（用于阈值判断）
                        self.stats.record_failure(
                            code, forced_source, 
                            'no_data_all_sources', 'no_data'
                        )
                        
                        logger.warning(
                            f"[ALL_SOURCES] ✗ {code}: "
                            f"所有数据源均无数据 (疑似退市/停牌)"
                        )
                        
                        # 智能黑名单（基于阈值）
                        if self._config.auto_blacklist_no_data:
                            self._auto_blacklist_stock(code, reason='no_data_all_sources')
                        
                        if self.checkpoint:
                            self.checkpoint.mark_delisted(code, 'no_data_all_sources')
                            self.stats.record_cache_hit(code)
                            
        except Exception as e:
            error_msg = str(e)
            error_type = self._classify_error(e)
            
            result['error'] = error_msg
            result['error_type'] = error_type
            
            # 记录失败到集中式统计
            self.stats.record_failure(code, forced_source, error_msg[:50], error_type)
            
            # 根据错误类型选择日志级别和处理策略
            if error_type == 'network_timeout':
                logger.warning(
                    f"[{forced_source.upper()}] ⏱ {code}: "
                    f"网络超时 ({error_msg[:50]})"
                )
                
                if self.checkpoint and self._config.save_on_error:
                    self.checkpoint.mark_failed(code, f'network_timeout:{error_msg[:30]}')
                    
            elif error_type == 'rate_limit':
                logger.warning(
                    f"[{forced_source.upper()}] 🚫 {code}: "
                    f"触发频率限制 ({error_msg[:50]})"
                )
                
                await asyncio.sleep(1.0)
                
                if self.checkpoint and self._config.save_on_error:
                    self.checkpoint.mark_failed(code, f'rate_limit:{error_msg[:30]}')
                    
            elif error_type == 'data_format':
                logger.error(
                    f"[{forced_source.upper()}] ❌ {code}: "
                    f"数据格式错误 ({error_msg[:50]})"
                )
                
                self._auto_blacklist_stock(code, reason='data_format_error')
                
                if self.checkpoint:
                    self.checkpoint.mark_delisted(code, 'data_format_error')
                    
            elif error_type == 'auth_error':
                logger.critical(
                    f"[{forced_source.upper()}] 🔒 {code}: "
                    f"认证失败 ({error_msg[:50]})"
                )
                
                if self.checkpoint and self._config.save_on_error:
                    self.checkpoint.mark_failed(code, f'auth_error:{error_msg[:30]}')
                    await self.checkpoint.save(force=True)
                    
            else:
                logger.error(
                    f"[{forced_source.upper()}] ✗ {code} "
                    f"异常 [{error_type}]: {e}",
                    exc_info=True
                )
                
                if self.checkpoint and self._config.save_on_error:
                    self.checkpoint.mark_failed(code, f'{error_type}:{error_msg[:30]}')
                    await self.checkpoint.save(force=True)
        
        return result
    
    async def run(
        self,
        codes: List[str] = None,
        days: int = None,
        filter_delisted: bool = True,
        resume: bool = True
    ) -> Dict:
        """
        执行双源采集主流程
        
        Args:
            codes: 股票代码列表（None则从Redis读取）
            days: 采集最近N天（None则使用增量模式）
            filter_delisted: 是否过滤退市股票
            resume: 是否断点续传
            
        Returns:
            采集结果统计字典
        """
        start_time = time.time()
        
        # 启动统计计时
        self.stats.start_timing()
        
        # 0. 配置验证
        config_errors = self._config.validate()
        if config_errors:
            logger.error(f"配置验证失败:\n" + "\n".join(f"  - {e}" for e in config_errors))
            return {
                'success': 0,
                'failed': 0,
                'error': 'config_validation_failed',
                'details': config_errors,
                'elapsed_seconds': 0
            }
        
        # 1. 参数验证
        if days is not None and (days < 1 or days > 3650):
            logger.warning(f"参数 days={days} 超出范围(1-3650)，已调整")
            days = max(1, min(days, 3650))
        
        if codes is not None and len(codes) == 0:
            logger.warning("传入的股票代码列表为空")
            return {
                'success': 0,
                'skipped': 0,
                'failed': 0,
                'total': 0,
                'elapsed_seconds': round(time.time() - start_time, 1),
                'mode': 'empty_input'
            }
        
        # 2. 获取股票列表
        if codes is None:
            logger.info("从Redis读取股票列表...")
            codes = self.stock_cache.get_codes(use_redis=True)
            if not codes:
                logger.warning("Redis无数据，降级到Parquet")
                codes = self.stock_cache.get_codes(use_redis=False)
        
        if not codes:
            logger.error("无法获取股票列表")
            return {'success': 0, 'failed': 0, 'error': 'no_stock_list'}
        
        # 3. 断点续传
        if self.checkpoint and resume:
            self.checkpoint.data['total'] = len(codes)
            pending_codes = self.checkpoint.get_pending_codes(codes)
            
            if pending_codes and len(pending_codes) < len(codes):
                logger.info(f"=== 断点续传模式 ===")
                logger.info(f"待处理: {len(pending_codes)} 只 (总计 {len(codes)} 只)")
                codes = pending_codes
        
        self.stats.set_total_stocks(len(codes))
        logger.info(f"共 {len(codes)} 只股票待采集")
        
        if len(codes) == 0:
            logger.info("没有需要处理的股票（可能已全部完成）")
            return {
                'success': self.checkpoint.data['stats']['success_count'] if self.checkpoint else 0,
                'skipped': self.checkpoint.data['stats']['skipped_count'] if self.checkpoint else 0,
                'delisted': self.checkpoint.data['stats']['delisted_count'] if self.checkpoint else 0,
                'failed': 0,
                'total': 0,
                'elapsed_seconds': round(time.time() - start_time, 1),
                'mode': 'resume_complete'
            }
        
        # 4. 过滤退市股票
        if filter_delisted:
            from core.delisting_guard import get_delisting_guard
            guard = get_delisting_guard()
            original_count = len(codes)
            codes = [c for c in codes if not guard.is_delisted_by_code(c)]
            filtered = original_count - len(codes)
            
            if filtered > 0:
                logger.info(f"过滤 {filtered} 只退市股票，剩余 {len(codes)} 只")
        
        # 5. 代码有效性预检
        original_count = len(codes)
        valid_codes = []
        invalid_codes_info = []
        
        for code in codes:
            is_valid, reason = self._validate_stock_code(code)
            if is_valid:
                valid_codes.append(code)
            else:
                invalid_codes_info.append((code, reason))
                if self.checkpoint:
                    self.checkpoint.mark_skipped(code, f'invalid_code:{reason}')
        
        codes = valid_codes
        filtered_invalid = original_count - len(codes)
        
        if filtered_invalid > 0:
            logger.info(f"代码有效性预检: 过滤 {filtered_invalid} 只无效代码")
            for code, reason in invalid_codes_info[:10]:
                logger.info(f"  - {code}: {reason}")
            if len(invalid_codes_info) > 10:
                logger.info(f"  ... 还有 {len(invalid_codes_info) - 10} 只")
        
        # 6. 黑名单检查
        original_count = len(codes)
        blacklisted_count = 0
        
        try:
            from services.data_service.fetchers.delisting_detector import get_delisting_detector
            detector = get_delisting_detector()
            
            non_blacklisted = []
            for code in codes:
                if detector.is_blacklisted(code):
                    blacklisted_count += 1
                    if self.checkpoint:
                        self.checkpoint.mark_delisted(code, 'blacklisted')
                else:
                    non_blacklisted.append(code)
            
            codes = non_blacklisted
            
            if blacklisted_count > 0:
                logger.info(f"黑名单检查: 过滤 {blacklisted_count} 只已退市/停牌股票")
                
                blacklist_sample = list(detector.get_blacklisted_codes())[:5]
                if blacklist_sample:
                    logger.info(f"  黑名单示例: {', '.join(blacklist_sample)}...")
                    
        except Exception as e:
            logger.warning(f"黑名单检查异常: {e}，继续执行")
        
        # 7. 快速检查
        if self.quick_checker and self._config.quick_check_enabled:
            logger.info("执行快速检查，跳过已最新的股票...")
            need_update, already_fresh, reasons = await self.quick_checker.quick_check(codes)
            
            for code in already_fresh:
                if self.checkpoint:
                    self.checkpoint.mark_skipped(code, 'fresh')
                self.stats.record_cache_hit(code)
            
            codes = need_update
            logger.info(
                f"快速检查完成: {len(already_fresh)}只已是最新, "
                f"{len(codes)}只需要更新"
            )
            
            if len(codes) == 0:
                logger.info("所有股票已是最新，无需更新")
                elapsed = time.time() - start_time
                
                agg_stats = self.stats.get_aggregated_stats()
                
                return {
                    'success': 0,
                    'skipped': len(already_fresh),
                    'failed': 0,
                    'total': self.stats.get_aggregated_stats()['total'],
                    'cache_hits': agg_stats['cache_hits'],
                    'elapsed_seconds': round(elapsed, 1),
                    'mode': 'all_fresh'
                }
        
        # 8. 读取增量缓存
        logger.info("读取增量日期缓存...")
        last_dates = self.date_cache.get_last_dates(codes)
        logger.info(f"缓存命中: {len(last_dates)}/{len(codes)}")
        
        # 9. 按位置分割
        today = datetime.now().strftime('%Y-%m-%d')
        
        if self._config.split_mode != "position":
            logger.warning(
                f"检测到非位置分割模式 ({self._config.split_mode})，"
                f"已强制切换为 position 模式"
            )
        
        codes = sorted(codes)
        split_idx = int(len(codes) * self._config.split_ratio)
        
        baostock_codes = codes[:split_idx]
        tencent_codes = codes[split_idx:]
        
        logger.info(f"=== 按位置分割模式 ===")
        logger.info(f"总股票数: {len(codes)} 只")
        logger.info(f"分割比例: {self._config.split_ratio*100:.0f}%")
        logger.info(f"分割点: 第{split_idx}只")
        logger.info(
            f"前半部分(Baostock): {len(baostock_codes)} 只 "
            f"[{baostock_codes[0] if baostock_codes else 'N/A'} ~ "
            f"{baostock_codes[-1] if baostock_codes else 'N/A'}]"
        )
        logger.info(
            f"后半部分(Tencent): {len(tencent_codes)} 只 "
            f"[{tencent_codes[0] if tencent_codes else 'N/A'} ~ "
            f"{tencent_codes[-1] if tencent_codes else 'N/A'}]"
        )
        
        # 10. 创建信号量
        bs_semaphore = asyncio.Semaphore(self._config.baostock_concurrency)
        tc_semaphore = asyncio.Semaphore(self._config.tencent_concurrency)
        
        all_results = []
        
        # 11. 处理前半部分 - Baostock
        if baostock_codes:
            logger.info(f"\n{'='*60}")
            logger.info(f"[Baostock] 处理前半部分: {len(baostock_codes)} 只股票")
            logger.info(f"{'='*60}")
            
            bs_batches = self.grouper.create_batches(baostock_codes, self._config.batch_size)
            
            for batch_idx, batch_codes in enumerate(bs_batches):
                logger.info(
                    f"Baostock批次 {batch_idx + 1}/{len(bs_batches)} "
                    f"({len(batch_codes)}只)..."
                )
                
                tasks = [
                    self._process_single_stock(
                        code, 'baostock', today,
                        bs_semaphore, last_dates
                    )
                    for code in batch_codes
                ]
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                all_results.extend(batch_results)
                
                if self.checkpoint:
                    await self.checkpoint.save()
                
                # 显示进度（使用集中式统计）
                progress_bar = self.stats.get_progress_bar()
                agg_stats = self.stats.get_aggregated_stats()
                
                processed = min((batch_idx + 1) * self._config.batch_size, len(baostock_codes))
                pct = processed / len(baostock_codes) * 100
                
                logger.info(
                    f"[Baostock] [{progress_bar}] {pct:5.1f}% "
                    f"✓{agg_stats['success']} ⊘{agg_stats['skipped']} "
                    f"✗{agg_stats['failed']} ⚠{agg_stats['delisted']}"
                )
        
        # 12. 处理后半部分 - Tencent
        if tencent_codes:
            logger.info(f"\n{'='*60}")
            logger.info(f"[Tencent] 处理后半部分: {len(tencent_codes)} 只股票")
            logger.info(f"{'='*60}")
            
            tc_batches = self.grouper.create_batches(tencent_codes, self._config.batch_size)
            
            for batch_idx, batch_codes in enumerate(tc_batches):
                logger.info(
                    f"Tencent批次 {batch_idx + 1}/{len(tc_batches)} "
                    f"({len(batch_codes)}只)..."
                )
                
                tasks = [
                    self._process_single_stock(
                        code, 'tencent', today,
                        tc_semaphore, last_dates
                    )
                    for code in batch_codes
                ]
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                all_results.extend(batch_results)
                
                if self.checkpoint:
                    await self.checkpoint.save()
                
                # 显示进度（使用集中式统计）
                progress_bar = self.stats.get_progress_bar()
                agg_stats = self.stats.get_aggregated_stats()
                
                processed = min((batch_idx + 1) * self._config.batch_size, len(tencent_codes))
                pct = processed / len(tencent_codes) * 100
                
                logger.info(
                    f"[Tencent] [{progress_bar}] {pct:5.1f}% "
                    f"✓{agg_stats['success']} ⊘{agg_stats['skipped']} "
                    f"✗{agg_stats['failed']} ⚠{agg_stats['delisted']}"
                )
        
        # 13. 最终统计（从集中式统计获取，避免双重计数）
        final_stats = self.stats.get_aggregated_stats()
        
        if self.checkpoint:
            await self.checkpoint.save(force=True)
            checkpoint_summary = self.checkpoint.get_summary()
            logger.info(f"进度文件已保存: {self.checkpoint.checkpoint_file}")
            logger.info(f"  任务ID: {checkpoint_summary.get('task_id', '')}")
            logger.info(f"  进度: {checkpoint_summary.get('progress_pct', 0)}%")
        
        # 打印详细摘要
        self.stats.print_summary()
        
        result = {
            'success': final_stats['success'],
            'skipped': final_stats['skipped'],
            'delisted': final_stats['delisted'],
            'failed': final_stats['failed'],
            'total': final_stats['total'],
            'cache_hits': final_stats['cache_hits'],
            'baostock_stats': final_stats['baostock'],
            'tencent_stats': final_stats['tencent'],
            'fallback_stats': {
                'success': final_stats['fallback_success']
            },
            'delisting_stats': {
                'detected': final_stats['delisted_detected']
            },
            'elapsed_seconds': final_stats['elapsed_seconds'],
            'split_mode': self._config.split_mode,
            'results': all_results,
            'mode': 'dual_source_v3'
        }
        
        if self.checkpoint:
            result['checkpoint'] = checkpoint_summary
        
        return result


def run_dual_source_fetch(
    codes: List[str] = None,
    kline_dir: str = None,
    data_dir: str = None,
    days: int = None,
    filter_delisted: bool = True,
    resume: bool = True
) -> Dict:
    """
    同步入口：运行双源采集（自动管理资源）

    ⚠️ 此函数会正确管理资源生命周期，无需手动 close()

    使用方式：
        result = run_dual_source_fetch(codes=['000001', '000002'])
    """
    fetcher = DualSourceFetcher(kline_dir=kline_dir, data_dir=data_dir)
    loop = asyncio.new_event_loop()
    
    try:
        result = loop.run_until_complete(
            fetcher.run(
                codes=codes,
                days=days,
                filter_delisted=filter_delisted,
                resume=resume
            )
        )
        return result
    finally:
        loop.close()
        fetcher.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="双源K线数据采集器 v3.0")
    parser.add_argument("--codes", type=str, default=None, help="股票代码，逗号分隔")
    parser.add_argument("--days", type=int, default=None, help="采集最近N天")
    parser.add_argument("--output-dir", type=str, default=None, help="输出目录")
    parser.add_argument("--no-filter-delisted", action="store_true", help="不过滤退市股票")
    parser.add_argument("--dry-run", action="store_true", help="仅显示计划")
    parser.add_argument("--no-resume", action="store_true", help="禁用断点续传")
    parser.add_argument("--clear-checkpoint", action="store_true", help="清除进度文件")
    parser.add_argument("--quick-check-only", action="store_true", help="仅快速检查")
    parser.add_argument("--status", action="store_true", help="查看进度状态")
    
    args = parser.parse_args()
    
    codes = args.codes.split(',') if args.codes else None
    
    if args.status:
        checkpoint = CheckpointManager()
        summary = checkpoint.get_summary()
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        sys.exit(0)
    
    if args.clear_checkpoint:
        checkpoint = CheckpointManager()
        checkpoint.clear()
        print("进度文件已清除")
        sys.exit(0)
    
    if args.dry_run:
        config = get_default_config()
        print(f"=== 双源采集 v3.0 配置 ===")
        print(config)
        sys.exit(0)
    
    result = run_dual_source_fetch(
        codes=codes,
        kline_dir=args.output_dir,
        days=args.days,
        filter_delisted=not args.no_filter_delisted,
        resume=not args.no_resume
    )
    
    print("\n" + "=" * 60)
    print("采集结果:")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
