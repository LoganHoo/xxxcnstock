#!/usr/bin/env python3
"""
快速检查器 (v3.0)

职责：
1. 快速判断哪些股票需要更新
2. 基于Redis缓存和Parquet元数据
3. 批量并行检查优化
4. 智能跳过策略

设计原则：
- 性能优先：最小化I/O操作
- 准确性：确保判断结果可靠
- 可扩展：支持多种检查策略

v3.0 改进：
- 批量并行I/O优化
- 增加数据质量预检
- 支持自定义检查策略
"""

import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class QuickChecker:
    """
    快速检查器 - 判断哪些股票需要更新
    
    使用方式：
        checker = QuickChecker(kline_dir, date_cache, config)
        need_update, already_fresh, reasons = await checker.quick_check(codes)
    """
    
    def __init__(
        self,
        kline_dir: Path,
        date_cache=None,
        config=None,
        max_concurrent_checks: int = 10
    ):
        """
        初始化快速检查器
        
        Args:
            kline_dir: K线数据目录
            date_cache: IncrementalDateCache实例
            config: DualSourceConfig实例（可选）
            max_concurrent_checks: 最大并行检查数
        """
        self.kline_dir = Path(kline_dir)
        self.date_cache = date_cache
        self._config = config
        self._max_concurrent = max_concurrent_checks
        
        if config is None:
            from services.data_service.fetchers.dual_source_config import get_default_config
            self._config = get_default_config()
    
    @property
    def _quick_check_enabled(self) -> bool:
        """检查快速检查是否启用"""
        return getattr(self._config, 'quick_check_enabled', True)
    
    @property
    def _skip_days(self) -> int:
        """获取跳过新鲜数据的天数阈值"""
        return getattr(self._config, 'quick_check_skip_days', 1)
    
    @property
    def _use_redis(self) -> bool:
        """是否使用Redis缓存进行检查"""
        return getattr(self._config, 'use_redis_for_check', True)
    
    async def quick_check(
        self,
        codes: List[str],
        skip_days: int = None
    ) -> Tuple[List[str], List[str], Dict[str, str]]:
        """
        执行快速检查
        
        Args:
            codes: 待检查的股票代码列表
            skip_days: 覆盖配置的跳过天数
            
        Returns:
            元组 (need_update, already_fresh, reasons)
            - need_update: 需要更新的代码列表
            - already_fresh: 已是最新的代码列表
            - reasons: 字典 {code: reason}
        """
        if not self._quick_check_enabled or not codes:
            return codes, [], {}
        
        skip_days = skip_days or self._skip_days
        today = datetime.now().date()
        threshold = today - timedelta(days=skip_days)
        
        need_update = []
        already_fresh = []
        reasons = {}
        
        cached_dates = {}
        
        if self.date_cache and self._use_redis:
            cached_dates = self.date_cache.get_last_dates(codes)
        
        missing_from_cache = [c for c in codes if c not in cached_dates]
        
        if missing_from_cache:
            parquet_dates = await self._batch_check_parquet(missing_from_cache)
            cached_dates.update(parquet_dates)
        
        for code in codes:
            last_date_str = cached_dates.get(code)
            
            if not last_date_str:
                need_update.append(code)
                reasons[code] = 'no_data'
                continue
            
            try:
                last_date = datetime.strptime(last_date_str[:10], '%Y-%m-%d').date()
                
                if last_date >= threshold:
                    already_fresh.append(code)
                    reasons[code] = f'fresh_{last_date}'
                else:
                    need_update.append(code)
                    reasons[code] = f'stale_{last_date}'
                    
            except (ValueError, TypeError):
                need_update.append(code)
                reasons[code] = 'invalid_date'
        
        logger.info(
            f"快速检查完成: {len(already_fresh)}只已是最新, "
            f"{len(need_update)}只需要更新"
        )
        
        return need_update, already_fresh, reasons
    
    async def _batch_check_parquet(
        self,
        codes: List[str],
        batch_size: int = 50
    ) -> Dict[str, str]:
        """
        批量并行检查Parquet文件
        
        Args:
            codes: 股票代码列表
            batch_size: 每批处理的数量
            
        Returns:
            字典 {code: latest_date}
        """
        if not codes:
            return {}
        
        results = {}
        semaphore = asyncio.Semaphore(self._max_concurrent)
        
        async def check_single(code: str) -> Tuple[str, Optional[str]]:
            async with semaphore:
                date = await self._check_parquet_quick(code)
                return code, date
        
        tasks = [check_single(code) for code in codes]
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        for task_result in completed_tasks:
            if isinstance(task_result, Exception):
                logger.debug(f"批量检查异常: {task_result}")
                continue
            
            code, date = task_result
            if date:
                results[code] = date
                
                if self.date_cache:
                    self.date_cache.set_last_date(code, date)
        
        logger.debug(f"批量Parquet检查完成: {len(results)}/{len(codes)} 只成功")
        return results
    
    async def _check_parquet_quick(self, code: str) -> Optional[str]:
        """
        快速检查单个Parquet文件的最新日期
        
        使用元数据读取，避免加载完整文件
        
        Args:
            code: 股票代码
            
        Returns:
            最新日期字符串或None
        """
        parquet_file = self.kline_dir / f"{code}.parquet"
        
        if not parquet_file.exists():
            return None
        
        try:
            import pyarrow.parquet as pq
            
            pf = pq.ParquetFile(parquet_file)
            
            if pf.metadata.num_rows == 0:
                return None
            
            schema = pf.schema_arrow
            date_col = None
            
            for field in schema:
                if field.name in ('date', 'trade_date'):
                    date_col = field.name
                    break
            
            if not date_col:
                return None
            
            last_row_idx = pf.metadata.num_rows - 1
            rg = pf.metadata.row_group(0)
            rg_idx = min(last_row_idx // rg.num_rows, pf.metadata.num_row_groups - 1)
            
            df = pf.read_row_group(rg_idx, columns=[date_col]).to_pandas()
            
            if df.empty:
                return None
            
            last_date = str(df[date_col].iloc[-1])[:10]
            return last_date
            
        except Exception as e:
            logger.debug(f"快速检查parquet失败 {code}: {e}")
            return None
    
    def check_data_quality(self, code: str) -> Dict:
        """
        检查单只股票的数据质量
        
        Args:
            code: 股票代码
            
        Returns:
            质量检查报告字典
        """
        parquet_file = self.kline_dir / f"{code}.parquet"
        
        if not parquet_file.exists():
            return {'exists': False, 'valid': False}
        
        try:
            import pandas as pd
            import pyarrow.parquet as pq
            
            pf = pq.ParquetFile(parquet_file)
            
            report = {
                'exists': True,
                'file_size_kb': round(parquet_file.stat().st_size / 1024, 1),
                'total_rows': pf.metadata.num_rows,
                'valid': True,
                'issues': []
            }
            
            if pf.metadata.num_rows == 0:
                report['valid'] = False
                report['issues'].append('empty_file')
                return report
            
            schema = pf.schema_arrow
            required_cols = {'trade_date', 'open', 'high', 'low', 'close'}
            actual_cols = {field.name for field in schema}
            
            missing_cols = required_cols - actual_cols
            if missing_cols:
                report['valid'] = False
                report['issues'].append(f'missing_columns:{missing_cols}')
            
            if report['valid']:
                df = pd.read_parquet(parquet_file)
                
                null_counts = df[required_cols & actual_cols].isnull().sum()
                if null_counts.any():
                    report['issues'].append(f'null_values:{null_counts.to_dict()}')
                
                if 'close' in df.columns and (df['close'] <= 0).any():
                    report['valid'] = False
                    report['issues'].append('invalid_close_prices')
                
                if 'trade_date' in df.columns:
                    dates = pd.to_datetime(df['trade_date'])
                    gaps = dates.diff().dt.days.dropna()
                    large_gaps = gaps[gaps > 7]
                    
                    if len(large_gaps) > 0:
                        report['issues'].append({
                            'type': 'date_gaps',
                            'count': len(large_gaps),
                            'max_gap': int(large_gaps.max())
                        })
                
                report['date_range'] = {
                    'start': str(df['trade_date'].min())[:10],
                    'end': str(df['trade_date'].max())[:10],
                    'total_days': len(df)
                }
            
            return report
            
        except Exception as e:
            return {
                'exists': True,
                'valid': False,
                'error': str(e)[:100],
                'issues': ['read_error']
            }
