"""
增量数据处理模块

提供数据增量检测、变更检测和数据完整性验证功能。
支持基于日期范围和哈希值的增量处理策略。
"""

import hashlib
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pandas as pd
from loguru import logger


@dataclass
class IncrementalCheckResult:
    """增量检查结果"""
    code: str
    needs_update: bool
    local_latest_date: Optional[str] = None
    target_start_date: Optional[str] = None
    target_end_date: Optional[str] = None
    missing_dates: List[str] = None
    reason: str = ""


@dataclass
class DataHashResult:
    """数据哈希结果"""
    code: str
    hash_value: str
    row_count: int
    date_range: Tuple[str, str]


class IncrementalDetector:
    """
    增量数据检测器
    
    检测数据缺失情况，确定需要更新的日期范围。
    """
    
    def __init__(self, kline_dir: Path):
        self.kline_dir = Path(kline_dir)
    
    def check_stock(
        self,
        code: str,
        target_start: str,
        target_end: str
    ) -> IncrementalCheckResult:
        """
        检查单只股票的增量需求
        
        Args:
            code: 股票代码
            target_start: 目标开始日期 YYYY-MM-DD
            target_end: 目标结束日期 YYYY-MM-DD
            
        Returns:
            IncrementalCheckResult: 检查结果
        """
        kline_file = self.kline_dir / f"{code}.parquet"
        
        # 文件不存在，需要全量采集
        if not kline_file.exists():
            return IncrementalCheckResult(
                code=code,
                needs_update=True,
                target_start_date=target_start,
                target_end_date=target_end,
                reason="文件不存在"
            )
        
        try:
            # 读取现有数据
            df = pl.read_parquet(kline_file)
            
            # 获取日期列
            date_col = self._get_date_column(df)
            if not date_col:
                return IncrementalCheckResult(
                    code=code,
                    needs_update=True,
                    target_start_date=target_start,
                    target_end_date=target_end,
                    reason="无法识别日期列"
                )
            
            # 获取最新日期
            latest_date_str = df[date_col].max()
            local_latest = datetime.strptime(str(latest_date_str), "%Y-%m-%d").date()
            target_end_date = datetime.strptime(target_end, "%Y-%m-%d").date()
            target_start_date = datetime.strptime(target_start, "%Y-%m-%d").date()
            
            # 数据已是最新
            if local_latest >= target_end_date:
                return IncrementalCheckResult(
                    code=code,
                    needs_update=False,
                    local_latest_date=local_latest.strftime("%Y-%m-%d"),
                    reason="数据已是最新"
                )
            
            # 计算缺失日期
            missing_dates = []
            current = local_latest + timedelta(days=1)
            while current <= target_end_date:
                # 只添加工作日（简单判断，实际应使用交易日历）
                if current.weekday() < 5:  # 周一到周五
                    missing_dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            
            if not missing_dates:
                return IncrementalCheckResult(
                    code=code,
                    needs_update=False,
                    local_latest_date=local_latest.strftime("%Y-%m-%d"),
                    reason="无缺失交易日"
                )
            
            return IncrementalCheckResult(
                code=code,
                needs_update=True,
                local_latest_date=local_latest.strftime("%Y-%m-%d"),
                target_start_date=missing_dates[0],
                target_end_date=missing_dates[-1],
                missing_dates=missing_dates,
                reason=f"缺失 {len(missing_dates)} 个交易日"
            )
            
        except Exception as e:
            logger.warning(f"检查 {code} 失败: {e}")
            return IncrementalCheckResult(
                code=code,
                needs_update=True,
                target_start_date=target_start,
                target_end_date=target_end,
                reason=f"检查失败: {e}"
            )
    
    def check_multiple(
        self,
        codes: List[str],
        target_start: str,
        target_end: str
    ) -> List[IncrementalCheckResult]:
        """
        批量检查增量需求
        
        Args:
            codes: 股票代码列表
            target_start: 目标开始日期
            target_end: 目标结束日期
            
        Returns:
            List[IncrementalCheckResult]: 检查结果列表
        """
        results = []
        for code in codes:
            result = self.check_stock(code, target_start, target_end)
            results.append(result)
        return results
    
    def get_codes_to_update(
        self,
        codes: List[str],
        target_start: str,
        target_end: str
    ) -> Tuple[List[str], List[IncrementalCheckResult]]:
        """
        获取需要更新的股票代码
        
        Returns:
            (codes_to_update, all_results)
        """
        results = self.check_multiple(codes, target_start, target_end)
        codes_to_update = [r.code for r in results if r.needs_update]
        return codes_to_update, results
    
    def _get_date_column(self, df: pl.DataFrame) -> Optional[str]:
        """获取日期列名"""
        for col in ['trade_date', 'date', 'Date', 'TradeDate']:
            if col in df.columns:
                return col
        return None


class DataHashChecker:
    """
    数据哈希检查器
    
    通过计算数据哈希值检测数据变更。
    """
    
    def __init__(self, kline_dir: Path):
        self.kline_dir = Path(kline_dir)
    
    def compute_hash(self, df: pl.DataFrame) -> str:
        """
        计算数据哈希值
        
        Args:
            df: Polars DataFrame
            
        Returns:
            str: MD5 哈希值
        """
        # 转换为字符串并排序以确保一致性
        data_str = df.to_pandas().to_json(sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def check_stock_hash(
        self,
        code: str,
        new_df: pl.DataFrame
    ) -> Tuple[bool, Optional[str]]:
        """
        检查股票数据是否变更
        
        Args:
            code: 股票代码
            new_df: 新数据
            
        Returns:
            (has_changed, old_hash)
        """
        new_hash = self.compute_hash(new_df)
        
        # 读取旧数据
        kline_file = self.kline_dir / f"{code}.parquet"
        if not kline_file.exists():
            return True, None
        
        try:
            old_df = pl.read_parquet(kline_file)
            old_hash = self.compute_hash(old_df)
            return new_hash != old_hash, old_hash
        except Exception as e:
            logger.warning(f"计算 {code} 哈希失败: {e}")
            return True, None
    
    def get_stock_hash_info(self, code: str) -> Optional[DataHashResult]:
        """获取股票数据的哈希信息"""
        kline_file = self.kline_dir / f"{code}.parquet"
        
        if not kline_file.exists():
            return None
        
        try:
            df = pl.read_parquet(kline_file)
            hash_value = self.compute_hash(df)
            
            date_col = None
            for col in ['trade_date', 'date', 'Date']:
                if col in df.columns:
                    date_col = col
                    break
            
            if date_col:
                min_date = df[date_col].min()
                max_date = df[date_col].max()
                date_range = (str(min_date), str(max_date))
            else:
                date_range = ("", "")
            
            return DataHashResult(
                code=code,
                hash_value=hash_value,
                row_count=len(df),
                date_range=date_range
            )
        except Exception as e:
            logger.warning(f"获取 {code} 哈希信息失败: {e}")
            return None


def check_date_continuity(
    df: pl.DataFrame,
    date_col: str = 'trade_date',
    max_gap_days: int = 7
) -> Tuple[bool, List[str]]:
    """
    检查日期连续性
    
    Args:
        df: 数据框
        date_col: 日期列名
        max_gap_days: 允许的最大间隔天数
        
    Returns:
        (is_continuous, gap_dates)
    """
    if df.is_empty():
        return True, []
    
    # 获取日期列表
    dates = sorted(df[date_col].to_list())
    
    # 检查间隔
    gaps = []
    for i in range(1, len(dates)):
        curr = datetime.strptime(str(dates[i]), "%Y-%m-%d")
        prev = datetime.strptime(str(dates[i-1]), "%Y-%m-%d")
        
        gap = (curr - prev).days
        if gap > max_gap_days:
            # 记录间隔期间的所有日期
            current = prev + timedelta(days=1)
            while current < curr:
                if current.weekday() < 5:  # 工作日
                    gaps.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
    
    return len(gaps) == 0, gaps


def validate_data_integrity(
    df: pl.DataFrame,
    code: str,
    min_rows: int = 50,
    check_ohlc: bool = True
) -> Tuple[bool, str]:
    """
    验证数据完整性
    
    Args:
        df: 数据框
        code: 股票代码
        min_rows: 最小行数要求
        check_ohlc: 是否检查 OHLC 逻辑
        
    Returns:
        (is_valid, message)
    """
    # 检查行数
    if len(df) < min_rows:
        return False, f"数据行数不足: {len(df)} < {min_rows}"
    
    # 检查必需列
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"缺少列: {missing_cols}"
    
    # 检查 OHLC 逻辑
    if check_ohlc:
        df_pd = df.to_pandas()
        
        # High >= max(Open, Close)
        high_valid = (df_pd['high'] >= df_pd[['open', 'close']].max(axis=1)).all()
        
        # Low <= min(Open, Close)
        low_valid = (df_pd['low'] <= df_pd[['open', 'close']].min(axis=1)).all()
        
        # High >= Low
        range_valid = (df_pd['high'] >= df_pd['low']).all()
        
        if not (high_valid and low_valid and range_valid):
            return False, "OHLC 逻辑错误"
    
    # 检查日期连续性
    date_col = None
    for col in ['trade_date', 'date']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col:
        is_continuous, gaps = check_date_continuity(df, date_col)
        if not is_continuous:
            return False, f"日期不连续，缺失 {len(gaps)} 天"
    
    return True, "验证通过"


class IncrementalDataLoader:
    """
    增量数据加载器
    
    支持增量加载和合并数据。
    """
    
    def __init__(self, kline_dir: Path):
        self.kline_dir = Path(kline_dir)
        self.detector = IncrementalDetector(kline_dir)
    
    def load_existing(self, code: str) -> Optional[pl.DataFrame]:
        """加载现有数据"""
        kline_file = self.kline_dir / f"{code}.parquet"
        
        if not kline_file.exists():
            return None
        
        try:
            return pl.read_parquet(kline_file)
        except Exception as e:
            logger.warning(f"加载 {code} 现有数据失败: {e}")
            return None
    
    def merge_and_save(
        self,
        code: str,
        new_df: pl.DataFrame,
        date_col: str = 'trade_date'
    ) -> bool:
        """
        合并新旧数据并保存
        
        Args:
            code: 股票代码
            new_df: 新数据
            date_col: 日期列名
            
        Returns:
            bool: 是否成功
        """
        try:
            # 加载现有数据
            existing_df = self.load_existing(code)
            
            if existing_df is None:
                # 没有现有数据，直接保存
                output_file = self.kline_dir / f"{code}.parquet"
                new_df.write_parquet(output_file)
                logger.info(f"保存 {code}: {len(new_df)} 行")
                return True
            
            # 合并数据
            combined = pl.concat([existing_df, new_df])
            
            # 去重（保留最新）
            combined = combined.unique(subset=[date_col], keep='last')
            
            # 排序
            combined = combined.sort(date_col)
            
            # 保存
            output_file = self.kline_dir / f"{code}.parquet"
            combined.write_parquet(output_file)
            
            logger.info(
                f"合并保存 {code}: "
                f"原有 {len(existing_df)} 行, "
                f"新增 {len(new_df)} 行, "
                f"合并后 {len(combined)} 行"
            )
            return True
            
        except Exception as e:
            logger.error(f"合并保存 {code} 失败: {e}")
            return False
    
    def get_update_summary(
        self,
        codes: List[str],
        target_start: str,
        target_end: str
    ) -> Dict:
        """
        获取更新摘要
        
        Returns:
            {
                'total': 总股票数,
                'needs_update': 需要更新的数量,
                'up_to_date': 已最新的数量,
                'details': 详细信息列表
            }
        """
        results = self.detector.check_multiple(codes, target_start, target_end)
        
        needs_update = [r for r in results if r.needs_update]
        up_to_date = [r for r in results if not r.needs_update]
        
        return {
            'total': len(codes),
            'needs_update': len(needs_update),
            'up_to_date': len(up_to_date),
            'details': [
                {
                    'code': r.code,
                    'needs_update': r.needs_update,
                    'local_latest': r.local_latest_date,
                    'missing_dates': len(r.missing_dates) if r.missing_dates else 0,
                    'reason': r.reason
                }
                for r in results
            ]
        }


# 便捷函数
def get_incremental_codes(
    codes: List[str],
    kline_dir: Path,
    target_start: str,
    target_end: str
) -> List[str]:
    """
    便捷函数：获取需要增量更新的股票代码
    
    使用示例：
        codes_to_update = get_incremental_codes(
            all_codes,
            Path("./data/kline"),
            "2024-01-01",
            "2024-12-31"
        )
    """
    detector = IncrementalDetector(kline_dir)
    codes_to_update, _ = detector.get_codes_to_update(codes, target_start, target_end)
    return codes_to_update
