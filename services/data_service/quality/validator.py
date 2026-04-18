#!/usr/bin/env python3
"""
数据质量验证器

验证股票数据的完整性和合理性
"""
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    checks_passed: List[str]


class DataValidator:
    """数据验证器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.max_price = self.config.get('max_price', 5000)
        self.max_volume = self.config.get('max_volume', 5e9)
        self.max_turnover = self.config.get('max_turnover', 1e11)
    
    def validate_all(self, df: pd.DataFrame) -> ValidationResult:
        """执行所有验证"""
        errors = []
        warnings = []
        checks_passed = []
        
        # 价格验证
        price_result = self.validate_price(df)
        if price_result.is_valid:
            checks_passed.append('price')
        else:
            errors.extend(price_result.errors)
        warnings.extend(price_result.warnings)
        
        # OHLC逻辑验证
        ohlc_result = self.validate_ohlc(df)
        if ohlc_result.is_valid:
            checks_passed.append('ohlc')
        else:
            errors.extend(ohlc_result.errors)
        
        # 成交量验证
        volume_result = self.validate_volume(df)
        if volume_result.is_valid:
            checks_passed.append('volume')
        else:
            errors.extend(volume_result.errors)
        warnings.extend(volume_result.warnings)
        
        # 连续性验证
        if 'date' in df.columns:
            continuity_result = self.validate_continuity(df)
            if continuity_result.is_valid:
                checks_passed.append('continuity')
            warnings.extend(continuity_result.warnings)
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=checks_passed
        )
    
    def validate_price(self, df: pd.DataFrame) -> ValidationResult:
        """验证价格数据"""
        errors = []
        warnings = []
        
        required_cols = ['open', 'high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
                continue
            
            # 检查负数价格
            if (df[col] < 0).any():
                errors.append(f"negative_price_in_{col}")
            
            # 检查极端价格
            if (df[col] > self.max_price).any():
                warnings.append(f"extreme_price_in_{col}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=[]
        )
    
    def validate_ohlc(self, df: pd.DataFrame) -> ValidationResult:
        """验证OHLC逻辑"""
        errors = []
        
        if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            return ValidationResult(False, ["Missing OHLC columns"], [], [])
        
        # high应>=open, close, low
        if (df['high'] < df[['open', 'close', 'low']].max(axis=1)).any():
            errors.append("ohlc_logic_error: high < max(open, close, low)")
        
        # low应<=open, close, high
        if (df['low'] > df[['open', 'close', 'high']].min(axis=1)).any():
            errors.append("ohlc_logic_error: low > min(open, close, high)")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=[],
            checks_passed=[]
        )
    
    def validate_volume(self, df: pd.DataFrame) -> ValidationResult:
        """验证成交量数据"""
        errors = []
        warnings = []
        
        if 'volume' not in df.columns:
            return ValidationResult(False, ["Missing volume column"], [], [])
        
        # 检查负数成交量
        if (df['volume'] < 0).any():
            errors.append("negative_volume")
        
        # 检查停牌 (成交量为0)
        if (df['volume'] == 0).any():
            warnings.append("suspension_detected")
        
        # 检查极端成交量
        if (df['volume'] > self.max_volume).any():
            warnings.append("extreme_volume")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checks_passed=[]
        )
    
    def validate_continuity(self, df: pd.DataFrame, expected_freq: str = 'D') -> ValidationResult:
        """验证数据连续性"""
        warnings = []
        
        if 'date' not in df.columns or df.empty:
            return ValidationResult(True, [], [], [])
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # 检查日期间隔
        date_diff = df['date'].diff().dropna()
        
        if expected_freq == 'D':
            # 日线数据，检查是否有超过5天的间隔 (排除周末)
            long_gaps = date_diff[date_diff > pd.Timedelta(days=5)]
            if not long_gaps.empty:
                warnings.append(f"data_gap_detected: {len(long_gaps)} gaps > 5 days")
        
        return ValidationResult(
            is_valid=True,
            errors=[],
            warnings=warnings,
            checks_passed=[]
        )
