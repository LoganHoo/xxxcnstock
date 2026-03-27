"""数据检查器"""
import logging
import polars as pl
from typing import Dict, Any
from datetime import datetime
from pathlib import Path


class DataValidator:
    """数据检查器
    
    负责检查数据质量，包括完整性、有效性、新鲜度和一致性
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化数据检查器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_all(self, df: pl.DataFrame) -> Dict[str, Any]:
        """执行所有检查
        
        Args:
            df: 待检查的数据框
            
        Returns:
            包含所有检查结果的字典
        """
        results = {
            'completeness': self.check_completeness(df),
            'validity': self.check_validity(df),
            'freshness': self.check_freshness(df)
        }
        
        # 根据配置决定是否执行一致性检查
        if self.config.get('check_consistency', True):
            results['consistency'] = self.check_consistency(df)
        
        results['passed'] = all(
            r.get('passed', False) for r in results.values() 
            if isinstance(r, dict)
        )
        
        return results
    
    def check_completeness(self, df: pl.DataFrame) -> Dict[str, Any]:
        """完整性检查
        
        检查必需字段是否存在，记录数是否足够
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        required_fields = ['code', 'name', 'price', 'grade', 'enhanced_score']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        min_records = self.config.get('min_records', 1000)
        record_count = len(df)
        
        passed = len(missing_fields) == 0 and record_count >= min_records
        
        return {
            'passed': passed,
            'missing_fields': missing_fields,
            'record_count': record_count,
            'min_records': min_records
        }
    
    def check_validity(self, df: pl.DataFrame) -> Dict[str, Any]:
        """有效性检查
        
        检查价格和涨跌幅是否在合理范围内
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        price_range = self.config.get('price_range', [0.1, 1000])
        invalid_prices = df.filter(
            (pl.col('price') < price_range[0]) | 
            (pl.col('price') > price_range[1])
        ) if 'price' in df.columns else pl.DataFrame()
        
        change_range = self.config.get('change_pct_range', [-20, 20])
        invalid_changes = df.filter(
            (pl.col('change_pct') < change_range[0]) | 
            (pl.col('change_pct') > change_range[1])
        ) if 'change_pct' in df.columns else pl.DataFrame()
        
        passed = len(invalid_prices) == 0 and len(invalid_changes) == 0
        
        return {
            'passed': passed,
            'invalid_price_count': len(invalid_prices),
            'invalid_change_count': len(invalid_changes)
        }
    
    def check_freshness(self, df: pl.DataFrame) -> Dict[str, Any]:
        """新鲜度检查
        
        检查数据是否过期
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        max_age_days = self.config.get('max_age_days', 7)
        
        if 'update_time' not in df.columns:
            return {'passed': True, 'message': '无时间戳字段，跳过检查'}
        
        latest_time = df['update_time'].max()
        if isinstance(latest_time, str):
            latest_time = datetime.fromisoformat(latest_time)
        
        age_days = (datetime.now() - latest_time).days
        
        return {
            'passed': age_days <= max_age_days,
            'age_days': age_days,
            'max_age_days': max_age_days
        }
    
    def check_consistency(self, df: pl.DataFrame) -> Dict[str, Any]:
        """一致性检查
        
        检查评分与等级的一致性
        
        Args:
            df: 待检查的数据框
            
        Returns:
            检查结果字典
        """
        if 'grade' not in df.columns or 'enhanced_score' not in df.columns:
            return {'passed': True, 'message': '缺少评分或等级字段'}
        
        inconsistent_grades = df.filter(
            ((pl.col('grade') == 'S') & (pl.col('enhanced_score') < 80)) |
            ((pl.col('grade') == 'A') & ((pl.col('enhanced_score') < 75) | (pl.col('enhanced_score') >= 80)))
        )
        
        return {
            'passed': len(inconsistent_grades) == 0,
            'inconsistent_count': len(inconsistent_grades)
        }
