#!/usr/bin/env python3
"""
数据质量守护模块
功能：
1. 数据对齐检查（K线数据日期是否为T-1/T）
2. 异常值过滤（ST股、退市股、停牌股）
3. 数据新鲜度验证

Author: AI Assistant
Date: 2026-04-28
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logger import setup_logger

logger = setup_logger("data_quality_guardian")


class DataQualityGuardian:
    """数据质量守护者"""
    
    # 风险关键词
    RISK_KEYWORDS = ['ST', '*ST', '退市', '摘牌', '终止上市']
    
    def __init__(self):
        self.data_dir = project_root / 'data'
        self.kline_dir = self.data_dir / 'kline'
        
    def check_kline_data_alignment(self, code: str, expected_date: str) -> Tuple[bool, str]:
        """
        检查K线数据对齐
        
        Args:
            code: 股票代码
            expected_date: 期望的最新数据日期 (YYYY-MM-DD)
            
        Returns:
            (是否对齐, 错误信息)
        """
        kline_file = self.kline_dir / f"{code}.parquet"
        
        if not kline_file.exists():
            return False, f"K线数据文件不存在: {code}"
        
        try:
            df = pd.read_parquet(kline_file)
            if df.empty:
                return False, f"K线数据为空: {code}"
            
            # 获取日期列
            date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
            latest_date = pd.to_datetime(df[date_col].max()).strftime('%Y-%m-%d')
            
            if latest_date != expected_date:
                return False, f"数据未对齐: {code} 最新日期 {latest_date}, 期望 {expected_date}"
            
            return True, "数据对齐"
            
        except Exception as e:
            return False, f"检查异常: {code} - {e}"
    
    def check_stock_risk_status(self, code: str, name: str) -> Tuple[bool, str]:
        """
        检查股票风险状态
        
        Returns:
            (是否安全, 错误信息)
        """
        # 检查名称中的风险关键词
        for keyword in self.RISK_KEYWORDS:
            if keyword in name:
                return False, f"风险股票: {code} {name} - 包含关键词 '{keyword}'"
        
        # 检查K线数据新鲜度（30天内必须有数据）
        kline_file = self.kline_dir / f"{code}.parquet"
        if kline_file.exists():
            try:
                df = pd.read_parquet(kline_file)
                if not df.empty:
                    date_col = 'trade_date' if 'trade_date' in df.columns else 'date'
                    latest_date = pd.to_datetime(df[date_col].max())
                    days_since_last_trade = (datetime.now() - latest_date).days
                    
                    if days_since_last_trade > 30:
                        return False, f"数据过旧: {code} {name} - 最近交易日期 {latest_date.strftime('%Y-%m-%d')} ({days_since_last_trade}天前)"
            except Exception as e:
                logger.warning(f"检查数据新鲜度失败: {code} - {e}")
        
        return True, "股票安全"
    
    def check_suspension_status(self, code: str) -> Tuple[bool, str]:
        """
        检查停牌状态（通过成交量判断）
        
        Returns:
            (是否交易, 错误信息)
        """
        kline_file = self.kline_dir / f"{code}.parquet"
        
        if not kline_file.exists():
            return False, f"无法检查停牌: {code} 无K线数据"
        
        try:
            df = pd.read_parquet(kline_file)
            if df.empty or len(df) < 5:
                return False, f"数据不足: {code}"
            
            # 最近5天成交量
            recent_volume = df.tail(5)['volume'].mean()
            
            # 成交量为0或极低视为停牌
            if recent_volume == 0:
                return False, f"停牌股票: {code} - 最近5天成交量为0"
            
            return True, "正常交易"
            
        except Exception as e:
            return False, f"检查停牌异常: {code} - {e}"
    
    def validate_selection_candidate(self, code: str, name: str, 
                                     trade_date: str) -> Tuple[bool, List[str]]:
        """
        综合验证选股候选股票
        
        Args:
            code: 股票代码
            name: 股票名称
            trade_date: 交易日期
            
        Returns:
            (是否通过, 错误列表)
        """
        errors = []
        
        # 1. 风险状态检查
        safe, msg = self.check_stock_risk_status(code, name)
        if not safe:
            errors.append(msg)
        
        # 2. 数据对齐检查
        aligned, msg = self.check_kline_data_alignment(code, trade_date)
        if not aligned:
            errors.append(msg)
        
        # 3. 停牌检查
        trading, msg = self.check_suspension_status(code)
        if not trading:
            errors.append(msg)
        
        return len(errors) == 0, errors
    
    def batch_validate_candidates(self, candidates: List[Dict], 
                                   trade_date: str) -> List[Dict]:
        """
        批量验证候选股票
        
        Args:
            candidates: 候选股票列表 [{'code': '600519', 'name': '贵州茅台'}, ...]
            trade_date: 交易日期
            
        Returns:
            通过验证的股票列表
        """
        valid_candidates = []
        
        for candidate in candidates:
            code = candidate.get('code')
            name = candidate.get('name', '')
            
            is_valid, errors = self.validate_selection_candidate(code, name, trade_date)
            
            if is_valid:
                valid_candidates.append(candidate)
            else:
                logger.warning(f"股票 {code} {name} 未通过验证:")
                for error in errors:
                    logger.warning(f"  - {error}")
        
        logger.info(f"批量验证完成: {len(valid_candidates)}/{len(candidates)} 通过")
        return valid_candidates

    def is_data_ready_for_selection(self, trade_date: str = None) -> Tuple[bool, str]:
        """
        检查数据是否就绪可用于选股
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD)，默认为昨日
            
        Returns:
            (是否就绪, 错误信息)
        """
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 检查涨停数据是否存在
        date_str = trade_date.replace('-', '')
        limitup_file = self.data_dir / 'limitup' / f'limitup_{date_str}.parquet'
        
        if not limitup_file.exists():
            return False, f"涨停数据文件不存在: {limitup_file}"
        
        try:
            df = pd.read_parquet(limitup_file)
            if df.empty:
                return False, f"涨停数据为空: {trade_date}"
            
            logger.info(f"✅ 数据就绪检查通过: {trade_date}, {len(df)}条涨停记录")
            return True, "数据就绪"
            
        except Exception as e:
            return False, f"读取涨停数据失败: {e}"



# 便捷函数
def validate_stock(code: str, name: str, trade_date: str) -> Tuple[bool, List[str]]:
    """便捷验证函数"""
    guardian = DataQualityGuardian()
    return guardian.validate_selection_candidate(code, name, trade_date)


def filter_risk_stocks(stock_list: List[Dict], trade_date: str) -> List[Dict]:
    """过滤风险股票"""
    guardian = DataQualityGuardian()
    return guardian.batch_validate_candidates(stock_list, trade_date)


if __name__ == "__main__":
    # 测试
    guardian = DataQualityGuardian()
    
    # 测试数据对齐检查
    print("=" * 60)
    print("数据质量检查测试")
    print("=" * 60)
    
    # 测试正常股票
    result, msg = guardian.check_stock_risk_status("600519", "贵州茅台")
    print(f"600519 贵州茅台: {msg}")
    
    # 测试ST股票
    result, msg = guardian.check_stock_risk_status("000001", "*ST平安")
    print(f"000001 *ST平安: {msg}")
    
    # 测试退市股票
    result, msg = guardian.check_stock_risk_status("000002", "退市万科")
    print(f"000002 退市万科: {msg}")
