#!/usr/bin/env python3
"""
财务数据存储管理器

管理三大财务报表和财务指标的存储:
- 资产负债表存储
- 利润表存储
- 现金流量表存储
- 财务指标存储

存储格式: Parquet (按股票代码分片)
存储路径: data/financial/
"""
import pandas as pd
import polars as pl
from typing import List, Dict, Optional, Union
from pathlib import Path
from datetime import datetime
import json

from core.logger import setup_logger
from core.paths import get_data_path

logger = setup_logger("financial_storage", log_file="system/financial_storage.log")


class FinancialStorageManager:
    """财务数据存储管理器"""
    
    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = Path(data_dir) if data_dir else get_data_path() / "financial"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 子目录
        self.balance_sheet_dir = self.data_dir / "balance_sheet"
        self.income_statement_dir = self.data_dir / "income_statement"
        self.cash_flow_dir = self.data_dir / "cash_flow"
        self.indicators_dir = self.data_dir / "indicators"
        
        # 创建子目录
        for dir_path in [self.balance_sheet_dir, self.income_statement_dir, 
                        self.cash_flow_dir, self.indicators_dir]:
            dir_path.mkdir(exist_ok=True)
        
        self.logger = logger
    
    def _get_file_path(self, code: str, data_type: str) -> Path:
        """获取文件路径"""
        dir_map = {
            'balance_sheet': self.balance_sheet_dir,
            'income_statement': self.income_statement_dir,
            'cash_flow': self.cash_flow_dir,
            'indicators': self.indicators_dir,
        }
        directory = dir_map.get(data_type, self.data_dir)
        return directory / f"{code}.parquet"
    
    def save_balance_sheet(
        self,
        code: str,
        df: pd.DataFrame,
        validate: bool = True
    ) -> bool:
        """
        保存资产负债表
        
        Args:
            code: 股票代码
            df: 资产负债表DataFrame
            validate: 是否验证数据
        
        Returns:
            是否保存成功
        """
        if df.empty:
            self.logger.warning(f"{code} 资产负债表为空,跳过保存")
            return False
        
        try:
            file_path = self._get_file_path(code, 'balance_sheet')
            
            # 读取现有数据
            existing_df = self._load_existing(file_path)
            
            # 合并数据(去重)
            if not existing_df.empty:
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                combined = combined.sort_values('report_date', ascending=False)
            else:
                combined = df
            
            # 保存为Parquet
            combined.to_parquet(file_path, index=False, compression='zstd')
            
            self.logger.info(f"{code} 资产负债表已保存: {len(combined)} 条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"{code} 资产负债表保存失败: {e}")
            return False
    
    def save_income_statement(
        self,
        code: str,
        df: pd.DataFrame
    ) -> bool:
        """保存利润表"""
        if df.empty:
            self.logger.warning(f"{code} 利润表为空,跳过保存")
            return False
        
        try:
            file_path = self._get_file_path(code, 'income_statement')
            existing_df = self._load_existing(file_path)
            
            if not existing_df.empty:
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                combined = combined.sort_values('report_date', ascending=False)
            else:
                combined = df
            
            combined.to_parquet(file_path, index=False, compression='zstd')
            self.logger.info(f"{code} 利润表已保存: {len(combined)} 条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"{code} 利润表保存失败: {e}")
            return False
    
    def save_cash_flow(
        self,
        code: str,
        df: pd.DataFrame
    ) -> bool:
        """保存现金流量表"""
        if df.empty:
            self.logger.warning(f"{code} 现金流量表为空,跳过保存")
            return False
        
        try:
            file_path = self._get_file_path(code, 'cash_flow')
            existing_df = self._load_existing(file_path)
            
            if not existing_df.empty:
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                combined = combined.sort_values('report_date', ascending=False)
            else:
                combined = df
            
            combined.to_parquet(file_path, index=False, compression='zstd')
            self.logger.info(f"{code} 现金流量表已保存: {len(combined)} 条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"{code} 现金流量表保存失败: {e}")
            return False
    
    def save_indicators(
        self,
        code: str,
        df: pd.DataFrame
    ) -> bool:
        """保存财务指标"""
        if df.empty:
            self.logger.warning(f"{code} 财务指标为空,跳过保存")
            return False
        
        try:
            file_path = self._get_file_path(code, 'indicators')
            existing_df = self._load_existing(file_path)
            
            if not existing_df.empty:
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                combined = combined.sort_values('report_date', ascending=False)
            else:
                combined = df
            
            combined.to_parquet(file_path, index=False, compression='zstd')
            self.logger.info(f"{code} 财务指标已保存: {len(combined)} 条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"{code} 财务指标保存失败: {e}")
            return False
    
    def load_balance_sheet(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载资产负债表"""
        file_path = self._get_file_path(code, 'balance_sheet')
        df = self._load_existing(file_path)
        
        if df.empty:
            return df
        
        # 日期过滤
        if start_date:
            df = df[df['report_date'] >= start_date]
        if end_date:
            df = df[df['report_date'] <= end_date]
        
        return df.sort_values('report_date', ascending=False)
    
    def load_income_statement(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载利润表"""
        file_path = self._get_file_path(code, 'income_statement')
        df = self._load_existing(file_path)
        
        if df.empty:
            return df
        
        if start_date:
            df = df[df['report_date'] >= start_date]
        if end_date:
            df = df[df['report_date'] <= end_date]
        
        return df.sort_values('report_date', ascending=False)
    
    def load_cash_flow(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载现金流量表"""
        file_path = self._get_file_path(code, 'cash_flow')
        df = self._load_existing(file_path)
        
        if df.empty:
            return df
        
        if start_date:
            df = df[df['report_date'] >= start_date]
        if end_date:
            df = df[df['report_date'] <= end_date]
        
        return df.sort_values('report_date', ascending=False)
    
    def load_indicators(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载财务指标"""
        file_path = self._get_file_path(code, 'indicators')
        df = self._load_existing(file_path)
        
        if df.empty:
            return df
        
        if start_date:
            df = df[df['report_date'] >= start_date]
        if end_date:
            df = df[df['report_date'] <= end_date]
        
        return df.sort_values('report_date', ascending=False)
    
    def load_all_financial_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        加载所有财务数据
        
        Returns:
            {
                'balance_sheet': DataFrame,
                'income_statement': DataFrame,
                'cash_flow': DataFrame,
                'indicators': DataFrame
            }
        """
        return {
            'balance_sheet': self.load_balance_sheet(code, start_date, end_date),
            'income_statement': self.load_income_statement(code, start_date, end_date),
            'cash_flow': self.load_cash_flow(code, start_date, end_date),
            'indicators': self.load_indicators(code, start_date, end_date),
        }
    
    def _load_existing(self, file_path: Path) -> pd.DataFrame:
        """加载已存在的数据文件"""
        if file_path.exists():
            try:
                return pd.read_parquet(file_path)
            except Exception as e:
                self.logger.error(f"加载文件失败 {file_path}: {e}")
        return pd.DataFrame()
    
    def get_available_codes(self, data_type: str = 'balance_sheet') -> List[str]:
        """获取可用的股票代码列表"""
        dir_map = {
            'balance_sheet': self.balance_sheet_dir,
            'income_statement': self.income_statement_dir,
            'cash_flow': self.cash_flow_dir,
            'indicators': self.indicators_dir,
        }
        directory = dir_map.get(data_type, self.balance_sheet_dir)
        
        if not directory.exists():
            return []
        
        codes = []
        for file_path in directory.glob("*.parquet"):
            codes.append(file_path.stem)
        
        return sorted(codes)
    
    def get_storage_stats(self) -> Dict:
        """获取存储统计信息"""
        stats = {
            'balance_sheet': {'count': 0, 'size_mb': 0},
            'income_statement': {'count': 0, 'size_mb': 0},
            'cash_flow': {'count': 0, 'size_mb': 0},
            'indicators': {'count': 0, 'size_mb': 0},
        }
        
        for data_type, dir_path in [
            ('balance_sheet', self.balance_sheet_dir),
            ('income_statement', self.income_statement_dir),
            ('cash_flow', self.cash_flow_dir),
            ('indicators', self.indicators_dir),
        ]:
            if dir_path.exists():
                files = list(dir_path.glob("*.parquet"))
                stats[data_type]['count'] = len(files)
                total_size = sum(f.stat().st_size for f in files)
                stats[data_type]['size_mb'] = round(total_size / 1024 / 1024, 2)
        
        return stats


# ==================== 便捷函数 ====================

def save_financial_data(
    code: str,
    balance_sheet: Optional[pd.DataFrame] = None,
    income_statement: Optional[pd.DataFrame] = None,
    cash_flow: Optional[pd.DataFrame] = None,
    indicators: Optional[pd.DataFrame] = None,
    data_dir: Optional[str] = None
) -> Dict[str, bool]:
    """
    保存财务数据 (便捷函数)
    
    Returns:
        {'balance_sheet': True/False, ...}
    """
    manager = FinancialStorageManager(data_dir)
    results = {}
    
    if balance_sheet is not None:
        results['balance_sheet'] = manager.save_balance_sheet(code, balance_sheet)
    if income_statement is not None:
        results['income_statement'] = manager.save_income_statement(code, income_statement)
    if cash_flow is not None:
        results['cash_flow'] = manager.save_cash_flow(code, cash_flow)
    if indicators is not None:
        results['indicators'] = manager.save_indicators(code, indicators)
    
    return results


def load_financial_data(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    data_dir: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """加载财务数据 (便捷函数)"""
    manager = FinancialStorageManager(data_dir)
    return manager.load_all_financial_data(code, start_date, end_date)


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 财务数据存储管理器")
    print("=" * 50)
    
    manager = FinancialStorageManager()
    
    # 测试保存
    test_df = pd.DataFrame([{
        'code': '000001',
        'report_date': '2023-12-31',
        'report_type': '年报',
        'total_assets': 1000,
        'total_liabilities': 400,
        'total_equity': 600,
    }])
    
    success = manager.save_balance_sheet('000001', test_df)
    print(f"\n保存资产负债表: {'成功' if success else '失败'}")
    
    # 测试加载
    loaded = manager.load_balance_sheet('000001')
    print(f"加载资产负债表: {len(loaded)} 条记录")
    print(loaded)
    
    # 测试统计
    stats = manager.get_storage_stats()
    print(f"\n存储统计: {json.dumps(stats, indent=2, ensure_ascii=False)}")
