#!/usr/bin/env python3
"""
增量更新优化任务

优化数据更新流程:
- 智能增量检测: 只更新变化的数据
- 差异对比: 对比新旧数据,只保存差异
- 并发更新: 多线程并行更新
- 断点续传: 支持中断后恢复

使用示例:
    task = IncrementalUpdateTask()
    task.run_incremental_financial_update()
"""
import pandas as pd
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import hashlib

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.fetchers.financial import (
    BalanceSheetFetcher,
    IncomeStatementFetcher,
    CashFlowFetcher,
)
from services.data_service.storage.optimized_financial_storage import (
    OptimizedFinancialStorageManager
)

logger = setup_logger("incremental_update", log_file="system/incremental_update.log")


class IncrementalUpdateTask:
    """增量更新任务"""
    
    def __init__(self, max_workers: int = 4):
        self.logger = logger
        self.max_workers = max_workers
        
        # 获取器
        self.balance_fetcher = BalanceSheetFetcher()
        self.income_fetcher = IncomeStatementFetcher()
        self.cash_flow_fetcher = CashFlowFetcher()
        
        # 存储
        self.storage = OptimizedFinancialStorageManager(enable_cache=True)
        
        # 状态文件
        self.status_dir = get_data_path() / "update_status"
        self.status_dir.mkdir(parents=True, exist_ok=True)
        self.status_file = self.status_dir / "incremental_update_status.json"
    
    def _load_status(self) -> Dict:
        """加载更新状态"""
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"加载状态失败: {e}")
        
        return {
            'last_update': None,
            'completed_codes': {},
            'failed_codes': {},
        }
    
    def _save_status(self, status: Dict):
        """保存更新状态"""
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存状态失败: {e}")
    
    def _calculate_data_hash(self, df: pd.DataFrame) -> str:
        """计算数据指纹(用于快速对比)"""
        if df.empty:
            return ""
        
        # 取最新10条数据计算hash
        sample = df.head(10).to_json()
        return hashlib.md5(sample.encode()).hexdigest()
    
    def _get_existing_report_dates(self, code: str, data_type: str) -> Set[str]:
        """获取已存在的报告期"""
        try:
            if data_type == 'balance_sheet':
                df = self.storage.load_balance_sheet(code, use_cache=False)
            elif data_type == 'income_statement':
                df = self.storage.load_income_statement(code, use_cache=False)
            elif data_type == 'cash_flow':
                df = self.storage.load_cash_flow(code, use_cache=False)
            else:
                return set()
            
            if df.empty:
                return set()
            
            return set(df['report_date'].tolist())
            
        except Exception as e:
            self.logger.debug(f"{code} 获取现有日期失败: {e}")
            return set()
    
    def _update_single_stock_financial(
        self,
        code: str,
        years: int = 3
    ) -> Dict:
        """
        更新单只股票的财务数据 (增量)
        
        Args:
            code: 股票代码
            years: 获取年数
        
        Returns:
            更新结果
        """
        result = {
            'code': code,
            'balance_sheet': {'added': 0, 'updated': 0},
            'income_statement': {'added': 0, 'updated': 0},
            'cash_flow': {'added': 0, 'updated': 0},
            'success': True,
            'error': None
        }
        
        try:
            # 获取现有数据日期
            existing_bs_dates = self._get_existing_report_dates(code, 'balance_sheet')
            existing_inc_dates = self._get_existing_report_dates(code, 'income_statement')
            existing_cf_dates = self._get_existing_report_dates(code, 'cash_flow')
            
            # 获取新数据
            new_bs = self.balance_fetcher.fetch_stock_balance_sheet(code, years=years)
            new_inc = self.income_fetcher.fetch_stock_income_statement(code, years=years)
            new_cf = self.cash_flow_fetcher.fetch_stock_cash_flow(code, years=years)
            
            # 对比并更新资产负债表
            if not new_bs.empty:
                new_dates = set(new_bs['report_date'].tolist())
                added_dates = new_dates - existing_bs_dates
                
                if added_dates:
                    added_data = new_bs[new_bs['report_date'].isin(added_dates)]
                    # 合并并保存
                    existing_bs = self.storage.load_balance_sheet(code, use_cache=False)
                    combined = pd.concat([existing_bs, added_data], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                    combined = combined.sort_values('report_date', ascending=False)
                    
                    file_path = self.storage._get_file_path(code, 'balance_sheet')
                    combined.to_parquet(file_path, index=False, compression='zstd')
                    
                    result['balance_sheet']['added'] = len(added_dates)
                    self.logger.debug(f"{code} 资产负债表新增 {len(added_dates)} 期")
            
            # 对比并更新利润表
            if not new_inc.empty:
                new_dates = set(new_inc['report_date'].tolist())
                added_dates = new_dates - existing_inc_dates
                
                if added_dates:
                    added_data = new_inc[new_inc['report_date'].isin(added_dates)]
                    existing_inc = self.storage.load_income_statement(code, use_cache=False)
                    combined = pd.concat([existing_inc, added_data], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                    combined = combined.sort_values('report_date', ascending=False)
                    
                    file_path = self.storage._get_file_path(code, 'income_statement')
                    combined.to_parquet(file_path, index=False, compression='zstd')
                    
                    result['income_statement']['added'] = len(added_dates)
            
            # 对比并更新现金流量表
            if not new_cf.empty:
                new_dates = set(new_cf['report_date'].tolist())
                added_dates = new_dates - existing_cf_dates
                
                if added_dates:
                    added_data = new_cf[new_cf['report_date'].isin(added_dates)]
                    existing_cf = self.storage.load_cash_flow(code, use_cache=False)
                    combined = pd.concat([existing_cf, added_data], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['report_date'], keep='last')
                    combined = combined.sort_values('report_date', ascending=False)
                    
                    file_path = self.storage._get_file_path(code, 'cash_flow')
                    combined.to_parquet(file_path, index=False, compression='zstd')
                    
                    result['cash_flow']['added'] = len(added_dates)
            
        except Exception as e:
            result['success'] = False
            result['error'] = str(e)
            self.logger.error(f"{code} 增量更新失败: {e}")
        
        return result
    
    def run_incremental_financial_update(
        self,
        stock_codes: Optional[List[str]] = None,
        years: int = 3,
        resume: bool = True
    ) -> Dict:
        """
        执行增量财务数据更新
        
        Args:
            stock_codes: 股票代码列表 (None表示全部)
            years: 获取年数
            resume: 是否从上次中断处恢复
        
        Returns:
            更新结果汇总
        """
        self.logger.info("=" * 60)
        self.logger.info("开始增量财务数据更新")
        self.logger.info("=" * 60)
        
        # 加载状态
        status = self._load_status() if resume else {
            'last_update': None,
            'completed_codes': {},
            'failed_codes': {},
        }
        
        # 获取股票列表
        if stock_codes is None:
            stock_codes = self.storage.get_available_codes('balance_sheet')
        
        # 排除已完成的
        if resume:
            completed = set(status['completed_codes'].keys())
            stock_codes = [c for c in stock_codes if c not in completed]
            self.logger.info(f"恢复模式: 跳过 {len(completed)} 只已完成股票")
        
        self.logger.info(f"待更新股票: {len(stock_codes)} 只")
        
        # 并发更新
        results = {
            'start_time': datetime.now().isoformat(),
            'total_codes': len(stock_codes),
            'success_count': 0,
            'fail_count': 0,
            'added_records': {
                'balance_sheet': 0,
                'income_statement': 0,
                'cash_flow': 0,
            }
        }
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(self._update_single_stock_financial, code, years): code
                for code in stock_codes
            }
            
            # 处理结果
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                
                try:
                    result = future.result()
                    
                    if result['success']:
                        results['success_count'] += 1
                        status['completed_codes'][code] = {
                            'updated_at': datetime.now().isoformat(),
                            'added': result['balance_sheet']['added'] + 
                                    result['income_statement']['added'] + 
                                    result['cash_flow']['added']
                        }
                        
                        results['added_records']['balance_sheet'] += result['balance_sheet']['added']
                        results['added_records']['income_statement'] += result['income_statement']['added']
                        results['added_records']['cash_flow'] += result['cash_flow']['added']
                    else:
                        results['fail_count'] += 1
                        status['failed_codes'][code] = {
                            'error': result['error'],
                            'failed_at': datetime.now().isoformat()
                        }
                    
                    # 每10只保存一次状态
                    if (results['success_count'] + results['fail_count']) % 10 == 0:
                        self._save_status(status)
                        
                except Exception as e:
                    self.logger.error(f"{code} 处理结果失败: {e}")
                    results['fail_count'] += 1
        
        # 保存最终状态
        status['last_update'] = datetime.now().isoformat()
        self._save_status(status)
        
        results['end_time'] = datetime.now().isoformat()
        
        self.logger.info("=" * 60)
        self.logger.info("增量更新完成")
        self.logger.info(f"成功: {results['success_count']}, 失败: {results['fail_count']}")
        self.logger.info(f"新增记录: 资产负债表 {results['added_records']['balance_sheet']}, "
                        f"利润表 {results['added_records']['income_statement']}, "
                        f"现金流量表 {results['added_records']['cash_flow']}")
        self.logger.info("=" * 60)
        
        return results
    
    def detect_changes(
        self,
        code: str,
        data_type: str = 'balance_sheet'
    ) -> Dict:
        """
        检测数据变化
        
        Args:
            code: 股票代码
            data_type: 数据类型
        
        Returns:
            变化信息
        """
        try:
            # 加载本地数据
            if data_type == 'balance_sheet':
                local_df = self.storage.load_balance_sheet(code, use_cache=False)
                new_df = self.balance_fetcher.fetch_stock_balance_sheet(code, years=1)
            elif data_type == 'income_statement':
                local_df = self.storage.load_income_statement(code, use_cache=False)
                new_df = self.income_fetcher.fetch_stock_income_statement(code, years=1)
            elif data_type == 'cash_flow':
                local_df = self.storage.load_cash_flow(code, use_cache=False)
                new_df = self.cash_flow_fetcher.fetch_stock_cash_flow(code, years=1)
            else:
                return {'error': '未知数据类型'}
            
            # 对比
            local_dates = set(local_df['report_date'].tolist()) if not local_df.empty else set()
            new_dates = set(new_df['report_date'].tolist()) if not new_df.empty else set()
            
            added = new_dates - local_dates
            removed = local_dates - new_dates
            
            return {
                'code': code,
                'data_type': data_type,
                'local_count': len(local_dates),
                'new_count': len(new_dates),
                'added': list(added),
                'removed': list(removed),
                'has_changes': len(added) > 0 or len(removed) > 0
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_update_summary(self) -> Dict:
        """获取更新摘要"""
        status = self._load_status()
        
        return {
            'last_update': status.get('last_update'),
            'completed_count': len(status.get('completed_codes', {})),
            'failed_count': len(status.get('failed_codes', {})),
            'recent_failures': list(status.get('failed_codes', {}).keys())[-10:]
        }


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 增量更新任务")
    print("=" * 60)
    
    task = IncrementalUpdateTask(max_workers=2)
    
    # 测试变化检测
    print("\n1. 测试变化检测:")
    changes = task.detect_changes("000001", "balance_sheet")
    print(f"变化信息: {changes}")
    
    # 测试单只股票增量更新
    print("\n2. 测试单只股票增量更新:")
    result = task._update_single_stock_financial("000001")
    print(f"更新结果: {result}")
    
    # 测试更新摘要
    print("\n3. 更新摘要:")
    summary = task.get_update_summary()
    print(f"摘要: {summary}")
