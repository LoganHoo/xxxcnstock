#!/usr/bin/env python3
"""
财务数据更新任务

定时更新财务数据:
- 季度更新: 年报、半年报、季报发布后更新
- 增量更新: 只更新新增数据
- 全量更新: 首次初始化时使用

执行频率:
- 季度报告发布后(4月、8月、10月)执行全量更新
- 其他时间执行增量更新
"""
import pandas as pd
from typing import List, Dict, Optional, Set
from datetime import datetime, date, timedelta
from pathlib import Path
import json

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.fetchers.stock_list import get_all_stocks

logger = setup_logger("financial_update_task", log_file="tasks/financial_update.log")


class FinancialUpdateTask:
    """财务数据更新任务"""
    
    def __init__(self, tushare_token: Optional[str] = None):
        self.service = UnifiedDataService(tushare_token)
        self.logger = logger
        
        # 任务状态文件
        self.status_file = get_data_path() / "tasks" / "financial_update_status.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
    
    def run_incremental_update(
        self,
        codes: Optional[List[str]] = None,
        lookback_days: int = 30
    ) -> Dict[str, any]:
        """
        执行增量更新
        
        Args:
            codes: 股票代码列表,None表示全部
            lookback_days: 回溯天数
        
        Returns:
            更新结果统计
        """
        self.logger.info("开始财务数据增量更新")
        
        # 获取股票列表
        if codes is None:
            codes = self._get_stock_codes()
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
        
        self.logger.info(f"更新范围: {start_date} - {end_date}, 共 {len(codes)} 只股票")
        
        # 执行更新
        results = self.service.batch_update_financial_data(codes, start_date, end_date)
        
        # 统计结果
        stats = self._calculate_stats(results)
        
        # 保存状态
        self._save_status({
            'type': 'incremental',
            'date': datetime.now().isoformat(),
            'stats': stats,
            'codes_count': len(codes)
        })
        
        self.logger.info(f"增量更新完成: {stats}")
        return stats
    
    def run_full_update(
        self,
        codes: Optional[List[str]] = None,
        years: int = 3
    ) -> Dict[str, any]:
        """
        执行全量更新
        
        Args:
            codes: 股票代码列表,None表示全部
            years: 回溯年数
        
        Returns:
            更新结果统计
        """
        self.logger.info("开始财务数据全量更新")
        
        # 获取股票列表
        if codes is None:
            codes = self._get_stock_codes()
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365*years)).strftime('%Y%m%d')
        
        self.logger.info(f"全量更新范围: {start_date} - {end_date}, 共 {len(codes)} 只股票")
        
        # 分批处理(避免内存溢出)
        batch_size = 100
        all_results = {}
        
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i+batch_size]
            self.logger.info(f"处理批次 {i//batch_size + 1}/{(len(codes)-1)//batch_size + 1}")
            
            batch_results = self.service.batch_update_financial_data(
                batch_codes, start_date, end_date
            )
            all_results.update(batch_results)
        
        # 统计结果
        stats = self._calculate_stats(all_results)
        
        # 保存状态
        self._save_status({
            'type': 'full',
            'date': datetime.now().isoformat(),
            'stats': stats,
            'codes_count': len(codes),
            'years': years
        })
        
        self.logger.info(f"全量更新完成: {stats}")
        return stats
    
    def run_quarterly_update(self) -> Dict[str, any]:
        """
        执行季度更新
        
        在财报季(4月、8月、10月)执行全量更新
        """
        now = datetime.now()
        month = now.month
        
        # 判断是否为财报季
        is_reporting_season = month in [4, 5, 8, 9, 10, 11]
        
        if is_reporting_season:
            self.logger.info(f"{month}月为财报季,执行全量更新")
            return self.run_full_update(years=3)
        else:
            self.logger.info(f"{month}月非财报季,执行增量更新")
            return self.run_incremental_update(lookback_days=30)
    
    def validate_recent_data(self, days: int = 7) -> Dict[str, any]:
        """
        验证最近更新的数据质量
        
        Args:
            days: 检查最近几天的数据
        
        Returns:
            验证结果
        """
        self.logger.info(f"验证最近{days}天的数据质量")
        
        # 获取已存储的股票代码
        codes = self.service.financial_storage.get_available_codes('balance_sheet')
        
        validation_results = {
            'total': len(codes),
            'valid': 0,
            'invalid': 0,
            'errors': []
        }
        
        # 计算检查日期
        check_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        for code in codes[:100]:  # 抽样检查前100只
            try:
                result = self.service.validate_financial_data(code, check_date)
                if result['is_valid']:
                    validation_results['valid'] += 1
                else:
                    validation_results['invalid'] += 1
                    validation_results['errors'].append({
                        'code': code,
                        'errors': result['errors']
                    })
            except Exception as e:
                self.logger.error(f"{code} 验证失败: {e}")
        
        self.logger.info(f"数据验证完成: 有效{validation_results['valid']}, 无效{validation_results['invalid']}")
        return validation_results
    
    def _get_stock_codes(self) -> List[str]:
        """获取股票代码列表"""
        try:
            stocks_df = get_all_stocks()
            return stocks_df['code'].tolist()
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")
            return []
    
    def _calculate_stats(self, results: Dict) -> Dict[str, int]:
        """计算更新统计"""
        stats = {
            'total_codes': len(results),
            'balance_sheet_success': 0,
            'income_statement_success': 0,
            'cash_flow_success': 0,
            'indicators_success': 0,
        }
        
        for code, result in results.items():
            if result.get('balance_sheet'):
                stats['balance_sheet_success'] += 1
            if result.get('income_statement'):
                stats['income_statement_success'] += 1
            if result.get('cash_flow'):
                stats['cash_flow_success'] += 1
            if result.get('indicators'):
                stats['indicators_success'] += 1
        
        return stats
    
    def _save_status(self, status: Dict):
        """保存任务状态"""
        try:
            # 读取现有状态
            if self.status_file.exists():
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            else:
                history = []
            
            # 添加新状态
            history.append(status)
            
            # 只保留最近30条记录
            history = history[-30:]
            
            # 保存
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.error(f"保存任务状态失败: {e}")
    
    def get_last_status(self) -> Optional[Dict]:
        """获取最后一次任务状态"""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
                    return history[-1] if history else None
        except Exception as e:
            self.logger.error(f"读取任务状态失败: {e}")
        return None


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 财务数据更新任务")
    print("=" * 50)
    
    task = FinancialUpdateTask()
    
    # 测试增量更新(只更新少量股票)
    print("\n1. 测试增量更新:")
    test_codes = ['000001', '000002', '600000']
    stats = task.run_incremental_update(codes=test_codes, lookback_days=365)
    print(f"更新统计: {stats}")
    
    # 测试数据验证
    print("\n2. 测试数据验证:")
    validation = task.validate_recent_data(days=30)
    print(f"验证结果: 有效{validation['valid']}, 无效{validation['invalid']}")
