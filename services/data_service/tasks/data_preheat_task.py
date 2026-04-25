#!/usr/bin/env python3
"""
数据预热任务

在系统启动或每日开盘前预热热点数据到内存:
- 热点股票财务数据
- 市场行为数据
- 公告数据

预热策略:
- 基于近期龙虎榜、资金流向确定热点股票
- 优先加载大盘蓝筹股财务数据
- 预加载最近公告的股票

使用示例:
    task = DataPreheatingTask()
    task.run_preheating()  # 执行预热
"""
import pandas as pd
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService
from services.data_service.storage.optimized_financial_storage import (
    OptimizedFinancialStorageManager
)
from services.data_service.fetchers.market_behavior import (
    DragonTigerFetcher,
    MoneyFlowFetcher,
)
from services.data_service.fetchers.announcement import AnnouncementFetcher

logger = setup_logger("data_preheating", log_file="system/data_preheating.log")


class DataPreheatingTask:
    """数据预热任务"""
    
    def __init__(self, max_workers: int = 4):
        self.logger = logger
        self.max_workers = max_workers
        
        # 服务
        self.data_service = UnifiedDataService()
        self.storage = OptimizedFinancialStorageManager(enable_cache=True)
        
        # 获取器
        self.dragon_tiger_fetcher = DragonTigerFetcher()
        self.money_flow_fetcher = MoneyFlowFetcher()
        self.announcement_fetcher = AnnouncementFetcher()
        
        # 预热配置
        self.config = {
            'hot_stock_count': 100,      # 热点股票数量
            'blue_chip_count': 50,       # 蓝筹股数量
            'announcement_days': 3,      # 公告回溯天数
        }
        
        # 预热状态
        self.preheat_status = {
            'last_preheat': None,
            'preheated_codes': [],
            'preheat_time': 0,
        }
    
    def _get_hot_stocks_from_dragon_tiger(self, days: int = 3) -> Set[str]:
        """从龙虎榜获取热点股票"""
        hot_codes = set()
        
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            # 获取机构买入数据
            df = self.dragon_tiger_fetcher.fetch_institution_trading(start_date, end_date)
            
            if not df.empty and 'code' in df.columns:
                # 按机构净买入排序
                if 'institution_net' in df.columns:
                    df = df.sort_values('institution_net', ascending=False)
                    hot_codes = set(df.head(self.config['hot_stock_count'])['code'].tolist())
                else:
                    hot_codes = set(df['code'].unique().tolist()[:self.config['hot_stock_count']])
                
                self.logger.info(f"龙虎榜热点股票: {len(hot_codes)} 只")
                
        except Exception as e:
            self.logger.error(f"获取龙虎榜热点失败: {e}")
        
        return hot_codes
    
    def _get_hot_stocks_from_money_flow(self, top_n: int = 100) -> Set[str]:
        """从资金流向获取热点股票"""
        hot_codes = set()
        
        try:
            # 获取板块资金流向中的热点股票
            # 简化处理：这里可以扩展为获取个股资金流向
            self.logger.info("资金流向热点股票分析暂略")
            
        except Exception as e:
            self.logger.error(f"获取资金流向热点失败: {e}")
        
        return hot_codes
    
    def _get_blue_chip_stocks(self) -> Set[str]:
        """获取大盘蓝筹股"""
        # 预定义的蓝筹股列表 (可以扩展为从配置或数据库加载)
        blue_chips = {
            '600519',  # 贵州茅台
            '000858',  # 五粮液
            '000001',  # 平安银行
            '600036',  # 招商银行
            '000002',  # 万科A
            '600000',  # 浦发银行
            '601398',  # 工商银行
            '601288',  # 农业银行
            '601988',  # 中国银行
            '601318',  # 中国平安
            '600276',  # 恒瑞医药
            '000333',  # 美的集团
            '000651',  # 格力电器
            '002415',  # 海康威视
            '600887',  # 伊利股份
            '603288',  # 海天味业
            '600309',  # 万华化学
            '002594',  # 比亚迪
            '300750',  # 宁德时代
            '600900',  # 长江电力
        }
        
        self.logger.info(f"蓝筹股: {len(blue_chips)} 只")
        return blue_chips
    
    def _get_stocks_from_recent_announcements(self, days: int = 3) -> Set[str]:
        """从近期公告获取股票"""
        announcement_codes = set()
        
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            # 获取重大事项
            major_events = self.announcement_fetcher.fetch_major_events(start_date, end_date)
            if not major_events.empty and 'code' in major_events.columns:
                announcement_codes.update(major_events['code'].unique().tolist())
            
            # 获取业绩预告
            forecasts = self.announcement_fetcher.fetch_performance_forecasts(start_date, end_date)
            if not forecasts.empty and 'code' in forecasts.columns:
                announcement_codes.update(forecasts['code'].unique().tolist())
            
            self.logger.info(f"公告相关股票: {len(announcement_codes)} 只")
            
        except Exception as e:
            self.logger.error(f"获取公告股票失败: {e}")
        
        return announcement_codes
    
    def _preheat_single_stock(self, code: str) -> Dict:
        """预热单只股票数据"""
        result = {
            'code': code,
            'success': True,
            'loaded_types': [],
            'error': None
        }
        
        try:
            # 预加载财务指标
            indicators = self.storage.load_indicators(code)
            if not indicators.empty:
                result['loaded_types'].append('indicators')
            
            # 预加载资产负债表
            bs = self.storage.load_balance_sheet(code)
            if not bs.empty:
                result['loaded_types'].append('balance_sheet')
            
            # 预加载利润表
            inc = self.storage.load_income_statement(code)
            if not inc.empty:
                result['loaded_types'].append('income_statement')
            
            # 预加载现金流量表
            cf = self.storage.load_cash_flow(code)
            if not cf.empty:
                result['loaded_types'].append('cash_flow')
                
        except Exception as e:
            result['success'] = False
            result['error'] = str(e)
        
        return result
    
    def run_preheating(self) -> Dict:
        """
        执行数据预热
        
        Returns:
            预热结果
        """
        import time
        start_time = time.time()
        
        self.logger.info("=" * 60)
        self.logger.info("开始数据预热")
        self.logger.info("=" * 60)
        
        # 收集需要预热的股票
        codes_to_preheat = set()
        
        # 1. 龙虎榜热点
        dragon_tiger_codes = self._get_hot_stocks_from_dragon_tiger()
        codes_to_preheat.update(dragon_tiger_codes)
        
        # 2. 资金流向热点
        money_flow_codes = self._get_hot_stocks_from_money_flow()
        codes_to_preheat.update(money_flow_codes)
        
        # 3. 蓝筹股
        blue_chip_codes = self._get_blue_chip_stocks()
        codes_to_preheat.update(blue_chip_codes)
        
        # 4. 公告相关
        announcement_codes = self._get_stocks_from_recent_announcements(
            self.config['announcement_days']
        )
        codes_to_preheat.update(announcement_codes)
        
        # 限制数量
        codes_to_preheat = list(codes_to_preheat)[:200]
        
        self.logger.info(f"需要预热的股票: {len(codes_to_preheat)} 只")
        
        # 并发预热
        results = {
            'total': len(codes_to_preheat),
            'success': 0,
            'failed': 0,
            'loaded_types': {
                'indicators': 0,
                'balance_sheet': 0,
                'income_statement': 0,
                'cash_flow': 0,
            }
        }
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._preheat_single_stock, code): code
                for code in codes_to_preheat
            }
            
            for future in futures:
                try:
                    result = future.result()
                    
                    if result['success']:
                        results['success'] += 1
                        for data_type in result['loaded_types']:
                            results['loaded_types'][data_type] += 1
                    else:
                        results['failed'] += 1
                        
                except Exception as e:
                    self.logger.error(f"预热结果处理失败: {e}")
                    results['failed'] += 1
        
        # 更新状态
        elapsed_time = time.time() - start_time
        self.preheat_status = {
            'last_preheat': datetime.now().isoformat(),
            'preheated_codes': codes_to_preheat,
            'preheat_time': elapsed_time,
        }
        
        self.logger.info("=" * 60)
        self.logger.info("数据预热完成")
        self.logger.info(f"成功: {results['success']}, 失败: {results['failed']}")
        self.logger.info(f"预热耗时: {elapsed_time:.2f}s")
        self.logger.info(f"缓存统计: {self.storage.get_cache_stats()}")
        self.logger.info("=" * 60)
        
        return {
            'success': results['success'],
            'failed': results['failed'],
            'elapsed_time': elapsed_time,
            'loaded_types': results['loaded_types'],
        }
    
    def get_preheat_status(self) -> Dict:
        """获取预热状态"""
        return self.preheat_status
    
    def is_preheat_needed(self, max_age_minutes: int = 60) -> bool:
        """检查是否需要预热"""
        if self.preheat_status['last_preheat'] is None:
            return True
        
        last_preheat = datetime.fromisoformat(self.preheat_status['last_preheat'])
        elapsed = (datetime.now() - last_preheat).total_seconds() / 60
        
        return elapsed > max_age_minutes


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 数据预热任务")
    print("=" * 60)
    
    task = DataPreheatingTask(max_workers=4)
    
    # 测试获取热点股票
    print("\n1. 测试获取热点股票:")
    hot_codes = task._get_hot_stocks_from_dragon_tiger()
    print(f"龙虎榜热点: {len(hot_codes)} 只")
    
    blue_chips = task._get_blue_chip_stocks()
    print(f"蓝筹股: {len(blue_chips)} 只")
    
    # 测试预热状态
    print("\n2. 预热状态:")
    status = task.get_preheat_status()
    print(f"状态: {status}")
    
    print(f"\n是否需要预热: {task.is_preheat_needed()}")
    
    # 测试单只股票预热
    print("\n3. 测试单只股票预热:")
    result = task._preheat_single_stock("000001")
    print(f"结果: {result}")
