#!/usr/bin/env python3
"""
每日综合更新任务

整合所有数据更新任务:
1. 财务数据更新(季度执行)
2. 市场行为数据更新(每日执行)
3. 公告数据更新(每日执行)
4. 数据质量检查

执行计划:
- 17:00: 龙虎榜数据
- 17:30: 资金流向数据
- 18:00: 公告数据
- 20:00: 数据质量检查
- 00:00: 财务数据(季度)
"""
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from pathlib import Path
import json
import schedule
import time

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.tasks.financial_update_task import FinancialUpdateTask
from services.data_service.tasks.market_behavior_task import MarketBehaviorUpdateTask
from services.data_service.tasks.announcement_task import AnnouncementUpdateTask

logger = setup_logger("daily_update_task", log_file="tasks/daily_update.log")


class DailyUpdateTask:
    """每日综合更新任务"""
    
    def __init__(self, tushare_token: Optional[str] = None):
        self.financial_task = FinancialUpdateTask(tushare_token)
        self.market_behavior_task = MarketBehaviorUpdateTask()
        self.announcement_task = AnnouncementUpdateTask()
        self.logger = logger
        
        # 状态文件
        self.status_file = get_data_path() / "tasks" / "daily_update_status.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 关注股票列表(用于个股公告更新)
        self.watch_list_file = get_data_path() / "config" / "watch_list.txt"
        self.watch_list_file.parent.mkdir(parents=True, exist_ok=True)
    
    def run_market_close_update(self) -> Dict[str, any]:
        """
        收盘后更新(17:00-18:00)
        
        更新市场行为数据和公告
        """
        self.logger.info("=" * 60)
        self.logger.info("开始收盘后数据更新")
        self.logger.info("=" * 60)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'market_close',
            'market_behavior': None,
            'announcements': None,
        }
        
        # 更新市场行为数据
        try:
            self.logger.info("[1/2] 更新市场行为数据...")
            results['market_behavior'] = self.market_behavior_task.run_daily_update()
            self.logger.info("市场行为数据更新完成")
        except Exception as e:
            self.logger.error(f"市场行为数据更新失败: {e}")
            results['market_behavior'] = {'success': False, 'error': str(e)}
        
        # 更新公告数据
        try:
            self.logger.info("[2/2] 更新公告数据...")
            watch_list = self._get_watch_list()
            results['announcements'] = self.announcement_task.run_daily_update(watch_list)
            self.logger.info("公告数据更新完成")
        except Exception as e:
            self.logger.error(f"公告数据更新失败: {e}")
            results['announcements'] = {'success': False, 'error': str(e)}
        
        # 保存状态
        self._save_status(results)
        
        self.logger.info("收盘后数据更新完成")
        return results
    
    def run_night_update(self) -> Dict[str, any]:
        """
        夜间更新(20:00-22:00)
        
        数据质量检查和补充更新
        """
        self.logger.info("=" * 60)
        self.logger.info("开始夜间数据更新")
        self.logger.info("=" * 60)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'night',
            'quality_check': None,
        }
        
        # 数据质量检查
        try:
            self.logger.info("[1/1] 执行数据质量检查...")
            validation_results = self.financial_task.validate_recent_data(days=7)
            results['quality_check'] = validation_results
            self.logger.info(f"数据质量检查完成: 有效{validation_results['valid']}, 无效{validation_results['invalid']}")
        except Exception as e:
            self.logger.error(f"数据质量检查失败: {e}")
            results['quality_check'] = {'success': False, 'error': str(e)}
        
        # 保存状态
        self._save_status(results)
        
        self.logger.info("夜间数据更新完成")
        return results
    
    def run_quarterly_update(self) -> Dict[str, any]:
        """
        季度更新(财报季)
        
        执行财务数据全量更新
        """
        self.logger.info("=" * 60)
        self.logger.info("开始季度财务数据更新")
        self.logger.info("=" * 60)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'quarterly',
            'financial': None,
        }
        
        try:
            self.logger.info("[1/1] 更新财务数据...")
            results['financial'] = self.financial_task.run_quarterly_update()
            self.logger.info("财务数据更新完成")
        except Exception as e:
            self.logger.error(f"财务数据更新失败: {e}")
            results['financial'] = {'success': False, 'error': str(e)}
        
        # 保存状态
        self._save_status(results)
        
        self.logger.info("季度财务数据更新完成")
        return results
    
    def run_full_update(self) -> Dict[str, any]:
        """
        执行完整更新
        
        包括所有数据类型
        """
        self.logger.info("=" * 60)
        self.logger.info("开始完整数据更新")
        self.logger.info("=" * 60)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'phase': 'full',
            'market_close': None,
            'night': None,
            'quarterly': None,
        }
        
        # 收盘后更新
        results['market_close'] = self.run_market_close_update()
        
        # 夜间更新
        results['night'] = self.run_night_update()
        
        # 季度更新
        results['quarterly'] = self.run_quarterly_update()
        
        # 保存总体状态
        self._save_status(results)
        
        self.logger.info("完整数据更新完成")
        return results
    
    def schedule_daily_jobs(self):
        """
        设置每日定时任务
        
        使用 schedule 库设置定时任务
        """
        # 收盘后更新: 17:00
        schedule.every().day.at("17:00").do(self.run_market_close_update)
        
        # 夜间更新: 20:00
        schedule.every().day.at("20:00").do(self.run_night_update)
        
        # 季度更新检查: 每月1日 00:00
        schedule.every().day.at("00:00").do(self._check_and_run_quarterly)
        
        self.logger.info("定时任务已设置:")
        self.logger.info("  - 17:00: 收盘后更新")
        self.logger.info("  - 20:00: 夜间更新")
        self.logger.info("  - 00:00: 季度更新检查")
    
    def run_scheduler(self):
        """运行调度器"""
        self.logger.info("启动数据更新调度器...")
        self.schedule_daily_jobs()
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    def _check_and_run_quarterly(self):
        """检查并执行季度更新"""
        now = datetime.now()
        
        # 只在财报季(4月、8月、10月)执行
        if now.month in [4, 8, 10] and now.day == 1:
            self.logger.info(f"{now.month}月1日,执行季度更新")
            return self.run_quarterly_update()
        
        return None
    
    def _get_watch_list(self) -> List[str]:
        """获取关注股票列表"""
        if not self.watch_list_file.exists():
            # 默认关注一些大盘蓝筹股
            default_stocks = ['000001', '000002', '600000', '600519', '000858']
            return default_stocks
        
        try:
            with open(self.watch_list_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.logger.error(f"读取关注列表失败: {e}")
            return []
    
    def set_watch_list(self, codes: List[str]):
        """设置关注股票列表"""
        try:
            with open(self.watch_list_file, 'w') as f:
                for code in codes:
                    f.write(f"{code}\n")
            self.logger.info(f"关注列表已更新: {len(codes)} 只股票")
        except Exception as e:
            self.logger.error(f"保存关注列表失败: {e}")
    
    def get_update_summary(self, days: int = 7) -> Dict[str, any]:
        """
        获取最近更新汇总
        
        Args:
            days: 最近几天
        
        Returns:
            更新汇总
        """
        try:
            if not self.status_file.exists():
                return {'message': '无更新记录'}
            
            with open(self.status_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
            
            # 筛选最近几天的记录
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_updates = []
            
            for record in history:
                try:
                    record_date = datetime.fromisoformat(record.get('timestamp', ''))
                    if record_date >= cutoff_date:
                        recent_updates.append(record)
                except:
                    pass
            
            # 统计
            summary = {
                'period': f"最近{days}天",
                'total_updates': len(recent_updates),
                'success_count': sum(1 for r in recent_updates if self._is_success(r)),
                'failed_count': sum(1 for r in recent_updates if not self._is_success(r)),
                'last_update': recent_updates[-1] if recent_updates else None
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"获取更新汇总失败: {e}")
            return {'error': str(e)}
    
    def _is_success(self, record: Dict) -> bool:
        """判断记录是否成功"""
        # 检查各种可能的成功标志
        if 'market_behavior' in record:
            mb = record['market_behavior']
            if isinstance(mb, dict):
                if mb.get('dragon_tiger', {}).get('success'):
                    return True
        
        if 'financial' in record:
            fin = record['financial']
            if isinstance(fin, dict):
                if fin.get('balance_sheet_success', 0) > 0:
                    return True
        
        return False
    
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
            
            # 只保留最近100条记录
            history = history[-100:]
            
            # 保存
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.error(f"保存任务状态失败: {e}")


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("测试: 每日综合更新任务")
    print("=" * 60)
    
    task = DailyUpdateTask()
    
    # 测试收盘后更新
    print("\n1. 测试收盘后更新:")
    result = task.run_market_close_update()
    print(f"市场行为更新: {'成功' if result['market_behavior'] else '失败'}")
    print(f"公告更新: {'成功' if result['announcements'] else '失败'}")
    
    # 测试夜间更新
    print("\n2. 测试夜间更新:")
    result = task.run_night_update()
    print(f"质量检查: {result.get('quality_check', {})}")
    
    # 测试更新汇总
    print("\n3. 测试更新汇总:")
    summary = task.get_update_summary(days=7)
    print(f"汇总: {summary}")
