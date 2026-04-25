#!/usr/bin/env python3
"""
公告数据更新任务

定时更新公告数据:
- 个股公告: 每日更新
- 重大事项: 每日更新
- 业绩预告: 每日更新
- 交易提示: 每日更新

执行频率:
- 每日 18:00 (收盘后)
- 每日 22:00 (补充更新)
"""
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from pathlib import Path
import json

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService

logger = setup_logger("announcement_task", log_file="tasks/announcement.log")


class AnnouncementUpdateTask:
    """公告数据更新任务"""
    
    def __init__(self):
        self.service = UnifiedDataService()
        self.logger = logger
        
        # 存储路径
        self.data_dir = get_data_path() / "announcements"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.major_events_dir = self.data_dir / "major_events"
        self.performance_dir = self.data_dir / "performance"
        self.trading_hints_dir = self.data_dir / "trading_hints"
        self.stock_announcements_dir = self.data_dir / "stock"
        
        for dir_path in [self.major_events_dir, self.performance_dir, 
                        self.trading_hints_dir, self.stock_announcements_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 状态文件
        self.status_file = get_data_path() / "tasks" / "announcement_status.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
    
    def update_major_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, any]:
        """
        更新重大事项公告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            更新结果
        """
        self.logger.info("更新重大事项公告")
        
        # 默认获取最近3天
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
        
        try:
            df = self.service.get_major_events(start_date, end_date)
            
            if df.empty:
                self.logger.warning("重大事项公告为空")
                return {'success': False, 'message': '数据为空'}
            
            # 保存数据
            date_str = datetime.now().strftime('%Y%m%d')
            file_path = self.major_events_dir / f"major_events_{date_str}.parquet"
            df.to_parquet(file_path, index=False, compression='zstd')
            
            # 按类型统计
            type_counts = df['announcement_type'].value_counts().to_dict() if 'announcement_type' in df.columns else {}
            
            result = {
                'success': True,
                'count': len(df),
                'date_range': f"{start_date}-{end_date}",
                'type_counts': type_counts,
                'file': str(file_path)
            }
            
            self.logger.info(f"重大事项公告已保存: {len(df)} 条")
            
            # 保存状态
            self._save_status('major_events', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"重大事项更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def update_performance_forecasts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, any]:
        """
        更新业绩预告
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            更新结果
        """
        self.logger.info("更新业绩预告")
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        try:
            df = self.service.get_performance_forecasts(start_date, end_date)
            
            if df.empty:
                self.logger.warning("业绩预告为空")
                return {'success': False, 'message': '数据为空'}
            
            # 保存数据
            date_str = datetime.now().strftime('%Y%m%d')
            file_path = self.performance_dir / f"performance_{date_str}.parquet"
            df.to_parquet(file_path, index=False, compression='zstd')
            
            result = {
                'success': True,
                'count': len(df),
                'date_range': f"{start_date}-{end_date}",
                'file': str(file_path)
            }
            
            self.logger.info(f"业绩预告已保存: {len(df)} 条")
            
            # 保存状态
            self._save_status('performance_forecasts', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"业绩预告更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def update_trading_hints(self, trade_date: Optional[str] = None) -> Dict[str, any]:
        """
        更新交易提示(停牌复牌)
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
        
        Returns:
            更新结果
        """
        self.logger.info(f"更新交易提示: {trade_date or '最新'}")
        
        try:
            df = self.service.get_trading_hints(trade_date)
            
            if df.empty:
                self.logger.warning("交易提示为空")
                return {'success': False, 'message': '数据为空'}
            
            # 保存数据
            date_str = trade_date or datetime.now().strftime('%Y%m%d')
            file_path = self.trading_hints_dir / f"trading_hints_{date_str}.parquet"
            df.to_parquet(file_path, index=False, compression='zstd')
            
            result = {
                'success': True,
                'count': len(df),
                'date': date_str,
                'file': str(file_path)
            }
            
            self.logger.info(f"交易提示已保存: {len(df)} 条")
            
            # 保存状态
            self._save_status('trading_hints', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"交易提示更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def update_stock_announcements(
        self,
        codes: List[str],
        days: int = 3
    ) -> Dict[str, any]:
        """
        更新个股公告
        
        Args:
            codes: 股票代码列表
            days: 回溯天数
        
        Returns:
            更新结果
        """
        self.logger.info(f"更新个股公告: {len(codes)} 只股票")
        
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        all_announcements = []
        success_count = 0
        
        for code in codes:
            try:
                df = self.service.get_announcements(code, start_date, end_date)
                if not df.empty:
                    all_announcements.append(df)
                    success_count += 1
            except Exception as e:
                self.logger.error(f"{code} 公告获取失败: {e}")
        
        if all_announcements:
            combined = pd.concat(all_announcements, ignore_index=True)
            
            # 保存数据
            date_str = datetime.now().strftime('%Y%m%d')
            file_path = self.stock_announcements_dir / f"stock_announcements_{date_str}.parquet"
            combined.to_parquet(file_path, index=False, compression='zstd')
            
            result = {
                'success': True,
                'count': len(combined),
                'codes_processed': success_count,
                'file': str(file_path)
            }
            
            self.logger.info(f"个股公告已保存: {len(combined)} 条")
        else:
            result = {'success': False, 'message': '无数据'}
        
        # 保存状态
        self._save_status('stock_announcements', result)
        
        return result
    
    def run_daily_update(
        self,
        stock_codes: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        执行每日公告更新
        
        Args:
            stock_codes: 关注的股票代码列表
        
        Returns:
            更新结果汇总
        """
        self.logger.info("开始每日公告更新")
        
        results = {
            'date': datetime.now().isoformat(),
            'major_events': None,
            'performance_forecasts': None,
            'trading_hints': None,
            'stock_announcements': None,
        }
        
        # 更新重大事项
        results['major_events'] = self.update_major_events()
        
        # 更新业绩预告
        results['performance_forecasts'] = self.update_performance_forecasts()
        
        # 更新交易提示
        results['trading_hints'] = self.update_trading_hints()
        
        # 更新个股公告(如果提供了代码列表)
        if stock_codes:
            results['stock_announcements'] = self.update_stock_announcements(stock_codes)
        
        # 保存总体状态
        self._save_status('daily_update', results)
        
        self.logger.info("每日公告更新完成")
        return results
    
    def get_recent_major_events(self, days: int = 7) -> pd.DataFrame:
        """获取最近的重点事项"""
        files = sorted(self.major_events_dir.glob("*.parquet"), reverse=True)
        
        dfs = []
        for file_path in files[:days]:
            df = pd.read_parquet(file_path)
            dfs.append(df)
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        
        return pd.DataFrame()
    
    def get_high_importance_events(self, days: int = 7) -> pd.DataFrame:
        """获取高重要性事件"""
        df = self.get_recent_major_events(days)
        
        if df.empty or 'importance' not in df.columns:
            return df
        
        return df[df['importance'] == 'high']
    
    def _save_status(self, task_type: str, result: Dict):
        """保存任务状态"""
        try:
            # 读取现有状态
            if self.status_file.exists():
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            else:
                history = {}
            
            # 更新状态
            if task_type not in history:
                history[task_type] = []
            
            history[task_type].append({
                'date': datetime.now().isoformat(),
                'result': result
            })
            
            # 只保留最近30条
            history[task_type] = history[task_type][-30:]
            
            # 保存
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.error(f"保存任务状态失败: {e}")


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("测试: 公告数据更新任务")
    print("=" * 50)
    
    task = AnnouncementUpdateTask()
    
    # 测试重大事项更新
    print("\n1. 测试重大事项更新:")
    result = task.update_major_events()
    print(f"结果: {result}")
    
    # 测试业绩预告
    print("\n2. 测试业绩预告更新:")
    result = task.update_performance_forecasts()
    print(f"结果: {result}")
    
    # 测试交易提示
    print("\n3. 测试交易提示更新:")
    result = task.update_trading_hints()
    print(f"结果: {result}")
