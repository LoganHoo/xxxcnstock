#!/usr/bin/env python3
"""
市场行为数据更新任务

定时更新市场行为数据:
- 龙虎榜数据: 每日收盘后更新
- 资金流向数据: 实时或收盘后更新
- 北向资金数据: 每日收盘后更新

执行频率:
- 龙虎榜: 每日 17:00 (收盘后)
- 资金流向: 每日 17:30
- 北向资金: 每日 17:30
"""
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from pathlib import Path
import json

from core.logger import setup_logger
from core.paths import get_data_path
from services.data_service.unified_data_service import UnifiedDataService

logger = setup_logger("market_behavior_task", log_file="tasks/market_behavior.log")


class MarketBehaviorUpdateTask:
    """市场行为数据更新任务"""
    
    def __init__(self):
        self.service = UnifiedDataService()
        self.logger = logger
        
        # 存储路径
        self.data_dir = get_data_path() / "market_behavior"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.dragon_tiger_dir = self.data_dir / "dragon_tiger"
        self.money_flow_dir = self.data_dir / "money_flow"
        self.northbound_dir = self.data_dir / "northbound"
        
        for dir_path in [self.dragon_tiger_dir, self.money_flow_dir, self.northbound_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 状态文件
        self.status_file = get_data_path() / "tasks" / "market_behavior_status.json"
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
    
    def update_dragon_tiger(self, trade_date: Optional[str] = None) -> Dict[str, any]:
        """
        更新龙虎榜数据
        
        Args:
            trade_date: 交易日期 (YYYYMMDD), None表示最新
        
        Returns:
            更新结果
        """
        self.logger.info(f"更新龙虎榜数据: {trade_date or '最新'}")
        
        try:
            # 获取数据
            df = self.service.get_dragon_tiger(trade_date)
            
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    self.logger.warning("龙虎榜数据为空")
                    return {'success': False, 'message': '数据为空'}
                
                # 确定日期
                if trade_date is None and 'trade_date' in df.columns:
                    trade_date = df.iloc[0]['trade_date']
                
                if not trade_date:
                    trade_date = datetime.now().strftime('%Y%m%d')
                
                # 保存数据
                file_path = self.dragon_tiger_dir / f"{trade_date}.parquet"
                df.to_parquet(file_path, index=False, compression='zstd')
                
                result = {
                    'success': True,
                    'date': trade_date,
                    'count': len(df),
                    'file': str(file_path)
                }
                
                self.logger.info(f"龙虎榜数据已保存: {len(df)} 条记录")
                
            else:
                result = {'success': False, 'message': '返回数据类型错误'}
            
            # 保存状态
            self._save_status('dragon_tiger', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"龙虎榜更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def update_dragon_tiger_history(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, any]:
        """
        更新历史龙虎榜数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        Returns:
            更新结果
        """
        self.logger.info(f"更新历史龙虎榜: {start_date} - {end_date}")
        
        try:
            df = self.service.get_dragon_tiger_history(start_date, end_date)
            
            if df.empty:
                return {'success': False, 'message': '数据为空'}
            
            # 按日期分组保存
            if 'trade_date' in df.columns:
                for date_val, group in df.groupby('trade_date'):
                    file_path = self.dragon_tiger_dir / f"{date_val}.parquet"
                    group.to_parquet(file_path, index=False, compression='zstd')
            
            result = {
                'success': True,
                'start_date': start_date,
                'end_date': end_date,
                'total_count': len(df)
            }
            
            self.logger.info(f"历史龙虎榜更新完成: {len(df)} 条记录")
            return result
            
        except Exception as e:
            self.logger.error(f"历史龙虎榜更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def update_sector_money_flow(self) -> Dict[str, any]:
        """
        更新板块资金流向
        
        Returns:
            更新结果
        """
        self.logger.info("更新板块资金流向")
        
        results = {}
        
        for sector_type in ['industry', 'concept', 'region']:
            try:
                df = self.service.get_sector_money_flow(sector_type)
                
                if not df.empty:
                    # 保存数据
                    date_str = datetime.now().strftime('%Y%m%d')
                    file_path = self.money_flow_dir / f"sector_{sector_type}_{date_str}.parquet"
                    df.to_parquet(file_path, index=False, compression='zstd')
                    
                    results[sector_type] = {
                        'success': True,
                        'count': len(df),
                        'file': str(file_path)
                    }
                else:
                    results[sector_type] = {'success': False, 'message': '数据为空'}
                    
            except Exception as e:
                self.logger.error(f"{sector_type} 板块资金流向更新失败: {e}")
                results[sector_type] = {'success': False, 'message': str(e)}
        
        # 保存状态
        self._save_status('sector_money_flow', results)
        
        return results
    
    def update_northbound_money_flow(self) -> Dict[str, any]:
        """
        更新北向资金流向
        
        Returns:
            更新结果
        """
        self.logger.info("更新北向资金流向")
        
        try:
            df = self.service.get_northbound_money_flow()
            
            if df.empty:
                return {'success': False, 'message': '数据为空'}
            
            # 保存数据
            date_str = datetime.now().strftime('%Y%m%d')
            file_path = self.northbound_dir / f"money_flow_{date_str}.parquet"
            df.to_parquet(file_path, index=False, compression='zstd')
            
            result = {
                'success': True,
                'count': len(df),
                'file': str(file_path)
            }
            
            self.logger.info(f"北向资金流向已保存: {len(df)} 条记录")
            
            # 保存状态
            self._save_status('northbound_money_flow', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"北向资金流向更新失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def run_daily_update(self) -> Dict[str, any]:
        """
        执行每日更新
        
        更新所有市场行为数据
        
        Returns:
            更新结果汇总
        """
        self.logger.info("开始每日市场行为数据更新")
        
        results = {
            'date': datetime.now().isoformat(),
            'dragon_tiger': None,
            'sector_money_flow': None,
            'northbound_money_flow': None,
        }
        
        # 更新龙虎榜
        results['dragon_tiger'] = self.update_dragon_tiger()
        
        # 更新板块资金流向
        results['sector_money_flow'] = self.update_sector_money_flow()
        
        # 更新北向资金
        results['northbound_money_flow'] = self.update_northbound_money_flow()
        
        # 保存总体状态
        self._save_status('daily_update', results)
        
        self.logger.info("每日市场行为数据更新完成")
        return results
    
    def get_dragon_tiger_by_date(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的龙虎榜数据"""
        file_path = self.dragon_tiger_dir / f"{trade_date}.parquet"
        
        if file_path.exists():
            return pd.read_parquet(file_path)
        
        # 如果本地没有,尝试从API获取
        df = self.service.get_dragon_tiger(trade_date)
        if isinstance(df, pd.DataFrame) and not df.empty:
            # 保存到本地
            df.to_parquet(file_path, index=False, compression='zstd')
            return df
        
        return pd.DataFrame()
    
    def get_latest_dragon_tiger(self) -> pd.DataFrame:
        """获取最新龙虎榜数据"""
        # 查找最新的文件
        files = sorted(self.dragon_tiger_dir.glob("*.parquet"), reverse=True)
        
        if files:
            return pd.read_parquet(files[0])
        
        # 如果没有本地文件,从API获取
        return self.service.get_dragon_tiger()
    
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
    print("测试: 市场行为数据更新任务")
    print("=" * 50)
    
    task = MarketBehaviorUpdateTask()
    
    # 测试龙虎榜更新
    print("\n1. 测试龙虎榜更新:")
    result = task.update_dragon_tiger()
    print(f"结果: {result}")
    
    # 测试板块资金流向
    print("\n2. 测试板块资金流向更新:")
    result = task.update_sector_money_flow()
    print(f"结果: {result}")
