#!/usr/bin/env python3
"""
工作流运行器

统一的工作流运行入口:
- 支持所有业务工作流
- 命令行接口
- 定时任务支持
- 工作流编排
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from enum import Enum

from core.logger import setup_logger
from core.paths import get_data_path

from workflows.data_collection_workflow import DataCollectionWorkflow, CollectionType
from workflows.stock_selection_workflow import StockSelectionWorkflow, StrategyType
from workflows.backtest_workflow import BacktestWorkflow, RebalanceFrequency, PositionSizing
from workflows.daily_operation_workflow import DailyOperationWorkflow, OperationTask


class WorkflowType(Enum):
    """工作流类型"""
    DATA_COLLECTION = "data_collection"
    STOCK_SELECTION = "stock_selection"
    BACKTEST = "backtest"
    DAILY_OPERATION = "daily_operation"


class WorkflowRunner:
    """工作流运行器"""
    
    def __init__(self):
        """初始化工作流运行器"""
        self.logger = setup_logger("workflow_runner")
        
        # 初始化各工作流
        self.data_collection = DataCollectionWorkflow()
        self.stock_selection = StockSelectionWorkflow()
        self.backtest = BacktestWorkflow()
        self.daily_operation = DailyOperationWorkflow()
        
        self.results_dir = get_data_path() / "workflow_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self,
            workflow_type: WorkflowType,
            params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        运行指定工作流
        
        Args:
            workflow_type: 工作流类型
            params: 工作流参数
        
        Returns:
            运行结果
        """
        params = params or {}
        
        self.logger.info(f"运行工作流: {workflow_type.value}")
        
        if workflow_type == WorkflowType.DATA_COLLECTION:
            return self._run_data_collection(params)
        elif workflow_type == WorkflowType.STOCK_SELECTION:
            return self._run_stock_selection(params)
        elif workflow_type == WorkflowType.BACKTEST:
            return self._run_backtest(params)
        elif workflow_type == WorkflowType.DAILY_OPERATION:
            return self._run_daily_operation(params)
        else:
            raise ValueError(f"未知的工作流类型: {workflow_type}")
    
    def _run_data_collection(self, params: Dict) -> Dict:
        """运行数据采集工作流"""
        collection_type = CollectionType(params.get('type', 'all'))
        date = params.get('date')
        codes = params.get('codes')
        validate = params.get('validate', True)
        
        result = self.data_collection.run(
            collection_type=collection_type,
            date=date,
            codes=codes,
            validate=validate
        )
        
        return {
            'workflow': 'data_collection',
            'results': {k: v.to_dict() for k, v in result.items()}
        }
    
    def _run_stock_selection(self, params: Dict) -> Dict:
        """运行选股策略工作流"""
        strategy_type = StrategyType(params.get('strategy', 'comprehensive'))
        universe = params.get('universe')
        top_n = params.get('top_n', 50)
        date = params.get('date')
        
        result = self.stock_selection.run(
            strategy_type=strategy_type,
            universe=universe,
            top_n=top_n,
            date=date
        )
        
        return {
            'workflow': 'stock_selection',
            'result': result.to_dict()
        }
    
    def _run_backtest(self, params: Dict) -> Dict:
        """运行回测验证工作流"""
        strategy_type = StrategyType(params.get('strategy', 'comprehensive'))
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        initial_capital = params.get('initial_capital', 1000000)
        rebalance_frequency = RebalanceFrequency(params.get('rebalance', 'weekly'))
        top_n = params.get('top_n', 20)
        
        result = self.backtest.run(
            strategy_type=strategy_type,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            rebalance_frequency=rebalance_frequency,
            top_n=top_n
        )
        
        return {
            'workflow': 'backtest',
            'result': result.to_dict()
        }
    
    def _run_daily_operation(self, params: Dict) -> Dict:
        """运行日常运营工作流"""
        tasks = params.get('tasks', ['all'])
        date = params.get('date')
        skip_market_check = params.get('skip_market_check', False)
        
        # 转换任务名称
        task_map = {
            'data_update': OperationTask.DATA_UPDATE,
            'quality_check': OperationTask.QUALITY_CHECK,
            'health_check': OperationTask.HEALTH_CHECK,
            'audit_report': OperationTask.AUDIT_REPORT,
            'cleanup': OperationTask.CLEANUP,
            'all': OperationTask.ALL
        }
        operation_tasks = [task_map.get(t, OperationTask.ALL) for t in tasks]
        
        result = self.daily_operation.run(
            tasks=operation_tasks,
            date=date,
            skip_market_check=skip_market_check
        )
        
        return {
            'workflow': 'daily_operation',
            'result': result.to_dict()
        }
    
    def run_batch(self, workflows: List[Dict]) -> List[Dict]:
        """
        批量运行多个工作流
        
        Args:
            workflows: 工作流配置列表
        
        Returns:
            结果列表
        """
        results = []
        
        for workflow_config in workflows:
            workflow_type = WorkflowType(workflow_config['type'])
            params = workflow_config.get('params', {})
            
            try:
                result = self.run(workflow_type, params)
                results.append({
                    'status': 'success',
                    'workflow': workflow_type.value,
                    'result': result
                })
            except Exception as e:
                self.logger.error(f"工作流运行失败 {workflow_type.value}: {e}")
                results.append({
                    'status': 'failed',
                    'workflow': workflow_type.value,
                    'error': str(e)
                })
        
        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='业务工作流运行器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 运行数据采集
  python -m workflows.workflow_runner data_collection --type all --date 2024-01-01
  
  # 运行选股策略
  python -m workflows.workflow_runner stock_selection --strategy comprehensive --top-n 50
  
  # 运行回测
  python -m workflows.workflow_runner backtest --strategy value_growth --start-date 2023-01-01 --end-date 2023-12-31
  
  # 运行日常运营
  python -m workflows.workflow_runner daily_operation --tasks data_update quality_check
  
  # 批量运行 (通过配置文件)
  python -m workflows.workflow_runner batch --config workflows.json
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 数据采集命令
    collection_parser = subparsers.add_parser('data_collection', help='数据采集工作流')
    collection_parser.add_argument('--type', choices=['financial', 'market_behavior', 'announcement', 'all'],
                                  default='all', help='采集类型')
    collection_parser.add_argument('--date', help='指定日期 (YYYY-MM-DD)')
    collection_parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    collection_parser.add_argument('--no-validate', action='store_true', help='跳过验证')
    
    # 选股策略命令
    selection_parser = subparsers.add_parser('stock_selection', help='选股策略工作流')
    selection_parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                                 default='comprehensive', help='策略类型')
    selection_parser.add_argument('--top-n', type=int, default=50, help='输出数量')
    selection_parser.add_argument('--date', help='选股日期 (YYYY-MM-DD)')
    selection_parser.add_argument('--codes', help='指定股票代码 (逗号分隔)')
    
    # 回测命令
    backtest_parser = subparsers.add_parser('backtest', help='回测验证工作流')
    backtest_parser.add_argument('--strategy', choices=['value_growth', 'main_force', 'event_driven', 'comprehensive'],
                                default='comprehensive', help='策略类型')
    backtest_parser.add_argument('--start-date', required=True, help='开始日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--end-date', required=True, help='结束日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--initial-capital', type=float, default=1000000, help='初始资金')
    backtest_parser.add_argument('--rebalance', choices=['daily', 'weekly', 'monthly', 'quarterly'],
                                default='weekly', help='调仓频率')
    backtest_parser.add_argument('--top-n', type=int, default=20, help='选股数量')
    
    # 日常运营命令
    operation_parser = subparsers.add_parser('daily_operation', help='日常运营工作流')
    operation_parser.add_argument('--tasks', nargs='+',
                                 choices=['data_update', 'quality_check', 'health_check', 'audit_report', 'cleanup', 'all'],
                                 default=['all'], help='要执行的任务')
    operation_parser.add_argument('--date', help='运营日期 (YYYY-MM-DD)')
    operation_parser.add_argument('--skip-market-check', action='store_true', help='跳过市场状态检查')
    
    # 批量运行命令
    batch_parser = subparsers.add_parser('batch', help='批量运行工作流')
    batch_parser.add_argument('--config', required=True, help='配置文件路径 (JSON格式)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 创建运行器
    runner = WorkflowRunner()
    
    # 执行命令
    if args.command == 'data_collection':
        params = {
            'type': args.type,
            'date': args.date,
            'codes': args.codes.split(',') if args.codes else None,
            'validate': not args.no_validate
        }
        result = runner.run(WorkflowType.DATA_COLLECTION, params)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.command == 'stock_selection':
        params = {
            'strategy': args.strategy,
            'top_n': args.top_n,
            'date': args.date,
            'universe': args.codes.split(',') if args.codes else None
        }
        result = runner.run(WorkflowType.STOCK_SELECTION, params)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.command == 'backtest':
        params = {
            'strategy': args.strategy,
            'start_date': args.start_date,
            'end_date': args.end_date,
            'initial_capital': args.initial_capital,
            'rebalance': args.rebalance,
            'top_n': args.top_n
        }
        result = runner.run(WorkflowType.BACKTEST, params)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.command == 'daily_operation':
        params = {
            'tasks': args.tasks,
            'date': args.date,
            'skip_market_check': args.skip_market_check
        }
        result = runner.run(WorkflowType.DAILY_OPERATION, params)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.command == 'batch':
        # 读取配置文件
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        workflows = config.get('workflows', [])
        results = runner.run_batch(workflows)
        
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
