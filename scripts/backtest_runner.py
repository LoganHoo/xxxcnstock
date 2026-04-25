#!/usr/bin/env python3
"""
回测运行器

支持策略回测和参数优化

使用方法:
    python scripts/backtest_runner.py --strategy endstock_pick --start 2024-01-01 --end 2024-12-31
"""
import os
import sys
import yaml
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.backtest_service.engine.backtrader_adapter import BacktraderAdapter
from services.backtest_service.engine.data_feeder import DataFeeder
from services.backtest_service.result_analyzer import ResultAnalyzer
from services.backtest_service.optimization.grid_search import GridSearchOptimizer
from services.backtest_service.optimization.genetic_algorithm import GeneticAlgorithmOptimizer


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BacktestRunner:
    """回测运行器"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.data_feeder = DataFeeder()
        self.result_analyzer = ResultAnalyzer()
        
    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def load_historical_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """加载历史数据"""
        logger.info(f"加载 {code} 历史数据: {start_date} ~ {end_date}")
        
        # 从parquet文件加载
        data_path = f"data/kline/{code}.parquet"
        if not os.path.exists(data_path):
            logger.warning(f"数据文件不存在: {data_path}")
            return pd.DataFrame()
        
        try:
            df = pd.read_parquet(data_path)
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]
            df = df.set_index('trade_date')
            df = df.sort_index()
            return df
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            return pd.DataFrame()
    
    def run_backtest(
        self,
        strategy_class,
        code: str,
        start_date: str,
        end_date: str,
        initial_cash: float = 100000.0,
        commission: float = 0.0003
    ) -> Dict[str, Any]:
        """运行回测"""
        logger.info("=" * 60)
        logger.info("🔄 开始回测")
        logger.info("=" * 60)
        logger.info(f"股票代码: {code}")
        logger.info(f"回测区间: {start_date} ~ {end_date}")
        logger.info(f"初始资金: ¥{initial_cash:,.2f}")
        logger.info(f"手续费率: {commission:.4%}")
        
        # 加载数据
        price_data = self.load_historical_data(code, start_date, end_date)
        if price_data.empty:
            logger.error("无可用数据")
            return {'error': 'No data available'}
        
        logger.info(f"数据条数: {len(price_data)}")
        
        # 配置Backtrader
        config = {
            'initial_cash': initial_cash,
            'commission': commission,
            'slippage': 0.001
        }
        
        adapter = BacktraderAdapter(config)
        
        # 添加数据
        data = self.data_feeder.prepare_data(price_data, name=code)
        adapter.add_data(data, name=code)
        
        # 添加策略
        adapter.add_strategy(strategy_class)
        
        # 运行回测
        result = adapter.run()
        
        # 分析结果
        if 'error' not in result:
            analysis = self.result_analyzer.analyze(result)
            report = self.result_analyzer.generate_report(result)
            
            logger.info("\n" + "=" * 60)
            logger.info("📊 回测结果")
            logger.info("=" * 60)
            logger.info(report)
        
        return result
    
    def run_optimization(
        self,
        strategy_class,
        code: str,
        start_date: str,
        end_date: str,
        param_grid: Dict[str, List],
        method: str = 'grid'
    ) -> tuple:
        """运行参数优化"""
        logger.info("=" * 60)
        logger.info("🔍 开始参数优化")
        logger.info("=" * 60)
        logger.info(f"优化方法: {method}")
        logger.info(f"参数空间: {param_grid}")
        
        # 加载数据
        price_data = self.load_historical_data(code, start_date, end_date)
        if price_data.empty:
            return None, None
        
        # 定义目标函数
        def objective(params):
            # 运行回测
            result = self.run_backtest(
                strategy_class,
                code,
                start_date,
                end_date
            )
            
            if 'error' in result:
                return -999
            
            # 返回夏普比率作为优化目标
            return result.get('sharpe_ratio', 0)
        
        # 选择优化器
        if method == 'grid':
            optimizer = GridSearchOptimizer(param_grid, n_jobs=1)
            best_params, best_score = optimizer.search(objective, maximize=True)
        elif method == 'genetic':
            param_bounds = {k: (min(v), max(v)) for k, v in param_grid.items()}
            param_types = {k: 'float' for k in param_grid}
            
            optimizer = GeneticAlgorithmOptimizer(
                param_bounds=param_bounds,
                param_types=param_types,
                population_size=20,
                generations=10
            )
            best_params, best_score = optimizer.optimize(objective, maximize=True)
        else:
            logger.error(f"未知优化方法: {method}")
            return None, None
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ 优化完成")
        logger.info("=" * 60)
        logger.info(f"最优参数: {best_params}")
        logger.info(f"最优得分: {best_score:.4f}")
        
        return best_params, best_score
    
    def run_multi_stock_backtest(
        self,
        strategy_class,
        codes: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """多股票回测"""
        logger.info("=" * 60)
        logger.info("🔄 开始多股票回测")
        logger.info("=" * 60)
        logger.info(f"股票列表: {codes}")
        
        results = []
        for code in codes:
            result = self.run_backtest(
                strategy_class,
                code,
                start_date,
                end_date
            )
            results.append({
                'code': code,
                'result': result
            })
        
        # 汇总结果
        total_return = np.mean([
            r['result'].get('return_pct', 0) 
            for r in results 
            if 'error' not in r['result']
        ])
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 多股票回测汇总")
        logger.info("=" * 60)
        logger.info(f"平均收益率: {total_return:.2%}")
        
        return {
            'individual_results': results,
            'average_return': total_return
        }


def main():
    parser = argparse.ArgumentParser(description='回测运行器')
    parser.add_argument(
        '--config',
        default='config/simulation_config.yaml',
        help='配置文件路径'
    )
    parser.add_argument(
        '--strategy',
        choices=['endstock_pick', 'dragon_head', 'limitup'],
        default='endstock_pick',
        help='策略类型'
    )
    parser.add_argument(
        '--code',
        default='000001',
        help='股票代码'
    )
    parser.add_argument(
        '--start',
        default='2024-01-01',
        help='开始日期 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        default='2024-12-31',
        help='结束日期 (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--mode',
        choices=['backtest', 'optimize'],
        default='backtest',
        help='运行模式'
    )
    parser.add_argument(
        '--optimize-method',
        choices=['grid', 'genetic'],
        default='grid',
        help='优化方法'
    )
    
    args = parser.parse_args()
    
    # 创建运行器
    runner = BacktestRunner(args.config)
    
    # 根据策略类型导入策略类
    if args.strategy == 'endstock_pick':
        from services.strategy_service.endstock_pick.strategy import EndstockPickStrategy
        strategy_class = EndstockPickStrategy
    elif args.strategy == 'dragon_head':
        from services.strategy_service.dragon_head.strategy import DragonHeadStrategy
        strategy_class = DragonHeadStrategy
    else:
        logger.error(f"未知策略: {args.strategy}")
        return
    
    # 运行
    if args.mode == 'backtest':
        result = runner.run_backtest(
            strategy_class,
            args.code,
            args.start,
            args.end
        )
    elif args.mode == 'optimize':
        param_grid = {
            'period': [5, 10, 20],
            'threshold': [0.01, 0.02, 0.03]
        }
        best_params, best_score = runner.run_optimization(
            strategy_class,
            args.code,
            args.start,
            args.end,
            param_grid,
            method=args.optimize_method
        )


if __name__ == '__main__':
    main()
