#!/usr/bin/env python3
"""
Alpha回测脚本

功能:
1. 基于有效因子构建Alpha策略
2. 计算策略收益、夏普比率、最大回撤
3. 生成回测报告

使用方法:
    python scripts/backtest_alpha.py --strategy fund_behavior --start-date 2024-01-01 --end-date 2024-12-31
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import argparse
import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
import logging

from core.unified_config import config, StrategyConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlphaBacktester:
    """Alpha回测器"""
    
    def __init__(
        self,
        initial_capital: float = 1000000,
        transaction_cost: float = 0.0003,
        position_pct: float = 0.9
    ):
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.position_pct = position_pct
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, start_date: str, end_date: str, data_dir: str = "data") -> pl.DataFrame:
        """加载市场数据"""
        data_path = Path(data_dir) / "kline"
        
        if not data_path.exists():
            logger.error(f"数据目录不存在: {data_path}")
            return pl.DataFrame()
        
        all_data = []
        
        for parquet_file in data_path.glob("*.parquet"):
            try:
                df = pl.read_parquet(parquet_file)
                df = df.filter(
                    (pl.col("trade_date") >= start_date) &
                    (pl.col("trade_date") <= end_date)
                )
                
                if len(df) > 0:
                    all_data.append(df)
            except Exception as e:
                logger.warning(f"加载文件失败 {parquet_file}: {e}")
        
        if not all_data:
            return pl.DataFrame()
        
        return pl.concat(all_data)
    
    def calculate_factor_score(
        self,
        data: pl.DataFrame,
        factor_weights: Dict[str, float]
    ) -> pl.DataFrame:
        """
        计算因子综合评分
        
        Args:
            data: 包含因子值的数据
            factor_weights: 因子权重 {factor_name: weight}
        
        Returns:
            添加了综合评分的数据
        """
        # 标准化因子值
        for factor_name in factor_weights.keys():
            col_name = f"factor_{factor_name}"
            if col_name in data.columns:
                # Z-score标准化
                mean_val = data[col_name].mean()
                std_val = data[col_name].std()
                
                if std_val > 0:
                    data = data.with_columns([
                        ((pl.col(col_name) - mean_val) / std_val).alias(f"{col_name}_zscore")
                    ])
        
        # 计算综合评分
        score_expr = pl.lit(0)
        total_weight = sum(factor_weights.values())
        
        for factor_name, weight in factor_weights.items():
            zscore_col = f"factor_{factor_name}_zscore"
            if zscore_col in data.columns:
                score_expr = score_expr + pl.col(zscore_col) * (weight / total_weight)
        
        data = data.with_columns([
            score_expr.alias("composite_score")
        ])
        
        return data
    
    def select_stocks(
        self,
        data: pl.DataFrame,
        date: str,
        top_n: int = 20
    ) -> List[str]:
        """
        选股
        
        Args:
            data: 数据
            date: 日期
            top_n: 选股数量
        
        Returns:
            选中的股票代码列表
        """
        day_data = data.filter(pl.col("trade_date") == date)
        
        if len(day_data) == 0:
            return []
        
        # 按综合评分排序
        selected = day_data.sort("composite_score", descending=True).head(top_n)
        
        return selected["code"].to_list()
    
    def run_backtest(
        self,
        data: pl.DataFrame,
        factor_weights: Dict[str, float],
        rebalance_freq: int = 5,  # 调仓频率(天)
        top_n: int = 20
    ) -> Dict:
        """
        运行回测
        
        Args:
            data: 市场数据
            factor_weights: 因子权重
            rebalance_freq: 调仓频率
            top_n: 选股数量
        
        Returns:
            回测结果
        """
        # 计算因子评分
        data = self.calculate_factor_score(data, factor_weights)
        
        # 获取所有交易日
        dates = data['trade_date'].unique().sort().to_list()
        
        if len(dates) < 2:
            logger.error("数据不足")
            return {}
        
        # 回测状态
        capital = self.initial_capital
        positions = {}  # 持仓 {code: shares}
        portfolio_values = []
        trades = []
        
        for i, date in enumerate(dates):
            # 获取当日数据
            day_data = data.filter(pl.col("trade_date") == date)
            
            # 计算当前持仓市值
            position_value = 0
            for code, shares in positions.items():
                stock_data = day_data.filter(pl.col("code") == code)
                if len(stock_data) > 0:
                    price = stock_data['close'][0]
                    position_value += shares * price
            
            total_value = capital + position_value
            portfolio_values.append({
                'date': date,
                'value': total_value,
                'cash': capital,
                'position_value': position_value
            })
            
            # 调仓日
            if i % rebalance_freq == 0 and i < len(dates) - 1:
                # 选股
                selected_stocks = self.select_stocks(data, date, top_n)
                
                if selected_stocks:
                    # 清空现有持仓
                    for code, shares in positions.items():
                        stock_data = day_data.filter(pl.col("code") == code)
                        if len(stock_data) > 0:
                            price = stock_data['close'][0]
                            sell_value = shares * price * (1 - self.transaction_cost)
                            capital += sell_value
                            trades.append({
                                'date': date,
                                'code': code,
                                'action': 'sell',
                                'shares': shares,
                                'price': price
                            })
                    
                    positions = {}
                    
                    # 买入新选股
                    position_size = (total_value * self.position_pct) / len(selected_stocks)
                    
                    for code in selected_stocks:
                        stock_data = day_data.filter(pl.col("code") == code)
                        if len(stock_data) > 0:
                            price = stock_data['close'][0]
                            if price > 0:
                                shares = int(position_size / price)
                                cost = shares * price * (1 + self.transaction_cost)
                                
                                if capital >= cost:
                                    capital -= cost
                                    positions[code] = shares
                                    trades.append({
                                        'date': date,
                                        'code': code,
                                        'action': 'buy',
                                        'shares': shares,
                                        'price': price
                                    })
        
        # 计算回测指标
        return self._calculate_metrics(portfolio_values, trades)
    
    def _calculate_metrics(
        self,
        portfolio_values: List[Dict],
        trades: List[Dict]
    ) -> Dict:
        """计算回测指标"""
        if len(portfolio_values) < 2:
            return {}
        
        values = np.array([p['value'] for p in portfolio_values])
        dates = [p['date'] for p in portfolio_values]
        
        # 总收益率
        total_return = (values[-1] - values[0]) / values[0]
        
        # 日收益率
        daily_returns = np.diff(values) / values[:-1]
        
        # 年化收益率
        n_days = len(portfolio_values)
        annual_return = (1 + total_return) ** (252 / n_days) - 1
        
        # 年化波动率
        annual_volatility = np.std(daily_returns) * np.sqrt(252)
        
        # 夏普比率
        sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else 0
        
        # 最大回撤
        cumulative = np.cumprod(1 + daily_returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = np.min(drawdown)
        
        # 胜率
        win_rate = np.sum(daily_returns > 0) / len(daily_returns)
        
        # 交易次数
        n_trades = len(trades)
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'n_trades': n_trades,
            'portfolio_values': portfolio_values,
            'trades': trades,
            'daily_returns': daily_returns
        }


def print_backtest_report(results: Dict):
    """打印回测报告"""
    print("\n" + "=" * 80)
    print("Alpha回测报告")
    print("=" * 80)
    
    print(f"\n【收益指标】")
    print(f"  总收益率:      {results['total_return']:>10.2%}")
    print(f"  年化收益率:    {results['annual_return']:>10.2%}")
    print(f"  年化波动率:    {results['annual_volatility']:>10.2%}")
    
    print(f"\n【风险指标】")
    print(f"  夏普比率:      {results['sharpe_ratio']:>10.4f}")
    print(f"  最大回撤:      {results['max_drawdown']:>10.2%}")
    print(f"  日胜率:        {results['win_rate']:>10.2%}")
    
    print(f"\n【交易统计】")
    print(f"  交易次数:      {results['n_trades']:>10}")
    
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Alpha回测工具')
    parser.add_argument('--strategy', type=str, default='fund_behavior',
                       help='策略名称')
    parser.add_argument('--start-date', type=str, default='2024-01-01',
                       help='开始日期')
    parser.add_argument('--end-date', type=str, default='2024-12-31',
                       help='结束日期')
    parser.add_argument('--top-n', type=int, default=20,
                       help='选股数量')
    parser.add_argument('--rebalance', type=int, default=5,
                       help='调仓频率(天)')
    parser.add_argument('--data-dir', type=str, default='data',
                       help='数据目录')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("Alpha回测")
    print("=" * 80)
    print(f"策略: {args.strategy}")
    print(f"回测区间: {args.start_date} ~ {args.end_date}")
    print(f"选股数量: {args.top_n}")
    print(f"调仓频率: {args.rebalance}天")
    print()
    
    # 1. 获取策略因子权重
    factor_weights = StrategyConfig.get_factor_weights(args.strategy, 'trend')
    
    if not factor_weights:
        logger.error(f"未找到策略因子配置: {args.strategy}")
        return
    
    print("【使用因子权重】")
    for factor, weight in factor_weights.items():
        print(f"  {factor}: {weight}")
    print()
    
    # 2. 加载数据
    backtester = AlphaBacktester()
    data = backtester.load_data(args.start_date, args.end_date, args.data_dir)
    
    if len(data) == 0:
        logger.error("未加载到数据")
        return
    
    logger.info(f"加载数据: {len(data)} 条记录")
    
    # 3. 运行回测
    results = backtester.run_backtest(
        data,
        factor_weights,
        rebalance_freq=args.rebalance,
        top_n=args.top_n
    )
    
    if results:
        print_backtest_report(results)
    else:
        logger.error("回测失败")


if __name__ == "__main__":
    main()
