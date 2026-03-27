"""
策略优化器
通过网格搜索和回测找到最优参数组合
"""
import sys
from pathlib import Path
import argparse
import polars as pl
import json
from datetime import datetime
from itertools import product
from typing import Dict, List, Any, Tuple
import logging
import yaml

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import factors
from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StrategyOptimizer:
    """策略优化器"""
    
    def __init__(self, stock_data: pl.DataFrame):
        self.stock_data = stock_data
        self.results = []
    
    def grid_search(
        self,
        base_strategy: dict,
        param_grid: Dict[str, List[Any]],
        start_date: str = None,
        end_date: str = None,
        initial_capital: float = 1000000,
        position_size: int = 5,
        holding_days: int = 5,
        sample_size: int = 500
    ) -> List[dict]:
        """
        网格搜索最优参数
        
        Args:
            base_strategy: 基础策略配置
            param_grid: 参数网格 {"factor_name__weight": [0.1, 0.2, 0.3], ...}
            start_date: 回测开始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            position_size: 持仓数量
            holding_days: 持仓天数
            sample_size: 采样股票数量
        
        Returns:
            所有参数组合的回测结果
        """
        logger.info("开始网格搜索参数优化...")
        
        param_keys = list(param_grid.keys())
        param_values = list(param_grid.values())
        param_combinations = list(product(*param_values))
        
        total_combinations = len(param_combinations)
        logger.info(f"共 {total_combinations} 种参数组合")
        
        for i, combo in enumerate(param_combinations):
            params = dict(zip(param_keys, combo))
            
            strategy_config = self._apply_params(base_strategy.copy(), params)
            
            logger.info(f"[{i+1}/{total_combinations}] 测试参数: {params}")
            
            result = self._backtest_strategy(
                strategy_config,
                start_date,
                end_date,
                initial_capital,
                position_size,
                holding_days,
                sample_size
            )
            
            result["params"] = params
            self.results.append(result)
        
        self.results.sort(key=lambda x: x.get("total_return", 0), reverse=True)
        
        return self.results
    
    def _apply_params(self, strategy: dict, params: dict) -> dict:
        """应用参数到策略配置"""
        for key, value in params.items():
            parts = key.split("__")
            
            if len(parts) == 2:
                factor_name, param_type = parts
                
                for factor in strategy.get("factors", []):
                    if factor["name"] == factor_name:
                        if param_type in factor:
                            factor[param_type] = value
                        elif param_type == "weight":
                            factor["weight"] = value
                        elif param_type == "threshold":
                            factor["threshold"] = value
        
        return strategy
    
    def _backtest_strategy(
        self,
        strategy_config: dict,
        start_date: str,
        end_date: str,
        initial_capital: float,
        position_size: int,
        holding_days: int,
        sample_size: int
    ) -> dict:
        """回测单个策略"""
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({"strategy": strategy_config}, f)
            temp_path = f.name
        
        try:
            factor_engine = FactorEngine()
            strategy_engine = StrategyEngine(temp_path, factor_engine)
            
            data = self.stock_data.clone()
            if start_date:
                data = data.filter(pl.col("trade_date") >= start_date)
            if end_date:
                data = data.filter(pl.col("trade_date") <= end_date)
            
            trade_dates = data.select("trade_date").unique().sort("trade_date")["trade_date"].to_list()
            codes = data.select("code").unique()["code"].to_list()[:sample_size]
            
            cash = initial_capital
            positions = {}
            daily_values = []
            trades = []
            
            for i, date in enumerate(trade_dates):
                if i % holding_days != 0:
                    continue
                
                current_data = data.filter(pl.col("trade_date") == date)
                
                if len(current_data) == 0:
                    continue
                
                for code in list(positions.keys()):
                    pos = positions[code]
                    sell_price = current_data.filter(pl.col("code") == code)
                    
                    if len(sell_price) > 0:
                        sell_price = sell_price["close"].item()
                        cash += pos["shares"] * sell_price
                        
                        trades.append({
                            "date": date,
                            "code": code,
                            "action": "sell",
                            "price": sell_price,
                            "shares": pos["shares"]
                        })
                
                positions.clear()
                
                selected = self._select_stocks(data, codes, date, strategy_engine)
                
                if len(selected) > 0:
                    buy_codes = selected.head(position_size)["code"].to_list()
                    capital_per_stock = cash / position_size
                    
                    for code in buy_codes:
                        stock_price = current_data.filter(pl.col("code") == code)
                        
                        if len(stock_price) > 0:
                            buy_price = stock_price["close"].item()
                            shares = int(capital_per_stock / buy_price / 100) * 100
                            
                            if shares > 0:
                                cost = shares * buy_price
                                cash -= cost
                                positions[code] = {"shares": shares, "buy_price": buy_price}
                                
                                trades.append({
                                    "date": date,
                                    "code": code,
                                    "action": "buy",
                                    "price": buy_price,
                                    "shares": shares
                                })
                
                position_value = sum(
                    pos["shares"] * current_data.filter(pl.col("code") == code)["close"].item()
                    for code, pos in positions.items()
                    if len(current_data.filter(pl.col("code") == code)) > 0
                )
                
                daily_values.append({
                    "date": date,
                    "total_value": cash + position_value
                })
            
            return self._calculate_metrics(daily_values, trades, initial_capital)
        
        finally:
            os.unlink(temp_path)
    
    def _select_stocks(
        self,
        data: pl.DataFrame,
        codes: List[str],
        date: str,
        strategy_engine: StrategyEngine
    ) -> pl.DataFrame:
        """选股"""
        results = []
        
        for code in codes:
            code_data = data.filter(
                (pl.col("code") == code) & 
                (pl.col("trade_date") <= date)
            ).sort("trade_date").tail(50)
            
            if len(code_data) < 30:
                continue
            
            try:
                selected = strategy_engine.select_stocks(code_data)
                if len(selected) > 0:
                    latest = selected.sort("trade_date", descending=True).head(1)
                    results.append(latest)
            except:
                pass
        
        if results:
            return pl.concat(results).sort("strategy_score", descending=True)
        return pl.DataFrame()
    
    def _calculate_metrics(self, daily_values: list, trades: list, initial_capital: float) -> dict:
        """计算回测指标"""
        if not daily_values:
            return {"total_return": 0, "annual_return": 0, "max_drawdown": 0, "sharpe_ratio": 0, "win_rate": 0}
        
        df = pl.DataFrame(daily_values)
        
        final_value = df["total_value"].tail(1).item()
        total_return = (final_value - initial_capital) / initial_capital
        
        days = len(df)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0
        
        df = df.with_columns([pl.col("total_value").cum_max().alias("cummax")])
        df = df.with_columns([((pl.col("cummax") - pl.col("total_value")) / pl.col("cummax")).alias("drawdown")])
        max_drawdown = df["drawdown"].max()
        
        df = df.with_columns([pl.col("total_value").pct_change().alias("daily_return")])
        daily_returns = df["daily_return"].drop_nulls()
        
        if len(daily_returns) > 0 and daily_returns.std() > 0:
            sharpe_ratio = (daily_returns.mean() * 252 - 0.03) / (daily_returns.std() * (252 ** 0.5))
        else:
            sharpe_ratio = 0
        
        buy_trades = [t for t in trades if t["action"] == "buy"]
        sell_trades = [t for t in trades if t["action"] == "sell"]
        
        wins = 0
        for buy, sell in zip(buy_trades, sell_trades):
            if sell["price"] > buy["price"]:
                wins += 1
        
        win_rate = wins / len(buy_trades) if buy_trades else 0
        
        return {
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "final_value": final_value
        }
    
    def get_best_params(self, top_n: int = 5) -> List[dict]:
        """获取最优参数"""
        return self.results[:top_n]
    
    def save_results(self, output_path: str):
        """保存结果"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"结果已保存到 {output_path}")


def main():
    parser = argparse.ArgumentParser(description="策略参数优化")
    parser.add_argument("--output", "-o", default="reports/optimization_result.json")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-03-26")
    parser.add_argument("--capital", type=float, default=1000000)
    parser.add_argument("--positions", type=int, default=5)
    parser.add_argument("--sample", type=int, default=300, help="采样股票数量")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("策略参数优化系统")
    print("=" * 60)
    
    print("\n加载股票数据...")
    kline_pattern = str(project_root / "data" / "kline" / "*.parquet")
    stock_data = pl.read_parquet(kline_pattern)
    print(f"加载了 {len(stock_data)} 条记录")
    
    base_strategy = {
        "name": "优化策略",
        "description": "参数优化测试",
        "version": "1.0",
        "factors": [
            {"name": "ma_trend", "weight": 0.25, "threshold": 45},
            {"name": "macd", "weight": 0.20, "threshold": 35},
            {"name": "rsi", "weight": 0.15, "threshold": 30},
            {"name": "kdj", "weight": 0.10, "threshold": 35},
            {"name": "bollinger", "weight": 0.10, "threshold": 35},
            {"name": "volume_ratio", "weight": 0.20, "threshold": 35}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 45}
    }
    
    param_grid = {
        "ma_trend__weight": [0.20, 0.25, 0.30],
        "macd__weight": [0.15, 0.20, 0.25],
        "rsi__weight": [0.10, 0.15, 0.20],
        "volume_ratio__weight": [0.15, 0.20, 0.25],
        "ma_trend__threshold": [40, 45, 50],
        "macd__threshold": [30, 35, 40]
    }
    
    optimizer = StrategyOptimizer(stock_data)
    
    results = optimizer.grid_search(
        base_strategy=base_strategy,
        param_grid=param_grid,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        position_size=args.positions,
        sample_size=args.sample
    )
    
    print("\n" + "=" * 60)
    print("优化结果 Top 5")
    print("=" * 60)
    
    for i, result in enumerate(optimizer.get_best_params(5)):
        print(f"\n#{i+1}")
        print(f"参数: {result['params']}")
        print(f"总收益率: {result['total_return']:.2%}")
        print(f"年化收益: {result['annual_return']:.2%}")
        print(f"最大回撤: {result['max_drawdown']:.2%}")
        print(f"夏普比率: {result['sharpe_ratio']:.2f}")
        print(f"胜率: {result['win_rate']:.2%}")
    
    optimizer.save_results(args.output)


if __name__ == "__main__":
    main()
