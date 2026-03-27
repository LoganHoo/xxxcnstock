"""
多策略组合测试
比较不同因子组合的回测效果
"""
import sys
from pathlib import Path
import argparse
import polars as pl
import json
from datetime import datetime
from typing import Dict, List
import logging
import yaml

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import factors
from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


STRATEGY_PRESETS = {
    "trend_only": {
        "name": "纯趋势策略",
        "description": "只使用趋势类因子",
        "factors": [
            {"name": "ma_trend", "weight": 0.50, "threshold": 45},
            {"name": "macd", "weight": 0.50, "threshold": 35}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 40}
    },
    
    "momentum": {
        "name": "动量策略",
        "description": "使用动量和超买超卖因子",
        "factors": [
            {"name": "rsi", "weight": 0.35, "threshold": 30},
            {"name": "kdj", "weight": 0.35, "threshold": 35},
            {"name": "macd", "weight": 0.30, "threshold": 35}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 40}
    },
    
    "volume_price": {
        "name": "量价策略",
        "description": "侧重成交量分析",
        "factors": [
            {"name": "volume_ratio", "weight": 0.40, "threshold": 35},
            {"name": "turnover", "weight": 0.30, "threshold": 35},
            {"name": "obv", "weight": 0.30, "threshold": 35}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 40}
    },
    
    "volatility": {
        "name": "波动率策略",
        "description": "利用波动率指标",
        "factors": [
            {"name": "bollinger", "weight": 0.40, "threshold": 35},
            {"name": "atr", "weight": 0.30, "threshold": 35},
            {"name": "rsi", "weight": 0.30, "threshold": 30}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 40}
    },
    
    "balanced": {
        "name": "均衡策略",
        "description": "均衡配置各类因子",
        "factors": [
            {"name": "ma_trend", "weight": 0.20, "threshold": 40},
            {"name": "macd", "weight": 0.15, "threshold": 35},
            {"name": "rsi", "weight": 0.15, "threshold": 30},
            {"name": "volume_ratio", "weight": 0.20, "threshold": 35},
            {"name": "kdj", "weight": 0.15, "threshold": 35},
            {"name": "bollinger", "weight": 0.15, "threshold": 35}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 40}
    },
    
    "aggressive": {
        "name": "激进策略",
        "description": "追求高收益高风险",
        "factors": [
            {"name": "kdj", "weight": 0.30, "threshold": 25},
            {"name": "rsi", "weight": 0.25, "threshold": 25},
            {"name": "volume_ratio", "weight": 0.25, "threshold": 30},
            {"name": "bollinger", "weight": 0.20, "threshold": 30}
        ],
        "filters": [{"type": "price", "min": 3, "max": 100}],
        "output": {"top_n": 20, "min_score": 35}
    },
    
    "conservative": {
        "name": "保守策略",
        "description": "稳健低风险",
        "factors": [
            {"name": "ma_trend", "weight": 0.35, "threshold": 55},
            {"name": "macd", "weight": 0.30, "threshold": 45},
            {"name": "obv", "weight": 0.20, "threshold": 45},
            {"name": "atr", "weight": 0.15, "threshold": 50}
        ],
        "filters": [{"type": "price", "min": 5, "max": 50}],
        "output": {"top_n": 15, "min_score": 50}
    }
}


class StrategyComparator:
    """策略比较器"""
    
    def __init__(self, stock_data: pl.DataFrame):
        self.stock_data = stock_data
        self.results = {}
    
    def compare_strategies(
        self,
        strategy_names: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        initial_capital: float = 1000000,
        position_size: int = 5,
        holding_days: int = 5,
        sample_size: int = 500
    ) -> Dict[str, dict]:
        """
        比较多个策略
        
        Args:
            strategy_names: 策略名称列表
            start_date: 回测开始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            position_size: 持仓数量
            holding_days: 持仓天数
            sample_size: 采样股票数量
        
        Returns:
            各策略的回测结果
        """
        if strategy_names is None:
            strategy_names = list(STRATEGY_PRESETS.keys())
        
        logger.info(f"开始比较 {len(strategy_names)} 个策略...")
        
        for name in strategy_names:
            if name not in STRATEGY_PRESETS:
                logger.warning(f"策略 {name} 不存在，跳过")
                continue
            
            logger.info(f"\n{'='*50}")
            logger.info(f"测试策略: {STRATEGY_PRESETS[name]['name']}")
            logger.info(f"{'='*50}")
            
            result = self._backtest_strategy(
                name,
                start_date,
                end_date,
                initial_capital,
                position_size,
                holding_days,
                sample_size
            )
            
            self.results[name] = result
        
        return self.results
    
    def _backtest_strategy(
        self,
        strategy_name: str,
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
        
        strategy_config = STRATEGY_PRESETS[strategy_name]
        
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
            
            return self._calculate_metrics(daily_values, trades, initial_capital, strategy_config)
        
        finally:
            os.unlink(temp_path)
    
    def _select_stocks(self, data: pl.DataFrame, codes: List[str], date: str, strategy_engine: StrategyEngine) -> pl.DataFrame:
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
    
    def _calculate_metrics(self, daily_values: list, trades: list, initial_capital: float, strategy_config: dict) -> dict:
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
            "strategy_name": strategy_config["name"],
            "description": strategy_config["description"],
            "factors": [f["name"] for f in strategy_config["factors"]],
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "final_value": final_value,
            "total_trades": len(trades)
        }
    
    def get_ranking(self, metric: str = "total_return") -> List[tuple]:
        """获取策略排名"""
        ranking = sorted(
            self.results.items(),
            key=lambda x: x[1].get(metric, 0),
            reverse=True
        )
        return ranking
    
    def save_results(self, output_path: str):
        """保存结果"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"结果已保存到 {output_path}")


def main():
    parser = argparse.ArgumentParser(description="多策略组合测试")
    parser.add_argument("--strategies", "-s", nargs="+", help="策略名称列表")
    parser.add_argument("--output", "-o", default="reports/strategy_comparison.json")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-03-26")
    parser.add_argument("--capital", type=float, default=1000000)
    parser.add_argument("--positions", type=int, default=5)
    parser.add_argument("--sample", type=int, default=300, help="采样股票数量")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("多策略组合测试系统")
    print("=" * 60)
    
    print("\n可用策略:")
    for name, config in STRATEGY_PRESETS.items():
        print(f"  - {name}: {config['name']} ({config['description']})")
    
    print("\n加载股票数据...")
    kline_pattern = str(project_root / "data" / "kline" / "*.parquet")
    stock_data = pl.read_parquet(kline_pattern)
    print(f"加载了 {len(stock_data)} 条记录")
    
    comparator = StrategyComparator(stock_data)
    
    results = comparator.compare_strategies(
        strategy_names=args.strategies,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        position_size=args.positions,
        sample_size=args.sample
    )
    
    print("\n" + "=" * 60)
    print("策略比较结果 (按总收益率排序)")
    print("=" * 60)
    
    ranking = comparator.get_ranking("total_return")
    
    print(f"\n{'排名':<4} {'策略名称':<15} {'总收益率':<12} {'年化收益':<12} {'最大回撤':<12} {'夏普比率':<10} {'胜率':<10}")
    print("-" * 75)
    
    for i, (name, result) in enumerate(ranking):
        print(f"#{i+1:<3} {result['strategy_name']:<15} "
              f"{result['total_return']:>10.2%}  "
              f"{result['annual_return']:>10.2%}  "
              f"{result['max_drawdown']:>10.2%}  "
              f"{result['sharpe_ratio']:>8.2f}  "
              f"{result['win_rate']:>8.2%}")
    
    print("\n" + "=" * 60)
    print("各策略因子组合")
    print("=" * 60)
    
    for name, result in results.items():
        print(f"\n{result['strategy_name']}:")
        print(f"  因子: {', '.join(result['factors'])}")
    
    comparator.save_results(args.output)


if __name__ == "__main__":
    main()
