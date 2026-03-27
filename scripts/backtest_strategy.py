"""
策略回测脚本
"""
import sys
from pathlib import Path
import argparse
import polars as pl
import json
from datetime import datetime
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.factor_engine import FactorEngine
from core.strategy_engine import StrategyEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, strategy_config: str):
        self.factor_engine = FactorEngine()
        self.strategy_engine = StrategyEngine(strategy_config, self.factor_engine)
        self.logger = logging.getLogger(__name__)
    
    def run(
        self, 
        stock_data: pl.DataFrame,
        start_date: str = None,
        end_date: str = None,
        initial_capital: float = 1000000,
        position_size: int = 5,
        holding_days: int = 5
    ) -> dict:
        """
        运行回测
        
        Args:
            stock_data: 股票K线数据
            start_date: 回测开始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            position_size: 持仓股票数量
            holding_days: 持仓天数
        """
        self.logger.info(f"开始回测: {start_date} ~ {end_date}")
        
        if start_date:
            stock_data = stock_data.filter(pl.col("trade_date") >= start_date)
        if end_date:
            stock_data = stock_data.filter(pl.col("trade_date") <= end_date)
        
        trade_dates = stock_data.select("trade_date").unique().sort("trade_date")["trade_date"].to_list()
        codes = stock_data.select("code").unique()["code"].to_list()
        
        trades = []
        daily_values = []
        cash = initial_capital
        positions = {}
        
        for i, date in enumerate(trade_dates):
            if i % holding_days != 0:
                continue
            
            current_data = stock_data.filter(pl.col("trade_date") == date)
            
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
                        "shares": pos["shares"],
                        "value": pos["shares"] * sell_price
                    })
            
            positions.clear()
            
            selected = self._select_stocks_for_date(stock_data, codes, date)
            
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
                                "shares": shares,
                                "value": cost
                            })
            
            position_value = sum(
                pos["shares"] * current_data.filter(pl.col("code") == code)["close"].item()
                for code, pos in positions.items()
                if len(current_data.filter(pl.col("code") == code)) > 0
            )
            
            total_value = cash + position_value
            daily_values.append({
                "date": date,
                "cash": cash,
                "position_value": position_value,
                "total_value": total_value
            })
        
        return self._calculate_metrics(daily_values, trades, initial_capital)
    
    def _select_stocks_for_date(self, stock_data: pl.DataFrame, codes: list, date: str) -> pl.DataFrame:
        """为特定日期选股"""
        results = []
        
        for code in codes[:500]:
            code_data = stock_data.filter(
                (pl.col("code") == code) & 
                (pl.col("trade_date") <= date)
            ).sort("trade_date").tail(50)
            
            if len(code_data) < 30:
                continue
            
            try:
                selected = self.strategy_engine.select_stocks(code_data)
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
            profit = (sell["price"] - buy["price"]) * buy["shares"]
            if profit > 0:
                wins += 1
        
        win_rate = wins / len(buy_trades) if buy_trades else 0
        
        return {
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "total_trades": len(trades),
            "final_value": final_value
        }


def main():
    parser = argparse.ArgumentParser(description="策略回测")
    parser.add_argument("--strategy", "-s", default="config/strategies/trend_following.yaml")
    parser.add_argument("--start", default="2025-01-01", help="开始日期")
    parser.add_argument("--end", default="2026-03-26", help="结束日期")
    parser.add_argument("--capital", type=float, default=1000000, help="初始资金")
    parser.add_argument("--positions", type=int, default=5, help="持仓数量")
    parser.add_argument("--holding", type=int, default=5, help="持仓天数")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("策略回测系统")
    print("=" * 60)
    
    print("\n加载股票数据...")
    kline_pattern = str(project_root / "data" / "kline" / "*.parquet")
    stock_data = pl.read_parquet(kline_pattern)
    print(f"加载了 {len(stock_data)} 条记录")
    
    print(f"\n回测参数:")
    print(f"  策略: {args.strategy}")
    print(f"  日期范围: {args.start} ~ {args.end}")
    print(f"  初始资金: {args.capital:,.0f}")
    print(f"  持仓数量: {args.positions}")
    print(f"  持仓天数: {args.holding}")
    
    engine = BacktestEngine(args.strategy)
    result = engine.run(
        stock_data,
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital,
        position_size=args.positions,
        holding_days=args.holding
    )
    
    print("\n" + "=" * 60)
    print("回测结果")
    print("=" * 60)
    print(f"总收益率: {result['total_return']:.2%}")
    print(f"年化收益: {result['annual_return']:.2%}")
    print(f"最大回撤: {result['max_drawdown']:.2%}")
    print(f"夏普比率: {result['sharpe_ratio']:.2f}")
    print(f"胜率: {result['win_rate']:.2%}")
    print(f"总交易次数: {result['total_trades']}")
    print(f"最终资金: {result['final_value']:,.0f}")
    
    output = {"timestamp": datetime.now().isoformat(), "params": vars(args), "result": result}
    with open("reports/backtest_result.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n结果已保存到: reports/backtest_result.json")


if __name__ == "__main__":
    main()
