"""
回测引擎
用于评估策略历史表现
"""
import polars as pl
from typing import Dict, List, Any
import logging

from core.strategy_engine import StrategyEngine
from core.factor_engine import FactorEngine

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
        position_size: int = 10,
        holding_days: int = 5
    ) -> Dict[str, Any]:
        """
        运行回测
        
        Args:
            stock_data: 股票K线数据
            start_date: 回测开始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            position_size: 持仓股票数量
            holding_days: 持仓天数
        
        Returns:
            回测结果
        """
        self.logger.info(f"开始回测: {start_date} ~ {end_date}")
        
        # 筛选日期范围
        if start_date:
            stock_data = stock_data.filter(pl.col("trade_date") >= start_date)
        if end_date:
            stock_data = stock_data.filter(pl.col("trade_date") <= end_date)
        
        # 获取所有交易日
        trade_dates = stock_data.select("trade_date").unique().sort("trade_date")["trade_date"].to_list()
        
        # 获取所有股票代码
        codes = stock_data.select("code").unique()["code"].to_list()
        
        # 回测记录
        trades = []
        daily_values = []
        cash = initial_capital
        positions = {}
        
        # 按日期遍历
        for i, date in enumerate(trade_dates):
            # 每 holding_days 天调仓一次
            if i % holding_days != 0:
                continue
            
            # 获取当日数据
            current_data = stock_data.filter(pl.col("trade_date") == date)
            
            if len(current_data) == 0:
                continue
            
            # 卖出持仓
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
            
            # 选股
            selected = self._select_stocks_for_date(stock_data, codes, date)
            
            if len(selected) > 0:
                # 买入
                buy_codes = selected.head(position_size)["code"].to_list()
                capital_per_stock = cash / position_size
                
                for code in buy_codes:
                    stock_price = current_data.filter(pl.col("code") == code)
                    
                    if len(stock_price) > 0:
                        buy_price = stock_price["close"].item()
                        shares = int(capital_per_stock / buy_price / 100) * 100  # 整手
                        
                        if shares > 0:
                            cost = shares * buy_price
                            cash -= cost
                            positions[code] = {
                                "shares": shares,
                                "buy_price": buy_price
                            }
                            
                            trades.append({
                                "date": date,
                                "code": code,
                                "action": "buy",
                                "price": buy_price,
                                "shares": shares,
                                "value": cost
                            })
            
            # 计算当日市值
            position_value = 0
            for code, pos in positions.items():
                stock_price = current_data.filter(pl.col("code") == code)
                if len(stock_price) > 0:
                    position_value += pos["shares"] * stock_price["close"].item()
            
            total_value = cash + position_value
            daily_values.append({
                "date": date,
                "cash": cash,
                "position_value": position_value,
                "total_value": total_value
            })
        
        # 计算回测指标
        result = self._calculate_metrics(daily_values, trades, initial_capital)
        
        return result
    
    def _select_stocks_for_date(self, stock_data: pl.DataFrame, codes: List[str], date: str) -> pl.DataFrame:
        """为特定日期选股"""
        results = []
        
        for code in codes:
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
            except Exception:
                pass
        
        if results:
            return pl.concat(results).sort("strategy_score", descending=True)
        return pl.DataFrame()
    
    def _calculate_metrics(
        self, 
        daily_values: List[dict], 
        trades: List[dict],
        initial_capital: float
    ) -> Dict[str, Any]:
        """计算回测指标"""
        if not daily_values:
            return {
                "total_return": 0,
                "annual_return": 0,
                "max_drawdown": 0,
                "sharpe_ratio": 0,
                "win_rate": 0,
                "total_trades": 0
            }
        
        # 转换为 DataFrame
        df = pl.DataFrame(daily_values)
        
        # 总收益率
        final_value = df["total_value"].tail(1).item()
        total_return = (final_value - initial_capital) / initial_capital
        
        # 年化收益率
        days = len(df)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0
        
        # 最大回撤
        df = df.with_columns([
            pl.col("total_value").cum_max().alias("cummax")
        ])
        df = df.with_columns([
            ((pl.col("cummax") - pl.col("total_value")) / pl.col("cummax")).alias("drawdown")
        ])
        max_drawdown = df["drawdown"].max()
        
        # 夏普比率 (假设无风险利率 3%)
        df = df.with_columns([
            pl.col("total_value").pct_change().alias("daily_return")
        ])
        daily_returns = df["daily_return"].drop_nulls()
        
        if len(daily_returns) > 0 and daily_returns.std() > 0:
            sharpe_ratio = (daily_returns.mean() * 252 - 0.03) / (daily_returns.std() * (252 ** 0.5))
        else:
            sharpe_ratio = 0
        
        # 胜率
        buy_trades = [t for t in trades if t["action"] == "buy"]
        sell_trades = [t for t in trades if t["action"] == "sell"]
        
        wins = 0
        total_pairs = min(len(buy_trades), len(sell_trades))
        
        for i in range(total_pairs):
            if sell_trades[i]["price"] > buy_trades[i]["price"]:
                wins += 1
        
        win_rate = wins / total_pairs if total_pairs > 0 else 0
        
        return {
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "total_trades": len(trades) // 2,
            "final_value": final_value,
            "daily_values": daily_values,
            "trades": trades
        }


def run_backtest(
    strategy_config: str,
    stock_data: pl.DataFrame,
    start_date: str = None,
    end_date: str = None,
    **kwargs
) -> Dict[str, Any]:
    """运行回测的便捷函数"""
    engine = BacktestEngine(strategy_config)
    return engine.run(stock_data, start_date, end_date, **kwargs)
