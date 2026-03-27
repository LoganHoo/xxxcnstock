"""
因子组合优化主程序
使用遗传算法搜索最优因子组合和参数
"""
import sys
import polars as pl
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timedelta
import yaml
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from optimization.genetic_optimizer import GeneticOptimizer, Chromosome
from core.factor_engine import FactorEngine
from core.factor_config_loader import factor_config_loader
from filters.filter_engine import FilterEngine
from core.filter_config_loader import filter_config_loader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FactorCombinationOptimizer:
    """因子组合优化器"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.factor_engine = FactorEngine()
        self.kline_data: Dict[str, pl.DataFrame] = {}
        self.index_data: pl.DataFrame = None
        self.stock_list: pl.DataFrame = None
        
        self._load_data()
    
    def _load_data(self):
        """加载历史数据"""
        logger.info("加载历史数据...")
        
        kline_dir = self.data_dir / "kline"
        if kline_dir.exists():
            parquet_files = list(kline_dir.glob("*.parquet"))
            logger.info(f"发现 {len(parquet_files)} 个股票数据文件")
            
            sample_files = parquet_files[:500]
            
            for file_path in sample_files:
                try:
                    code = file_path.stem
                    df = pl.read_parquet(file_path)
                    if len(df) > 0:
                        self.kline_data[code] = df
                except Exception as e:
                    logger.warning(f"加载 {file_path} 失败: {e}")
            
            logger.info(f"成功加载 {len(self.kline_data)} 只股票数据")
        
        index_file = self.data_dir / "index" / "000001.parquet"
        if index_file.exists():
            self.index_data = pl.read_parquet(index_file)
            logger.info(f"加载指数数据: {len(self.index_data)} 条")
        
        stock_list_file = self.data_dir / "stock_list.parquet"
        if stock_list_file.exists():
            self.stock_list = pl.read_parquet(stock_list_file)
            logger.info(f"加载股票列表: {len(self.stock_list)} 只")
    
    def get_available_factors(self) -> List[str]:
        """获取可用因子列表"""
        all_configs = factor_config_loader.load_all_factors()
        return [cache_key.split("/")[-1] for cache_key in all_configs.keys()]
    
    def get_available_filters(self) -> List[str]:
        """获取可用过滤器列表"""
        all_configs = filter_config_loader.load_all_filters()
        return [cache_key.split("/")[-1] for cache_key in all_configs.keys()]
    
    def get_factor_param_ranges(self) -> Dict[str, Dict[str, List]]:
        """获取因子参数范围"""
        param_ranges = {
            "rsi": {
                "period": [7, 14, 21, 28]
            },
            "macd": {
                "fast_period": [8, 10, 12, 14],
                "slow_period": [20, 24, 26, 30],
                "signal_period": [7, 9, 11]
            },
            "kdj": {
                "n": [5, 9, 14],
                "m1": [2, 3, 4],
                "m2": [2, 3, 4]
            },
            "bollinger": {
                "period": [10, 20, 30],
                "std_dev": [1.5, 2.0, 2.5]
            },
            "atr": {
                "period": [10, 14, 20]
            },
            "cci": {
                "period": [10, 14, 20, 30]
            },
            "wr": {
                "period": [10, 14, 20]
            },
            "dmi": {
                "period": [10, 14, 20]
            },
            "mtm": {
                "period": [6, 10, 14]
            },
            "roc": {
                "period": [10, 12, 14]
            },
            "psy": {
                "period": [10, 12, 20]
            },
            "obv": {
                "ma_period": [10, 20, 30]
            },
            "vr": {
                "period": [20, 26, 30]
            },
            "mfi": {
                "period": [10, 14, 20]
            },
            "vma": {
                "period": [5, 10, 20]
            },
            "vosc": {
                "fast_period": [5, 10],
                "slow_period": [20, 30]
            },
            "volume_ratio": {
                "period": [5, 10, 20]
            },
            "turnover": {
                "period": [5, 10, 20]
            },
            "wvad": {
                "period": [10, 20, 30]
            },
            "ma_trend": {
                "short_period": [5, 10],
                "long_period": [20, 30, 60]
            },
            "emv": {
                "period": [10, 14, 20]
            },
            "asi": {
                "limit": [0.5, 1.0, 1.5]
            }
        }
        return param_ranges
    
    def backtest_chromosome(self, chromosome: Chromosome) -> Dict[str, Any]:
        """
        回测染色体策略
        
        Args:
            chromosome: 染色体
        
        Returns:
            回测结果
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3*365)
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            all_dates = set()
            for code, df in self.kline_data.items():
                dates = df.filter(
                    (pl.col("trade_date") >= start_date_str) &
                    (pl.col("trade_date") <= end_date_str)
                )["trade_date"].unique().to_list()
                all_dates.update(dates)
            
            trade_dates = sorted(list(all_dates))
            
            if len(trade_dates) < 50:
                return self._empty_result()
            
            holding_days = chromosome.holding_days
            position_size = chromosome.position_size
            
            initial_capital = 1000000
            cash = initial_capital
            positions = {}
            trades = []
            daily_values = []
            
            for i, date in enumerate(trade_dates):
                if i % holding_days != 0:
                    continue
                
                current_stocks = self._get_stocks_for_date(date)
                
                for code in list(positions.keys()):
                    stock_data = self.kline_data.get(code)
                    if stock_data is not None:
                        sell_data = stock_data.filter(pl.col("trade_date") == date)
                        if len(sell_data) > 0:
                            sell_price = sell_data["close"].item()
                            pos = positions[code]
                            cash += pos["shares"] * sell_price
                            
                            trades.append({
                                "date": date,
                                "code": code,
                                "action": "sell",
                                "price": sell_price,
                                "shares": pos["shares"]
                            })
                
                positions.clear()
                
                selected_stocks = self._select_stocks(
                    current_stocks,
                    date,
                    chromosome.factors,
                    chromosome.factor_weights,
                    chromosome.factor_params,
                    chromosome.filters,
                    position_size
                )
                
                if len(selected_stocks) > 0:
                    capital_per_stock = cash / position_size
                    
                    for code in selected_stocks:
                        stock_data = self.kline_data.get(code)
                        if stock_data is not None:
                            buy_data = stock_data.filter(pl.col("trade_date") == date)
                            if len(buy_data) > 0:
                                buy_price = buy_data["close"].item()
                                shares = int(capital_per_stock / buy_price / 100) * 100
                                
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
                                        "shares": shares
                                    })
                
                position_value = 0
                for code, pos in positions.items():
                    stock_data = self.kline_data.get(code)
                    if stock_data is not None:
                        price_data = stock_data.filter(pl.col("trade_date") == date)
                        if len(price_data) > 0:
                            position_value += pos["shares"] * price_data["close"].item()
                
                total_value = cash + position_value
                daily_values.append({
                    "date": date,
                    "total_value": total_value
                })
            
            return self._calculate_metrics(daily_values, trades, initial_capital)
            
        except Exception as e:
            logger.error(f"回测失败: {e}")
            return self._empty_result()
    
    def _get_stocks_for_date(self, date: str) -> List[str]:
        """获取指定日期有交易的股票"""
        stocks = []
        for code, df in self.kline_data.items():
            if len(df.filter(pl.col("trade_date") == date)) > 0:
                stocks.append(code)
        return stocks
    
    def _select_stocks(
        self,
        stocks: List[str],
        date: str,
        factors: List[str],
        weights: Dict[str, float],
        params: Dict[str, Dict],
        filters: List[str],
        top_n: int
    ) -> List[str]:
        """选股"""
        scores = []
        
        for code in stocks[:200]:
            try:
                stock_data = self.kline_data.get(code)
                if stock_data is None or len(stock_data) < 50:
                    continue
                
                historical_data = stock_data.filter(pl.col("trade_date") <= date).tail(100)
                
                if len(historical_data) < 30:
                    continue
                
                total_score = 0.0
                
                for factor_name in factors:
                    try:
                        factor_params = params.get(factor_name, {})
                        factor = self.factor_engine.get_factor(factor_name, factor_params)
                        
                        if factor is not None:
                            factor_data = factor.calculate(historical_data)
                            
                            if len(factor_data) > 0:
                                factor_col = f"factor_{factor_name}"
                                if factor_col in factor_data.columns:
                                    latest_score = factor_data[factor_col].tail(1).item()
                                    weight = weights.get(factor_name, 0.1)
                                    total_score += latest_score * weight
                    except Exception:
                        pass
                
                scores.append((code, total_score))
                
            except Exception:
                pass
        
        scores.sort(key=lambda x: x[1], reverse=True)
        
        selected = [s[0] for s in scores[:top_n]]
        return selected
    
    def _calculate_metrics(
        self,
        daily_values: List[dict],
        trades: List[dict],
        initial_capital: float
    ) -> Dict[str, Any]:
        """计算回测指标"""
        if not daily_values:
            return self._empty_result()
        
        df = pl.DataFrame(daily_values)
        
        final_value = df["total_value"].tail(1).item()
        total_return = (final_value - initial_capital) / initial_capital
        
        days = len(df)
        annual_return = (1 + total_return) ** (252 / max(days, 1)) - 1 if days > 0 else 0
        
        df = df.with_columns([
            pl.col("total_value").cum_max().alias("cummax")
        ])
        df = df.with_columns([
            ((pl.col("cummax") - pl.col("total_value")) / pl.col("cummax")).alias("drawdown")
        ])
        max_drawdown = df["drawdown"].max() if len(df) > 0 else 0
        
        df = df.with_columns([
            pl.col("total_value").pct_change().alias("daily_return")
        ])
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
            "total_trades": len(trades),
            "final_value": final_value
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """返回空结果"""
        return {
            "total_return": 0,
            "annual_return": 0,
            "max_drawdown": 1,
            "sharpe_ratio": 0,
            "win_rate": 0,
            "total_trades": 0,
            "final_value": 0
        }
    
    def run_optimization(
        self,
        population_size: int = 30,
        generations: int = 20,
        output_dir: str = "optimization/results"
    ) -> Chromosome:
        """
        运行优化
        
        Args:
            population_size: 种群大小
            generations: 迭代代数
            output_dir: 输出目录
        
        Returns:
            最优染色体
        """
        logger.info("=" * 60)
        logger.info("开始因子组合优化")
        logger.info("=" * 60)
        
        available_factors = self.get_available_factors()
        available_filters = self.get_available_filters()
        factor_param_ranges = self.get_factor_param_ranges()
        
        logger.info(f"可用因子: {len(available_factors)} 个")
        logger.info(f"可用过滤器: {len(available_filters)} 个")
        
        optimizer = GeneticOptimizer(
            available_factors=available_factors,
            available_filters=available_filters,
            factor_param_ranges=factor_param_ranges,
            population_size=population_size,
            generations=generations,
            crossover_rate=0.8,
            mutation_rate=0.3,
            elite_size=3,
            min_factors=3,
            max_factors=8,
            min_filters=2,
            max_filters=6
        )
        
        best_chromosome = optimizer.evolve(self.backtest_chromosome)
        
        self._save_results(best_chromosome, optimizer.get_history(), output_dir)
        
        logger.info("=" * 60)
        logger.info("优化完成")
        logger.info("=" * 60)
        
        return best_chromosome
    
    def _save_results(
        self,
        best_chromosome: Chromosome,
        history: List[Dict],
        output_dir: str
    ):
        """保存优化结果"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        champion_config = {
            "strategy": {
                "name": "champion_strategy",
                "description": "遗传算法优化得出的冠军策略",
                "created_at": datetime.now().isoformat(),
                "fitness": best_chromosome.fitness
            },
            "factors": {
                "selected": best_chromosome.factors,
                "weights": best_chromosome.factor_weights,
                "params": best_chromosome.factor_params
            },
            "filters": {
                "selected": best_chromosome.filters
            },
            "execution": {
                "holding_days": best_chromosome.holding_days,
                "position_size": best_chromosome.position_size
            }
        }
        
        config_file = output_path / f"champion_strategy_{timestamp}.yaml"
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(champion_config, f, allow_unicode=True, default_flow_style=False)
        logger.info(f"冠军策略配置已保存: {config_file}")
        
        champion_config_latest = output_path / "champion_strategy_latest.yaml"
        with open(champion_config_latest, 'w', encoding='utf-8') as f:
            yaml.dump(champion_config, f, allow_unicode=True, default_flow_style=False)
        
        history_file = output_path / f"optimization_history_{timestamp}.yaml"
        with open(history_file, 'w', encoding='utf-8') as f:
            yaml.dump(history, f, allow_unicode=True, default_flow_style=False)
        logger.info(f"优化历史已保存: {history_file}")
        
        report = self._generate_report(best_chromosome, history)
        report_file = output_path / f"optimization_report_{timestamp}.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"优化报告已保存: {report_file}")
    
    def _generate_report(self, best_chromosome: Chromosome, history: List[Dict]) -> str:
        """生成优化报告"""
        report = f"""# 因子组合优化报告

## 优化时间
{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 最优策略配置

### 选中的因子
| 因子名称 | 权重 | 参数 |
|---------|------|------|
"""
        
        for factor in best_chromosome.factors:
            weight = best_chromosome.factor_weights.get(factor, 0)
            params = best_chromosome.factor_params.get(factor, {})
            params_str = ", ".join([f"{k}={v}" for k, v in params.items()]) if params else "默认"
            report += f"| {factor} | {weight:.4f} | {params_str} |\n"
        
        report += f"""
### 选中的过滤器
{', '.join(best_chromosome.filters)}

### 执行参数
- 持仓天数: {best_chromosome.holding_days}
- 持仓数量: {best_chromosome.position_size}

## 优化结果

### 适应度
{best_chromosome.fitness:.4f}

### 进化过程
| 代数 | 最优适应度 | 平均适应度 |
|------|-----------|-----------|
"""
        
        for h in history[-10:]:
            report += f"| {h['generation']} | {h['best_fitness']:.4f} | {h['avg_fitness']:.4f} |\n"
        
        report += """
## 建议

1. 将冠军策略配置应用到生产环境
2. 定期重新运行优化以适应市场变化
3. 监控策略表现，及时调整参数
"""
        
        return report


def main():
    """主函数"""
    optimizer = FactorCombinationOptimizer(data_dir="data")
    
    best = optimizer.run_optimization(
        population_size=20,
        generations=10
    )
    
    print("\n" + "=" * 60)
    print("最优策略配置:")
    print("=" * 60)
    print(f"因子: {best.factors}")
    print(f"权重: {best.factor_weights}")
    print(f"过滤器: {best.filters}")
    print(f"持仓天数: {best.holding_days}")
    print(f"持仓数量: {best.position_size}")
    print(f"适应度: {best.fitness:.4f}")


if __name__ == "__main__":
    main()
