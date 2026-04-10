"""
因子组合优化主程序
使用遗传算法搜索最优因子组合和参数
增强版：全量股票回测、性能优化、交易成本模拟、标准策略YAML输出
"""
import sys
import polars as pl
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import yaml
import logging
import hashlib
import json

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

STAMP_TAX = 0.001
COMMISSION = 0.00025


class FactorCombinationOptimizer:
    """因子组合优化器"""

    def __init__(self, data_dir: str = "data", max_stocks: Optional[int] = None):
        self.data_dir = Path(data_dir)
        self.factor_engine = FactorEngine()
        self.kline_data: Dict[str, pl.DataFrame] = {}
        self.index_data: Optional[pl.DataFrame] = None
        self.stock_list: Optional[pl.DataFrame] = None
        self.max_stocks = max_stocks
        self._factor_cache: Dict[str, Dict[str, pl.DataFrame]] = {}
        self._date_index: Dict[str, List[str]] = {}
        self._all_trade_dates: List[str] = []

        self._load_data()
        self._build_date_index()

    def _load_data(self):
        logger.info("加载历史数据...")

        kline_dir = self.data_dir / "kline"
        if kline_dir.exists():
            parquet_files = sorted(kline_dir.glob("*.parquet"))
            logger.info(f"发现 {len(parquet_files)} 个股票数据文件")

            files_to_load = parquet_files
            if self.max_stocks:
                files_to_load = parquet_files[:self.max_stocks]

            for file_path in files_to_load:
                try:
                    code = file_path.stem
                    df = pl.read_parquet(file_path)
                    if len(df) > 0:
                        df = df.sort("trade_date")
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

    def _build_date_index(self):
        logger.info("构建日期索引...")
        self._date_index = {}
        all_dates = set()

        for code, df in self.kline_data.items():
            dates = df["trade_date"].to_list()
            for d in dates:
                if d not in self._date_index:
                    self._date_index[d] = []
                self._date_index[d].append(code)
            all_dates.update(dates)

        self._all_trade_dates = sorted(list(all_dates))
        logger.info(f"共 {len(self._all_trade_dates)} 个交易日")

    def get_available_factors(self) -> List[str]:
        # 使用 FactorEngine 的因子列表
        factors = self.factor_engine.list_factors(enabled_only=True)
        return [f["name"] for f in factors]

    def get_available_filters(self) -> List[str]:
        all_configs = filter_config_loader.load_all_filters()
        return [cache_key.split("/")[-1] for cache_key in all_configs.keys()]

    def get_factor_param_ranges(self) -> Dict[str, Dict[str, List]]:
        return {
            "rsi": {"period": [7, 14, 21, 28]},
            "macd": {
                "fast_period": [8, 10, 12, 14],
                "slow_period": [20, 24, 26, 30],
                "signal_period": [7, 9, 11]
            },
            "kdj": {"n": [5, 9, 14], "m1": [2, 3, 4], "m2": [2, 3, 4]},
            "bollinger": {"period": [10, 20, 30], "std_dev": [1.5, 2.0, 2.5]},
            "atr": {"period": [10, 14, 20]},
            "cci": {"period": [10, 14, 20, 30]},
            "wr": {"period": [10, 14, 20]},
            "dmi": {"period": [10, 14, 20]},
            "mtm": {"period": [6, 10, 14]},
            "roc": {"period": [10, 12, 14]},
            "psy": {"period": [10, 12, 20]},
            "obv": {"ma_period": [10, 20, 30]},
            "vr": {"period": [20, 26, 30]},
            "mfi": {"period": [10, 14, 20]},
            "vma": {"period": [5, 10, 20]},
            "vosc": {"fast_period": [5, 10], "slow_period": [20, 30]},
            "volume_ratio": {"period": [5, 10, 20]},
            "turnover": {"period": [5, 10, 20]},
            "wvad": {"period": [10, 20, 30]},
            "ma_trend": {"short_period": [5, 10], "long_period": [20, 30, 60]},
            "emv": {"period": [10, 14, 20]},
            "asi": {"limit": [0.5, 1.0, 1.5]},
            "market_trend": {"period": [15, 20, 25, 30]},
            "market_breadth": {"period": [5, 10, 15, 20]},
            "market_sentiment": {"vol_period": [15, 20, 25], "price_period": [5, 10, 15]},
            "market_temperature": {"period": [15, 20, 25], "high_low_period": [40, 52, 60]}
        }

    def _get_factor_cache_key(self, code: str, factor_name: str, params: Dict) -> str:
        param_str = json.dumps(params, sort_keys=True)
        raw = f"{code}:{factor_name}:{param_str}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _get_cached_factor(self, code: str, factor_name: str, params: Dict) -> Optional[float]:
        cache_key = self._get_factor_cache_key(code, factor_name, params)
        cache_entry = self._factor_cache.get(cache_key)
        if cache_entry is not None:
            return cache_entry.get("score")
        return None

    def _set_factor_cache(self, code: str, factor_name: str, params: Dict, score: float):
        cache_key = self._get_factor_cache_key(code, factor_name, params)
        self._factor_cache[cache_key] = {"score": score}

    def backtest_chromosome(self, chromosome: Chromosome) -> Dict[str, Any]:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3 * 365)

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            trade_dates = [
                d for d in self._all_trade_dates
                if start_date_str <= d <= end_date_str
            ]

            if len(trade_dates) < 50:
                return self._empty_result()

            holding_days = chromosome.holding_days
            position_size = chromosome.position_size

            initial_capital = 1000000
            cash = initial_capital
            positions: Dict[str, Dict] = {}
            trades = []
            daily_values = []

            for i, date in enumerate(trade_dates):
                if i % holding_days != 0:
                    position_value = 0
                    for code, pos in positions.items():
                        stock_data = self.kline_data.get(code)
                        if stock_data is not None:
                            price_data = stock_data.filter(pl.col("trade_date") == date)
                            if len(price_data) > 0:
                                position_value += pos["shares"] * price_data["close"].item()
                    total_value = cash + position_value
                    daily_values.append({"date": date, "total_value": total_value})
                    continue

                current_stocks = self._date_index.get(date, [])

                for code in list(positions.keys()):
                    stock_data = self.kline_data.get(code)
                    if stock_data is not None:
                        sell_data = stock_data.filter(pl.col("trade_date") == date)
                        if len(sell_data) > 0:
                            sell_price = sell_data["close"].item()
                            pos = positions[code]
                            gross = pos["shares"] * sell_price
                            cost = gross * STAMP_TAX + gross * COMMISSION
                            cash += gross - cost

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
                                    gross = shares * buy_price
                                    cost = gross * COMMISSION
                                    cash -= gross + cost
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
                daily_values.append({"date": date, "total_value": total_value})

            return self._calculate_metrics(daily_values, trades, initial_capital)

        except Exception as e:
            logger.error(f"回测失败: {e}")
            return self._empty_result()

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
        scores = []

        candidate_stocks = stocks[:300]

        for code in candidate_stocks:
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

                        cached = self._get_cached_factor(code, factor_name, factor_params)
                        if cached is not None:
                            weight = weights.get(factor_name, 0.1)
                            total_score += cached * weight
                            continue

                        factor = self.factor_engine.get_factor(factor_name, factor_params)

                        if factor is not None:
                            factor_data = factor.calculate(historical_data)

                            if len(factor_data) > 0:
                                factor_col = f"factor_{factor_name}"
                                if factor_col in factor_data.columns:
                                    latest_score = factor_data[factor_col].tail(1).item()
                                    self._set_factor_cache(code, factor_name, factor_params, latest_score)
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

        calmar_ratio = annual_return / max_drawdown if max_drawdown > 0 else 0

        buy_by_code: Dict[str, list] = {}
        sell_by_code: Dict[str, list] = {}

        for t in trades:
            code = t["code"]
            if t["action"] == "buy":
                buy_by_code.setdefault(code, []).append(t)
            else:
                sell_by_code.setdefault(code, []).append(t)

        wins = 0
        matched = 0

        for code, buys in buy_by_code.items():
            sells = sell_by_code.get(code, [])
            min_len = min(len(buys), len(sells))
            for i in range(min_len):
                if sells[i]["price"] > buys[i]["price"]:
                    wins += 1
                matched += 1

        win_rate = wins / matched if matched > 0 else 0

        return {
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "calmar_ratio": calmar_ratio,
            "win_rate": win_rate,
            "total_trades": len(trades),
            "final_value": final_value
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "total_return": 0,
            "annual_return": 0,
            "max_drawdown": 1,
            "sharpe_ratio": 0,
            "calmar_ratio": 0,
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
        logger.info("=" * 60)
        logger.info("开始因子组合优化")
        logger.info("=" * 60)

        available_factors = self.get_available_factors()
        available_filters = self.get_available_filters()
        factor_param_ranges = self.get_factor_param_ranges()

        logger.info(f"可用因子: {len(available_factors)} 个")
        logger.info(f"可用过滤器: {len(available_filters)} 个")
        logger.info(f"回测股票: {len(self.kline_data)} 只")

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
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        champion_raw = {
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
            yaml.dump(champion_raw, f, allow_unicode=True, default_flow_style=False)
        logger.info(f"冠军策略原始配置已保存: {config_file}")

        champion_config_latest = output_path / "champion_strategy_latest.yaml"
        with open(champion_config_latest, 'w', encoding='utf-8') as f:
            yaml.dump(champion_raw, f, allow_unicode=True, default_flow_style=False)

        champion_yaml = self._generate_strategy_yaml(best_chromosome)
        champion_strategy_path = Path("config/strategies/champion.yaml")
        champion_strategy_path.parent.mkdir(parents=True, exist_ok=True)
        with open(champion_strategy_path, 'w', encoding='utf-8') as f:
            f.write(champion_yaml)
        logger.info(f"冠军策略已写入: {champion_strategy_path} (可直接用于 run_strategy.py)")

        history_file = output_path / f"optimization_history_{timestamp}.yaml"
        with open(history_file, 'w', encoding='utf-8') as f:
            yaml.dump(history, f, allow_unicode=True, default_flow_style=False)
        logger.info(f"优化历史已保存: {history_file}")

        report = self._generate_report(best_chromosome, history)
        report_file = output_path / f"optimization_report_{timestamp}.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"优化报告已保存: {report_file}")

    def _generate_strategy_yaml(self, chromosome: Chromosome) -> str:
        factors_list = []
        for factor_name in chromosome.factors:
            weight = chromosome.factor_weights.get(factor_name, 0.1)
            params = chromosome.factor_params.get(factor_name, {})
            factor_entry = {
                "name": factor_name,
                "weight": round(weight, 4),
                "threshold": 30,
            }
            if params:
                factor_entry["params"] = params
            factors_list.append(factor_entry)

        strategy_config = {
            "strategy": {
                "name": "champion_strategy",
                "description": f"遗传算法优化冠军策略 (fitness={chromosome.fitness:.4f})",
                "version": "1.0",
                "factors": factors_list,
                "filters": [
                    {"type": "price", "min": 3, "max": 100}
                ],
                "output": {
                    "top_n": chromosome.position_size,
                    "min_score": 40,
                    "formats": ["json", "txt"]
                }
            }
        }

        return yaml.dump(strategy_config, allow_unicode=True, default_flow_style=False, sort_keys=False)

    def _generate_report(self, best_chromosome: Chromosome, history: List[Dict]) -> str:
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
