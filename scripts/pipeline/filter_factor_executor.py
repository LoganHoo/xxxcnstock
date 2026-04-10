"""过滤+因子组合策略执行器

支持三种预设策略:
1. 保守稳健策略 (conservative)
2. 激进进攻策略 (aggressive)
3. 均衡配置策略 (balanced)

使用方法:
    python scripts/pipeline/filter_factor_executor.py --preset conservative
    python scripts/pipeline/filter_factor_executor.py --preset aggressive
    python scripts/pipeline/filter_factor_executor.py --preset balanced
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import polars as pl

from filters.filter_engine import FilterEngine
from core.factor_engine import FactorEngine


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FilterFactorExecutor:
    """过滤+因子组合策略执行器"""

    PRESETS = {
        "conservative": "config/strategies/filter_factor_strategy_1_conservative.yaml",
        "aggressive": "config/strategies/filter_factor_strategy_2_aggressive.yaml",
        "balanced": "config/strategies/filter_factor_strategy_3_balanced.yaml",
    }

    def __init__(self, preset: str = "balanced"):
        self.preset = preset
        self.config_path = self.PRESETS.get(preset)
        if not self.config_path:
            raise ValueError(f"Unknown preset: {preset}. Available: {list(self.PRESETS.keys())}")

        self.config = self._load_config()
        self.filter_engine = FilterEngine(preset=self.config.get("preset", "standard"))
        self.factor_engine = FactorEngine()

    def _load_config(self) -> dict:
        """加载策略配置"""
        import yaml
        config_file = Path(self.config_path)
        if not config_file.exists():
            config_file = Path(__file__).parent.parent.parent / self.config_path

        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def load_kline_data(self, kline_dir: str) -> pl.DataFrame:
        """加载K线数据（最新交易日）"""
        kline_path = Path(kline_dir)
        all_dfs = []
        target_columns = {"code", "trade_date", "open", "close", "high", "low", "volume"}

        for parquet_file in kline_path.glob("*.parquet"):
            if parquet_file.name == ".fetch_progress.json":
                continue
            try:
                df = pl.read_parquet(parquet_file)
            except Exception:
                continue
            if df is None or len(df) < 20:
                continue
            if set(df.columns) != target_columns:
                continue
            df = df.with_columns([
                pl.col("volume").cast(pl.Int64)
            ])
            cols_to_keep = ["code", "trade_date", "open", "close", "high", "low", "volume"]
            df = df.select([pl.col(c) for c in cols_to_keep if c in df.columns])
            all_dfs.append(df)

        if not all_dfs:
            return pl.DataFrame()

        panel_data = pl.concat(all_dfs, rechunk=True)

        if "trade_date" in panel_data.columns:
            latest_date = panel_data["trade_date"].max()
            stock_list = panel_data.filter(pl.col("trade_date") == latest_date)
            logger.info(f"最新交易日: {latest_date}, 股票数: {len(stock_list)}")
            return stock_list

        return panel_data

    def apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用过滤器链"""
        if len(df) == 0:
            return df

        filter_configs = self.config.get("filters", [])
        result = df.clone()

        for fc in filter_configs:
            if not fc.get("enabled", True):
                continue

            filter_name = fc["name"]
            filter_instance = self.filter_engine.get_filter(filter_name)

            if filter_instance:
                try:
                    before = len(result)
                    result = filter_instance.filter(result)
                    after = len(result)
                    if before != after:
                        logger.info(f"  过滤器 [{filter_name}]: {before} -> {after} (移除 {before - after})")
                except Exception as e:
                    logger.warning(f"  过滤器 [{filter_name}] 执行失败: {e}")
            else:
                logger.warning(f"  过滤器 [{filter_name}] 未找到")

        return result

    def calculate_factors(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算因子并生成综合评分"""
        if len(df) == 0:
            return df

        try:
            result = self.factor_engine.calculate_all_factors(df)

            factor_configs = self.config.get("factors", {})
            tech_factors = factor_configs.get("technical", [])
            vol_factors = factor_configs.get("volume_price", [])
            mkt_factors = factor_configs.get("market", [])

            all_factor_names = []
            weights = []

            for fc in tech_factors + vol_factors + mkt_factors:
                fname = fc.get("name")
                weight = fc.get("weight", 0)
                if fname and weight > 0:
                    col_name = f"factor_{fname}"
                    all_factor_names.append(col_name)
                    weights.append(weight)

            if all_factor_names:
                total_weight = sum(weights)
                normalized_weights = [w / total_weight for w in weights]

                score_expr = pl.lit(0.0)
                for col, w in zip(all_factor_names, normalized_weights):
                    score_expr = score_expr + pl.col(col).fill_null(50) * w
                result = result.with_columns([
                    score_expr.alias("score")
                ])
            else:
                result = result.with_columns([
                    pl.lit(50.0).alias("score")
                ])

            return result
        except Exception as e:
            logger.warning(f"因子计算失败: {e}")
            return df

    def select_stocks(self, df: pl.DataFrame) -> pl.DataFrame:
        """执行选股"""
        config = self.config.get("selection", {})
        top_n = config.get("top_n", 30)
        min_score = config.get("min_score", 60)

        if "score" not in df.columns:
            logger.warning("No score column found, returning empty result")
            return pl.DataFrame()

        result = df.filter(pl.col("score") >= min_score)
        result = result.sort("score", descending=True).head(top_n)

        return result

    def execute(self, kline_dir: str) -> Dict:
        """执行完整流程"""
        logger.info(f"=" * 60)
        logger.info(f"开始执行策略: {self.config['strategy']['name']}")
        logger.info(f"预设: {self.preset}")
        logger.info(f"=" * 60)

        start_time = datetime.now()
        today = start_time.strftime("%Y%m%d")

        df = self.load_kline_data(kline_dir)
        total_stocks = len(df.group_by("code").count()) if len(df) > 0 else 0
        logger.info(f"总股票数: {total_stocks}")

        if len(df) == 0:
            return {"stocks": [], "metadata": {"error": "No data"}}

        logger.info("\n[阶段1] 应用过滤器...")
        filtered = self.apply_filters(df)
        filtered_count = len(filtered.group_by("code").count()) if len(filtered) > 0 else 0
        logger.info(f"过滤后股票数: {filtered_count}")

        logger.info("\n[阶段2] 计算因子...")
        scored = self.calculate_factors(filtered)

        logger.info("\n[阶段3] 执行选股...")
        selected = self.select_stocks(scored)
        selected_count = len(selected) if selected is not None and not selected.is_empty() else 0
        logger.info(f"选中股票数: {selected_count}")

        stocks = []
        for row in selected.iter_rows(named=True):
            stocks.append({
                "code": row.get("code", ""),
                "score": row.get("score", 0),
                "close": row.get("close", 0),
                "volume": row.get("volume", 0),
            })

        execution_time = (datetime.now() - start_time).total_seconds()

        result = {
            "strategy_name": self.config["strategy"]["name"],
            "preset": self.preset,
            "pick_date": today,
            "stocks": stocks,
            "metadata": {
                "total_stocks": total_stocks,
                "filtered_stocks": filtered_count,
                "selected_stocks": selected_count,
                "execution_time": execution_time,
            }
        }

        logger.info(f"\n执行完成! 耗时: {execution_time:.2f}秒")
        return result

    def print_summary(self, result: Dict):
        """打印结果汇总"""
        print("\n" + "=" * 60)
        print(f"策略: {result['strategy_name']}")
        print(f"日期: {result['pick_date']}")
        print("=" * 60)

        m = result["metadata"]
        print(f"总股票: {m['total_stocks']} -> 过滤: {m['filtered_stocks']} -> 选中: {m['selected_stocks']}")

        print("\n选股结果:")
        print(f"{'代码':<10} {'评分':<10} {'收盘价':<12} {'成交量':<15}")
        print("-" * 50)
        for stock in result["stocks"][:20]:
            print(f"{stock['code']:<10} {stock['score']:<10.2f} {stock['close']:<12.2f} {stock['volume']:<15.0f}")

        if len(result["stocks"]) > 20:
            print(f"... 还有 {len(result['stocks']) - 20} 只")


def main():
    parser = argparse.ArgumentParser(description="过滤+因子组合策略执行器")
    parser.add_argument("--preset", type=str, default="balanced",
                        choices=["conservative", "aggressive", "balanced"],
                        help="策略预设")
    parser.add_argument("--kline-dir", type=str, default="data/kline",
                        help="K线数据目录")

    args = parser.parse_args()

    project_root = Path(__file__).parent.parent.parent
    kline_dir = project_root / args.kline_dir

    if not kline_dir.exists():
        logger.error(f"K线目录不存在: {kline_dir}")
        return

    executor = FilterFactorExecutor(preset=args.preset)
    result = executor.execute(str(kline_dir))
    executor.print_summary(result)


if __name__ == "__main__":
    main()
