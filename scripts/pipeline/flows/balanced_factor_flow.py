"""流程B: 因子优先 (Balanced)
特点：全量因子计算，综合评分选股
"""
import time
from pathlib import Path
from datetime import datetime
from typing import Optional
import polars as pl

from .base_flow import BaseFlow, FlowResult, StockPick


class BalancedFactorFlow(BaseFlow):
    """因子优先流程

    流程：
    1. 读取K线数据（不预过滤）
    2. 计算全量因子（技术指标 + 量价 + 市场情绪）
    3. 综合评分：多因子加权平均
    4. 选股：取评分前30只
    """

    def __init__(self):
        super().__init__(
            name="balanced_factor",
            description="因子优先流程 - 均衡配置"
        )
        self.max_stocks = 30

    def select(self, kline_dir: str, scores_path: Optional[str] = None) -> FlowResult:
        start_time = time.time()
        self.logger.info("开始执行因子优先流程...")

        kline_path = Path(kline_dir)
        today = datetime.now().strftime("%Y%m%d")

        try:
            all_dfs = []
            for parquet_file in kline_path.glob("*.parquet"):
                if parquet_file.name == ".fetch_progress.json":
                    continue
                df = pl.read_parquet(parquet_file)
                if df is None or len(df) < 20:
                    continue
                if len(all_dfs) > 0 and df.columns != all_dfs[0].columns:
                    continue
                all_dfs.append(df)

            if not all_dfs:
                return FlowResult(self.name, today, [], time.time() - start_time)

            all_data = pl.concat(all_dfs)
            self.logger.info(f"全市场股票: {len(all_data.group_by('code').count())}只")

            scored = self._calculate_factors(all_data)

            top_picks = self._get_top_n(scored, self.max_stocks, "factor_score")

            execution_time = time.time() - start_time
            self.logger.info(f"流程完成: 选出{len(top_picks)}只, 耗时{execution_time:.2f}秒")

            return FlowResult(
                flow_name=self.name,
                pick_date=today,
                stocks=top_picks,
                execution_time=execution_time,
                metadata={"total_stocks": len(all_data.group_by("code").count())}
            )

        except Exception as e:
            self.logger.error(f"流程执行失败: {e}")
            return FlowResult(self.name, today, [], time.time() - start_time, {"error": str(e)})

    def _calculate_factors(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算全量因子"""
        from core.factor_engine import FactorEngine

        try:
            engine = FactorEngine()
            result = engine.calculate_all_factors(df, factor_names=None)
            return self._compute_factor_score(result)
        except Exception as e:
            self.logger.warning(f"因子引擎计算失败，使用简化评分: {e}")
            return self._simplified_scoring(df)

    def _compute_factor_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算综合因子评分"""
        factor_cols = [
            "factor_ma_trend", "factor_macd", "factor_rsi", "factor_kdj",
            "factor_bollinger", "factor_cci", "factor_wr", "factor_atr",
            "factor_volume_ratio", "factor_turnover", "factor_mfi",
            "factor_obv", "factor_vr", "factor_vosc"
        ]

        existing_cols = [c for c in factor_cols if c in df.columns]

        if not existing_cols:
            return self._simplified_scoring(df)

        score_df = df.group_by("code").agg([
            *[pl.col(c).last().alias(c) for c in existing_cols]
        ])

        from polars import col
        score_df = score_df.with_columns([
            sum(col(c) for c in existing_cols).alias("factor_score")
        ])

        latest = df.sort("trade_date").group_by("code").last()
        score_df = score_df.join(
            latest.select(["code", "close", "volume", "change_pct"]),
            on="code",
            how="left"
        )

        return score_df

    def _simplified_scoring(self, df: pl.DataFrame) -> pl.DataFrame:
        """简化评分：基于基础数据"""
        latest = df.sort("trade_date").group_by("code").last()

        latest = latest.with_columns([
            ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1) * 100).alias("daily_return")
        ])

        avg_volume = df.group_by("code").agg([
            pl.col("volume").mean().alias("avg_volume")
        ])

        latest = latest.join(avg_volume, on="code", how="left")

        latest = latest.with_columns([
            (pl.col("volume") / pl.col("avg_volume")).alias("volume_ratio")
        ])

        score = (
            pl.when(pl.col("volume_ratio") > 1.5).then(30)
            .when(pl.col("volume_ratio") > 1.0).then(20)
            .otherwise(10)
        )
        latest = latest.with_columns([
            score.alias("factor_score")
        ])

        return latest
