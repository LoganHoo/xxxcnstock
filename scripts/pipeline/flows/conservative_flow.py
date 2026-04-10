"""流程A: 过滤优先 (Conservative)
特点：先过滤，后因子计算，再共振信号
"""
import time
from pathlib import Path
from datetime import datetime
from typing import Optional
import polars as pl

from .base_flow import BaseFlow, FlowResult, StockPick


class ConservativeFlow(BaseFlow):
    """过滤优先流程

    流程：
    1. 读取K线数据
    2. 过滤：剔除涨跌停/停牌/低流动性/科创创业
    3. 计算基础因子 (MA、MACD、KDJ)
    4. 计算主力共振信号
    5. 综合评分：因子40% + 共振信号60%
    6. 选股：取评分前30只
    """

    def __init__(self):
        super().__init__(
            name="conservative",
            description="过滤优先流程 - 保守稳健"
        )
        self.max_stocks = 30

    def select(self, kline_dir: str, scores_path: Optional[str] = None) -> FlowResult:
        start_time = time.time()
        self.logger.info("开始执行过滤优先流程...")

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

            filtered = self._filter_basic(all_data)
            self.logger.info(f"基础过滤后: {len(filtered)}只")

            filtered = self._filter_suspended_and_limit(filtered)

            resonance_result = self._calculate_resonance(kline_dir)

            final_df = self._merge_and_score(filtered, resonance_result)

            top_picks = self._get_top_n(final_df, self.max_stocks, "conservative_score")

            execution_time = time.time() - start_time
            self.logger.info(f"流程完成: 选出{len(top_picks)}只, 耗时{execution_time:.2f}秒")

            return FlowResult(
                flow_name=self.name,
                pick_date=today,
                stocks=top_picks,
                execution_time=execution_time,
                metadata={"filtered_count": len(filtered)}
            )

        except Exception as e:
            self.logger.error(f"流程执行失败: {e}")
            return FlowResult(self.name, today, [], time.time() - start_time, {"error": str(e)})

    def _filter_basic(self, df: pl.DataFrame) -> pl.DataFrame:
        """基础过滤：涨跌停、停牌、低流动性"""
        MIN_VOLUME = 1_000_000

        if "pct_change" in df.columns:
            return df.filter(
                (pl.col("volume") > 0) &
                (pl.col("volume") >= MIN_VOLUME) &
                (pl.col("pct_change") < 9.9) &
                (pl.col("pct_change") > -9.9)
            )
        elif "close" in df.columns and "open" in df.columns:
            return df.filter(
                (pl.col("volume") > 0) &
                (pl.col("volume") >= MIN_VOLUME) &
                ((pl.col("close") - pl.col("open")) / pl.col("open") < 0.099) &
                ((pl.col("close") - pl.col("open")) / pl.col("open") > -0.099)
            )
        else:
            return df.filter(
                (pl.col("volume") > 0) &
                (pl.col("volume") >= MIN_VOLUME)
            )

    def _filter_suspended_and_limit(self, df: pl.DataFrame) -> pl.DataFrame:
        """过滤科创、创业、北交所（首板计算逻辑差异大）"""
        if "code" not in df.columns:
            return df

        result = df.filter(
            ~pl.col("code").str.starts_with("688") &
            ~pl.col("code").str.starts_with("300") &
            ~pl.col("code").str.starts_with("430") &
            ~pl.col("code").str.starts_with("870")
        )
        return result

    def _calculate_resonance(self, kline_dir: str) -> pl.DataFrame:
        """计算主力共振信号"""
        from services.mainforce_resonance import scan_mainforce_signals

        try:
            result = scan_mainforce_signals(kline_dir)
            if result is None or len(result) == 0:
                return pl.DataFrame()
            return result.select(["code", "grade", "signal_count", "S1", "S2", "S3", "S4"])
        except Exception as e:
            self.logger.warning(f"共振信号计算失败: {e}")
            return pl.DataFrame()

    def _merge_and_score(self, df: pl.DataFrame, resonance: pl.DataFrame) -> pl.DataFrame:
        """合并数据并计算综合评分"""
        latest = df.sort("trade_date").group_by("code").last()

        if resonance is None or len(resonance) == 0:
            latest = latest.with_columns([
                pl.lit(50.0).alias("conservative_score"),
                pl.lit("N").alias("grade"),
                pl.lit(0).alias("signal_count")
            ])
            return latest

        merged = latest.join(
            resonance,
            on="code",
            how="left"
        )

        merged = merged.with_columns([
            pl.col("grade").fill_null("N"),
            pl.col("signal_count").fill_null(0)
        ])

        grade_map = {"S+": 100, "A": 80, "B": 60, "C": 40, "N": 20}
        merged = merged.with_columns([
            pl.col("grade").map_elements(lambda x: grade_map.get(x, 20)).alias("grade_score")
        ])

        signal_count = merged["signal_count"]
        merged = merged.with_columns([
            (pl.col("grade_score") * 0.6 + signal_count * 10 * 0.4).alias("conservative_score")
        ])

        return merged
