"""流程C: 信号优先 (Aggressive)
特点：纯主力共振信号驱动，快速筛选
"""
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import polars as pl

from .base_flow import BaseFlow, FlowResult, StockPick


class AggressiveSignalFlow(BaseFlow):
    """信号优先流程

    流程：
    1. 读取K线数据
    2. 计算主力共振信号（S+/A/B级）
    3. 过滤：剔除不满足买入条件的
    4. 选股：S+全取 + A取前50% + B取前20%
    """

    def __init__(self):
        super().__init__(
            name="aggressive_signal",
            description="信号优先流程 - 激进跟随"
        )

    def select(self, kline_dir: str, scores_path: Optional[str] = None) -> FlowResult:
        start_time = time.time()
        self.logger.info("开始执行信号优先流程...")

        today = datetime.now().strftime("%Y%m%d")

        try:
            resonance_result = self._calculate_resonance(kline_dir)

            if resonance_result is None or len(resonance_result) == 0:
                self.logger.warning("未发现共振信号")
                return FlowResult(self.name, today, [], time.time() - start_time)

            picks = self._select_by_grade(resonance_result)

            execution_time = time.time() - start_time
            self.logger.info(f"流程完成: 选出{len(picks)}只, 耗时{execution_time:.2f}秒")

            return FlowResult(
                flow_name=self.name,
                pick_date=today,
                stocks=picks,
                execution_time=execution_time,
                metadata={
                    "total_signals": len(resonance_result),
                    "s_plus_count": len([p for p in picks if p.grade == "S+"]),
                    "a_count": len([p for p in picks if p.grade == "A"]),
                    "b_count": len([p for p in picks if p.grade == "B"])
                }
            )

        except Exception as e:
            self.logger.error(f"流程执行失败: {e}")
            return FlowResult(self.name, today, [], time.time() - start_time, {"error": str(e)})

    def _calculate_resonance(self, kline_dir: str) -> pl.DataFrame:
        """计算主力共振信号"""
        from services.mainforce_resonance import scan_mainforce_signals

        try:
            result = scan_mainforce_signals(kline_dir)
            if result is None or len(result) == 0:
                return pl.DataFrame()
            return result
        except Exception as e:
            self.logger.warning(f"共振信号计算失败: {e}")
            return pl.DataFrame()

    def _select_by_grade(self, resonance: pl.DataFrame) -> List[StockPick]:
        """按等级筛选股票"""
        picks: List[StockPick] = []

        s_plus = resonance.filter(pl.col("grade") == "S+")
        for row in s_plus.iter_rows(named=True):
            picks.append(StockPick(
                code=row["code"],
                grade=row["grade"],
                score=float(row["signal_count"]) * 25,
                signals={"S1": row["S1"], "S2": row["S2"], "S3": row["S3"], "S4": row["S4"]},
                factors={},
                reason="S+级全取"
            ))

        a_stocks = resonance.filter(pl.col("grade") == "A").sort("signal_count", descending=True)
        a_take_count = max(1, len(a_stocks) // 2)
        for i, row in enumerate(a_stocks.iter_rows(named=True)):
            if i >= a_take_count:
                break
            picks.append(StockPick(
                code=row["code"],
                grade=row["grade"],
                score=float(row["signal_count"]) * 25,
                signals={"S1": row["S1"], "S2": row["S2"], "S3": row["S3"], "S4": row["S4"]},
                factors={},
                reason="A级前50%"
            ))

        b_stocks = resonance.filter(pl.col("grade") == "B").sort("signal_count", descending=True)
        b_take_count = max(1, len(b_stocks) // 5)
        for i, row in enumerate(b_stocks.iter_rows(named=True)):
            if i >= b_take_count:
                break
            picks.append(StockPick(
                code=row["code"],
                grade=row["grade"],
                score=float(row["signal_count"]) * 25,
                signals={"S1": row["S1"], "S2": row["S2"], "S3": row["S3"], "S4": row["S4"]},
                factors={},
                reason="B级前20%"
            ))

        return picks
