"""选股流程基类"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
import polars as pl


@dataclass
class StockPick:
    """单只股票选股结果"""
    code: str
    grade: str
    score: float
    signals: Dict[str, bool] = field(default_factory=dict)
    factors: Dict[str, float] = field(default_factory=dict)
    reason: str = ""


@dataclass
class FlowResult:
    """流程执行结果"""
    flow_name: str
    pick_date: str
    stocks: List[StockPick]
    execution_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "flow_name": self.flow_name,
            "pick_date": self.pick_date,
            "stock_count": len(self.stocks),
            "stocks": [
                {
                    "code": s.code,
                    "grade": s.grade,
                    "score": s.score,
                    "signals": s.signals,
                    "factors": s.factors,
                    "reason": s.reason
                }
                for s in self.stocks
            ],
            "execution_time": self.execution_time,
            "metadata": self.metadata
        }


class BaseFlow(ABC):
    """选股流程基类"""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        return logging.getLogger(f"{__name__}.{self.name}")

    @abstractmethod
    def select(self, kline_dir: str, scores_path: Optional[str] = None) -> FlowResult:
        """
        执行选股流程

        Args:
            kline_dir: K线数据目录
            scores_path: 预计算评分文件路径（可选）

        Returns:
            FlowResult: 选股结果
        """
        pass

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

    def _get_top_n(self, df: pl.DataFrame, n: int, score_col: str = "score") -> List[StockPick]:
        """获取评分最高的N只股票"""
        if len(df) == 0:
            return []

        top_df = df.sort(score_col, descending=True).head(n)

        picks = []
        for row in top_df.iter_rows(named=True):
            pick = StockPick(
                code=row.get("code", ""),
                grade=row.get("grade", "N"),
                score=row.get(score_col, 0.0),
                signals=row.get("signals", {}),
                factors=row.get("factors", {}),
                reason=row.get("reason", "")
            )
            picks.append(pick)

        return picks

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
