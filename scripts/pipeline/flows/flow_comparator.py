"""选股流程对比分析器"""
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import polars as pl

from .base_flow import FlowResult, StockPick


@dataclass
class PositionRecord:
    """持仓记录"""
    flow_name: str
    code: str
    pick_date: str
    entry_price: float
    current_price: float = 0.0
    holding_days: int = 0
    profit_pct: float = 0.0
    max_profit_pct: float = 0.0
    max_loss_pct: float = 0.0
    status: str = "open"


@dataclass
class FlowPerformance:
    """流程表现"""
    flow_name: str
    total_picks: int = 0
    closed_picks: int = 0
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    avg_holding_days: float = 0.0
    max_return: float = 0.0
    min_return: float = 0.0
    total_return: float = 0.0


class FlowComparator:
    """选股流程对比分析器"""

    def __init__(self, output_dir: str = "data/picks"):
        self.output_dir = Path(output_dir)
        self.flow_dirs = {
            "conservative": self.output_dir / "flow_a",
            "balanced_factor": self.output_dir / "flow_b",
            "aggressive_signal": self.output_dir / "flow_c"
        }
        for d in self.flow_dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    def save_result(self, result: FlowResult) -> str:
        """保存流程结果"""
        flow_dir = self.flow_dirs.get(result.flow_name, self.output_dir)
        date_dir = flow_dir / result.pick_date
        date_dir.mkdir(parents=True, exist_ok=True)

        filepath = date_dir / "picks.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)

        self.logger.info(f"结果已保存: {filepath}")
        return str(filepath)

    def load_history(self, flow_name: str, days: int = 30) -> List[FlowResult]:
        """加载历史选股结果"""
        flow_dir = self.flow_dirs.get(flow_name)
        if not flow_dir or not flow_dir.exists():
            return []

        results = []
        end_date = datetime.now()

        for i in range(days):
            date = (end_date - timedelta(days=i)).strftime("%Y%m%d")
            date_dir = flow_dir / date
            if date_dir.exists():
                filepath = date_dir / "picks.json"
                if filepath.exists():
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    results.append(FlowResult(
                        flow_name=data["flow_name"],
                        pick_date=data["pick_date"],
                        stocks=[StockPick(**s) for s in data["stocks"]],
                        execution_time=data["execution_time"],
                        metadata=data.get("metadata", {})
                    ))

        return results

    def track_positions(self, flow_name: str, kline_dir: str, days: int = 5) -> List[PositionRecord]:
        """追踪持仓表现"""
        history = self.load_history(flow_name, days=days)
        if not history:
            return []

        positions = []
        kline_path = Path(kline_dir)

        for result in history:
            pick_date = datetime.strptime(result.pick_date, "%Y%m%d")

            for stock in result.stocks:
                parquet_file = kline_path / f"{stock.code}.parquet"
                if not parquet_file.exists():
                    continue

                df = pl.read_parquet(parquet_file)
                df = df.sort("trade_date")

                entry_row = df.filter(pl.col("trade_date") >= result.pick_date).head(1)
                if len(entry_row) == 0:
                    continue

                entry_price = entry_row["close"][0]

                exit_date = (pick_date + timedelta(days=days)).strftime("%Y%m%d")
                exit_rows = df.filter(pl.col("trade_date") >= exit_date)
                if len(exit_rows) > 0:
                    exit_price = exit_rows.head(1)["close"][0]
                    status = "closed"
                else:
                    latest = df.tail(1)
                    exit_price = latest["close"][0]
                    status = "open"

                profit_pct = (exit_price - entry_price) / entry_price * 100

                positions.append(PositionRecord(
                    flow_name=flow_name,
                    code=stock.code,
                    pick_date=result.pick_date,
                    entry_price=entry_price,
                    current_price=exit_price,
                    holding_days=days if status == "closed" else (datetime.now() - pick_date).days,
                    profit_pct=profit_pct,
                    status=status
                ))

        return positions

    def compute_performance(self, positions: List[PositionRecord]) -> FlowPerformance:
        """计算流程表现"""
        if not positions:
            return FlowPerformance(flow_name="")

        closed = [p for p in positions if p.status == "closed"]
        wins = [p for p in closed if p.profit_pct > 0]

        if not closed:
            return FlowPerformance(flow_name=positions[0].flow_name if positions else "")

        returns = [p.profit_pct for p in closed]

        return FlowPerformance(
            flow_name=positions[0].flow_name,
            total_picks=len(positions),
            closed_picks=len(closed),
            win_count=len(wins),
            loss_count=len(closed) - len(wins),
            win_rate=len(wins) / len(closed) * 100 if closed else 0,
            avg_return=sum(returns) / len(returns),
            total_return=sum(returns),
            max_return=max(returns) if returns else 0,
            min_return=min(returns) if returns else 0
        )

    def compare_flows(self, kline_dir: str, days: int = 30) -> Dict[str, FlowPerformance]:
        """对比所有流程"""
        performances = {}

        for flow_name in self.flow_dirs.keys():
            positions = self.track_positions(flow_name, kline_dir, days)
            perf = self.compute_performance(positions)
            performances[flow_name] = perf

        return performances

    def generate_report(self, performances: Dict[str, FlowPerformance]) -> str:
        """生成对比报告"""
        report_lines = [
            f"# 选股流程对比报告",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 流程表现对比",
            "",
            "| 流程 | 总选股 | 已平仓 | 胜率 | 平均收益 | 最大收益 | 最小收益 |",
            "|------|--------|--------|------|----------|----------|----------|",
        ]

        for name, perf in sorted(performances.items(), key=lambda x: x[1].avg_return, reverse=True):
            report_lines.append(
                f"| {name} | {perf.total_picks} | {perf.closed_picks} | "
                f"{perf.win_rate:.1f}% | {perf.avg_return:.2f}% | "
                f"{perf.max_return:.2f}% | {perf.min_return:.2f}% |"
            )

        return "\n".join(report_lines)

    def save_report(self, report: str, filename: str = None):
        """保存报告"""
        if filename is None:
            filename = f"compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        report_dir = self.output_dir.parent / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)

        filepath = report_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report)

        return str(filepath)

    @property
    def logger(self):
        import logging
        return logging.getLogger(__name__)
