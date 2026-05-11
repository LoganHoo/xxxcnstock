"""Flows 服务 - 封装多策略选股流程"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

from nextai.services.data_access import DataAccess


FLOW_REGISTRY = {
    "conservative": "nextai.pipeline.flows.conservative_flow.ConservativeFlow",
    "balanced_factor": "nextai.pipeline.flows.balanced_factor_flow.BalancedFactorFlow",
    "aggressive_signal": "nextai.pipeline.flows.aggressive_signal_flow.AggressiveSignalFlow",
}


@dataclass
class FlowStockResult:
    code: str
    name: str
    grade: str
    score: float
    signals: Dict[str, bool]
    factors: Dict[str, float]
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FlowResponse:
    success: bool
    flow_name: str
    pick_date: str
    stock_count: int
    stocks: List[FlowStockResult]
    execution_time: float
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "flow_name": self.flow_name,
            "pick_date": self.pick_date,
            "stock_count": self.stock_count,
            "stocks": [s.to_dict() for s in self.stocks],
            "execution_time": round(self.execution_time, 2),
            "error": self.error,
        }


class FlowsService:
    """多策略选股流程服务"""

    def __init__(self, data_access: DataAccess, kline_dir: str = "data/kline"):
        self.data_access = data_access
        self.kline_dir = kline_dir

    def run_flow(
        self, flow_name: str, scores_path: Optional[str] = None
    ) -> FlowResponse:
        import time
        start = time.time()

        if flow_name not in FLOW_REGISTRY:
            return FlowResponse(
                success=False,
                flow_name=flow_name,
                pick_date=datetime.now().strftime("%Y%m%d"),
                stock_count=0,
                stocks=[],
                execution_time=time.time() - start,
                error=f"未知流程: {flow_name}, 可选: {list(FLOW_REGISTRY.keys())}",
            )

        try:
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            module_path, class_name = FLOW_REGISTRY[flow_name].rsplit(".", 1)
            import importlib
            module = importlib.import_module(module_path)
            flow_class = getattr(module, class_name)

            flow = flow_class()
            result = flow.select(kline_dir=self.kline_dir, scores_path=scores_path)

            stocks = []
            for s in result.stocks:
                stocks.append(
                    FlowStockResult(
                        code=s.code,
                        name=s.name,
                        grade=s.grade,
                        score=s.score,
                        signals=s.signals,
                        factors=s.factors,
                        reason=s.reason,
                    )
                )

            return FlowResponse(
                success=True,
                flow_name=flow_name,
                pick_date=result.pick_date,
                stock_count=len(stocks),
                stocks=stocks,
                execution_time=time.time() - start,
            )

        except Exception as e:
            return FlowResponse(
                success=False,
                flow_name=flow_name,
                pick_date=datetime.now().strftime("%Y%m%d"),
                stock_count=0,
                stocks=[],
                execution_time=time.time() - start,
                error=str(e),
            )

    def run_all_flows(self) -> Dict[str, FlowResponse]:
        results = {}
        for flow_name in FLOW_REGISTRY:
            results[flow_name] = self.run_flow(flow_name)
        return results

    def compare_flows(self) -> Dict[str, Any]:
        import time
        start = time.time()

        try:
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            from nextai.pipeline.flows.flow_comparator import FlowComparator

            comparator = FlowComparator()
            all_results = self.run_all_flows()

            for flow_name, response in all_results.items():
                if response.success:
                    from nextai.pipeline.flows.base_flow import FlowResult, StockPick

                    picks = [
                        StockPick(
                            code=s.code,
                            name=s.name,
                            grade=s.grade,
                            score=s.score,
                            signals=s.signals,
                            factors=s.factors,
                            reason=s.reason,
                        )
                        for s in response.stocks
                    ]
                    flow_result = FlowResult(
                        flow_name=flow_name,
                        pick_date=response.pick_date,
                        stocks=picks,
                        execution_time=response.execution_time,
                    )
                    comparator.save_result(flow_result)

            return {
                "success": True,
                "flows": {k: v.to_dict() for k, v in all_results.items()},
                "execution_time": round(time.time() - start, 2),
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "execution_time": round(time.time() - start, 2),
            }

    def list_flows(self) -> List[Dict[str, str]]:
        return [
            {"name": name, "class_path": path}
            for name, path in FLOW_REGISTRY.items()
        ]
