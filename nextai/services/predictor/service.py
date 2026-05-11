"""Predictor 服务 - 封装每日涨停预测逻辑"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime

from nextai.services.data_access import DataAccess


@dataclass
class PredictionResult:
    code: str
    name: str
    confidence: float
    strategy: str
    signals: Dict[str, bool] = field(default_factory=dict)
    factors: Dict[str, float] = field(default_factory=dict)
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PredictionResponse:
    success: bool
    trade_date: str
    total_predictions: int
    stocks: List[PredictionResult]
    execution_time: float
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "trade_date": self.trade_date,
            "total_predictions": self.total_predictions,
            "stocks": [s.to_dict() for s in self.stocks],
            "execution_time": round(self.execution_time, 2),
            "error": self.error,
        }


class PredictorService:
    """涨停预测服务"""

    def __init__(
        self,
        data_access: DataAccess,
        model_path: Optional[str] = None,
        min_confidence: float = 70.0,
    ):
        self.data_access = data_access
        self.model_path = model_path
        self.min_confidence = min_confidence

    def predict(
        self,
        trade_date: Optional[str] = None,
        top_n: int = 10,
        min_confidence: Optional[float] = None,
    ) -> PredictionResponse:
        import time
        start = time.time()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        threshold = min_confidence or self.min_confidence

        try:
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            from nextai.daily_prediction import DailyPredictionPipeline

            pipeline = DailyPredictionPipeline()
            result = pipeline.run(
                trade_date=trade_date,
                top_n=top_n,
                min_confidence=threshold,
            )

            stocks = []
            predictions = result.get("predictions", result.get("stocks", []))
            for p in predictions:
                stocks.append(
                    PredictionResult(
                        code=p.get("code", ""),
                        name=p.get("name", ""),
                        confidence=p.get("confidence", p.get("score", 0.0)),
                        strategy=p.get("strategy", "ensemble"),
                        signals=p.get("signals", {}),
                        factors=p.get("factors", {}),
                        reason=p.get("reason", ""),
                    )
                )

            return PredictionResponse(
                success=True,
                trade_date=trade_date,
                total_predictions=len(stocks),
                stocks=stocks,
                execution_time=time.time() - start,
            )

        except Exception as e:
            return PredictionResponse(
                success=False,
                trade_date=trade_date,
                total_predictions=0,
                stocks=[],
                execution_time=time.time() - start,
                error=str(e),
            )

    def get_status(self) -> Dict[str, Any]:
        return {
            "service": "predictor",
            "status": "running",
            "model_path": self.model_path,
            "min_confidence": self.min_confidence,
            "last_check": datetime.now().isoformat(),
        }
