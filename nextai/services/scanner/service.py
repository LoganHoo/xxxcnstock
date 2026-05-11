"""Scanner 服务 - 封装实时盘面扫描逻辑"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime

from nextai.services.data_access import DataAccess


@dataclass
class ScanResult:
    code: str
    name: str
    price: float
    change_pct: float
    volume: int
    amount: float
    signal_type: str
    signal_score: float
    reason: str
    industry: str = ""
    board_type: str = ""
    timestamp: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ScanResponse:
    success: bool
    trade_date: str
    total_signals: int
    stocks: List[ScanResult]
    execution_time: float
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "trade_date": self.trade_date,
            "total_signals": self.total_signals,
            "stocks": [s.to_dict() for s in self.stocks],
            "execution_time": round(self.execution_time, 2),
            "error": self.error,
        }


class ScannerService:
    """实时扫描服务"""

    def __init__(self, data_access: DataAccess, max_results: int = 100):
        self.data_access = data_access
        self.max_results = max_results

    def scan(self, signal_threshold: float = 0.5) -> ScanResponse:
        import time
        start = time.time()
        trade_date = datetime.now().strftime("%Y%m%d")

        try:
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            from nextai.run_realtime_scanner import RealtimeScanner

            scanner = RealtimeScanner()
            signals = scanner.scan_all()

            results = []
            for s in signals[: self.max_results]:
                results.append(
                    ScanResult(
                        code=s.code,
                        name=s.name,
                        price=s.price,
                        change_pct=s.change_pct,
                        volume=s.volume,
                        amount=s.amount,
                        signal_type=s.signal_type,
                        signal_score=s.signal_score,
                        reason=s.reason,
                        industry=getattr(s, "industry", ""),
                        board_type=getattr(s, "board_type", ""),
                        timestamp=datetime.now().isoformat(),
                    )
                )

            return ScanResponse(
                success=True,
                trade_date=trade_date,
                total_signals=len(results),
                stocks=results,
                execution_time=time.time() - start,
            )

        except Exception as e:
            return ScanResponse(
                success=False,
                trade_date=trade_date,
                total_signals=0,
                stocks=[],
                execution_time=time.time() - start,
                error=str(e),
            )

    def get_status(self) -> Dict[str, Any]:
        codes = self.data_access.get_available_codes()
        return {
            "service": "scanner",
            "status": "running",
            "available_stocks": len(codes),
            "max_results": self.max_results,
            "last_check": datetime.now().isoformat(),
        }
