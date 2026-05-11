"""数据模型定义"""
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime


@dataclass
class StockInfo:
    code: str
    name: str
    industry: str = ""
    market_cap: float = 0.0
    is_st: bool = False
    is_suspended: bool = False
    is_delisted: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class KlineData:
    code: str
    trade_date: str
    open: float
    close: float
    high: float
    low: float
    volume: int
    amount: float = 0.0
    turnover_rate: float = 0.0
    change_pct: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SelectionResult:
    code: str
    name: str
    score: float
    confidence: float
    strategy: str
    signals: Dict[str, bool] = field(default_factory=dict)
    factors: Dict[str, float] = field(default_factory=dict)
    reason: str = ""
    pick_date: str = ""
    timestamp: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ServiceHealth:
    service: str
    status: str
    uptime: float = 0.0
    last_error: str = ""
    dependencies: Dict[str, str] = field(default_factory=dict)
    timestamp: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
